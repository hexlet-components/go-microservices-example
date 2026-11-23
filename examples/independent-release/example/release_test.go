package release_test

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	release "hexlet.local/independentrelease"
	pb "hexlet.local/independentrelease/checks/v1"
)

func TestEventVersionMatrix(t *testing.T) {
	for _, file := range []string{"v1.json", "v2.json"} {
		data, err := os.ReadFile(filepath.Join("fixtures", file))
		if err != nil {
			t.Fatal(err)
		}
		var event release.EventV1
		if err := json.Unmarshal(data, &event); err != nil {
			t.Fatal(err)
		}
		for _, consumer := range []struct {
			name string
			read func([]byte) (release.Report, error)
		}{{"v1", release.ConsumeV1}, {"v2", release.ConsumeV2}} {
			t.Run(file+"-reports-"+consumer.name, func(t *testing.T) {
				report, err := consumer.read(data)
				if err != nil {
					t.Fatal(err)
				}
				if !report.Ready || !reflect.DeepEqual(report.Findings, event.Findings) {
					t.Fatalf("report=%+v", report)
				}
			})
		}
	}
}

type process struct {
	command *exec.Cmd
	done    chan struct{}
	logPath string
	waitErr error
}

func start(t *testing.T, binary string, args, env []string) *process {
	t.Helper()
	logFile, err := os.CreateTemp(t.TempDir(), "process-*.log")
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(binary, args...)
	command.Env = append(os.Environ(), env...)
	command.Stdout = logFile
	command.Stderr = logFile
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	p := &process{command: command, done: make(chan struct{}), logPath: logFile.Name()}
	go func() { p.waitErr = command.Wait(); logFile.Close(); close(p.done) }()
	t.Cleanup(p.stop)
	return p
}
func (p *process) stop() {
	select {
	case <-p.done:
		return
	default:
	}
	p.command.Process.Signal(syscall.SIGTERM)
	select {
	case <-p.done:
	case <-time.After(4 * time.Second):
		p.command.Process.Kill()
		<-p.done
	}
}
func (p *process) alive() bool {
	select {
	case <-p.done:
		return false
	default:
		return true
	}
}
func address(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	value := listener.Addr().String()
	listener.Close()
	return value
}
func waitFor(t *testing.T, label string, predicate func() bool) {
	t.Helper()
	deadline := time.Now().Add(12 * time.Second)
	for {
		if predicate() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("timeout waiting for " + label)
		}
		time.Sleep(25 * time.Millisecond)
	}
}
func get(address string) (int, []byte) {
	client := &http.Client{Timeout: time.Second}
	response, err := client.Get(address)
	if err != nil {
		return 0, nil
	}
	defer response.Body.Close()
	data, _ := io.ReadAll(response.Body)
	return response.StatusCode, data
}
func ready(t *testing.T, address string, p *process) {
	t.Helper()
	waitFor(t, address, func() bool { code, _ := get("http://" + address + "/ready"); return code == 200 })
	if !p.alive() {
		data, _ := os.ReadFile(p.logPath)
		t.Fatalf("process stopped: %s", data)
	}
}
func fixture(t *testing.T, address, id string) {
	t.Helper()
	client := &http.Client{Timeout: 3 * time.Second}
	response, err := client.Post("http://"+address+"/fixture/"+id, "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != 204 {
		data, _ := io.ReadAll(response.Body)
		t.Fatalf("fixture status=%d %s", response.StatusCode, data)
	}
}
func reportReady(t *testing.T, apiAddress, id string) {
	t.Helper()
	waitFor(t, "report "+id, func() bool {
		code, data := get("http://" + apiAddress + "/audits/" + id + "/report")
		if code != 200 {
			return false
		}
		var report release.Report
		if json.Unmarshal(data, &report) != nil || !report.Ready {
			return false
		}
		if report.AuditID != id || len(report.Findings) != 1 || report.Findings[0].Code != "missing-content-security-policy" || report.Findings[0].PageURL != "http://127.0.0.1:8090/" {
			t.Fatalf("wrong report content: %s", data)
		}
		return true
	})
}

func TestIndependentRelease(t *testing.T) {
	dsn := os.Getenv("RELEASE_TEST_DATABASE_URL")
	broker := os.Getenv("KAFKA_BROKERS")
	if dsn == "" || broker == "" {
		t.Skip("RELEASE_TEST_DATABASE_URL and KAFKA_BROKERS are required for the process release test")
	}
	var random [8]byte
	if _, err := rand.Read(random[:]); err != nil {
		t.Fatal(err)
	}
	suffix := hex.EncodeToString(random[:])
	schema := "release_test_" + suffix
	admin, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	if _, err := admin.Exec("CREATE SCHEMA " + schema); err != nil {
		t.Fatal(err)
	}
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	testDSN := parsed.String()
	storeDB, err := sql.Open("pgx", testDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer storeDB.Close()
	env := []string{"DATABASE_URL=" + testDSN, "KAFKA_BROKERS=" + broker, "EVENT_TOPIC=gm.release." + suffix, "CONSUMER_GROUP=gm.release.reports." + suffix}
	binDir := t.TempDir()
	binaries := map[string]string{}
	for _, item := range []struct{ name, target, version string }{{"checks-v1", "./cmd/checks", "v1"}, {"checks-v2", "./cmd/checks", "v2"}, {"api-v1", "./cmd/api", ""}, {"reports-v1", "./cmd/reports", ""}} {
		path := filepath.Join(binDir, item.name)
		args := []string{"build", "-o", path}
		if item.version != "" {
			args = append(args, "-ldflags", "-X main.version="+item.version)
		}
		args = append(args, item.target)
		command := exec.Command("go", args...)
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("build %s: %v %s", item.name, err, output)
		}
		binaries[item.name] = path
	}
	activeRPC, activeHTTP, candidateRPC, candidateHTTP, apiHTTP, reportsHTTP := address(t), address(t), address(t), address(t), address(t), address(t)
	reports := start(t, binaries["reports-v1"], []string{"-http", reportsHTTP}, env)
	ready(t, reportsHTTP, reports)
	api := start(t, binaries["api-v1"], []string{"-http", apiHTTP, "-checks", activeRPC, "-reports", "http://" + reportsHTTP}, env)
	ready(t, apiHTTP, api)
	active := start(t, binaries["checks-v1"], []string{"-grpc", activeRPC, "-http", activeHTTP}, env)
	ready(t, activeHTTP, active)
	fixture(t, activeHTTP, "a-before")
	reportReady(t, apiHTTP, "a-before")
	waitFor(t, "v1 result", func() bool { code, _ := get("http://" + apiHTTP + "/audits/a-before/result"); return code == 200 })
	candidate := start(t, binaries["checks-v2"], []string{"-grpc", candidateRPC, "-http", candidateHTTP}, env)
	ready(t, candidateHTTP, candidate)
	fixture(t, candidateHTTP, "a-candidate")
	connection, err := grpc.NewClient(candidateRPC, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	result, err := pb.NewChecksServiceClient(connection).GetResult(ctx, &pb.GetResultRequest{AuditId: "a-candidate"})
	cancel()
	if err != nil || result.GetRuleVersion() != "2026-11" || len(result.GetFindings()) != 1 {
		t.Fatalf("candidate=%v error=%v", result, err)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	oldResult, oldErr := pb.NewChecksServiceClient(connection).GetResult(ctx, &pb.GetResultRequest{AuditId: "a-before"})
	cancel()
	if oldErr != nil || oldResult.RuleVersion != nil {
		t.Fatalf("old result metadata changed: %v error=%v", oldResult, oldErr)
	}
	reportReady(t, apiHTTP, "a-candidate")
	candidate.stop()
	outageStarted := time.Now()
	active.stop()
	for i := 0; i < 2; i++ {
		started := time.Now()
		code, _ := get("http://" + apiHTTP + "/audits/a-before/result")
		if code != 503 || time.Since(started) > 2*time.Second {
			t.Fatalf("outage status=%d duration=%v", code, time.Since(started))
		}
	}
	_, diagnostics := get("http://" + apiHTTP + "/diagnostics")
	if !strings.Contains(string(diagnostics), "open") {
		t.Fatalf("circuit not open: %s", diagnostics)
	}
	reportReady(t, apiHTTP, "a-before")
	current := start(t, binaries["checks-v2"], []string{"-grpc", activeRPC, "-http", activeHTTP}, env)
	ready(t, activeHTTP, current)
	waitFor(t, "persistent gRPC client recovery", func() bool { code, _ := get("http://" + apiHTTP + "/audits/a-before/result"); return code == 200 })
	t.Logf("observed bounded 503 responses; recovery including reconnect and breaker probe took %s", time.Since(outageStarted).Round(time.Millisecond))
	_, diagnostics = get("http://" + apiHTTP + "/diagnostics")
	if !strings.Contains(string(diagnostics), "closed") {
		t.Fatalf("circuit did not recover: %s", diagnostics)
	}
	fixture(t, activeHTTP, "a-after")
	reportReady(t, apiHTTP, "a-after")
	var dualWritten bool
	if err := storeDB.QueryRow(`SELECT findings=issues FROM results WHERE audit_id='a-after'`).Scan(&dualWritten); err != nil || !dualWritten {
		t.Fatalf("dual write=%v error=%v", dualWritten, err)
	}
	current.stop()
	rolledBack := start(t, binaries["checks-v1"], []string{"-grpc", activeRPC, "-http", activeHTTP}, env)
	ready(t, activeHTTP, rolledBack)
	waitFor(t, "rollback reads v2 data", func() bool { code, _ := get("http://" + apiHTTP + "/audits/a-after/result"); return code == 200 })
	reportReady(t, apiHTTP, "a-after")
	if !api.alive() || !reports.alive() {
		t.Fatal("an unchanged v1 neighbour stopped")
	}
	t.Logf("API v1 pid=%d and reports v1 pid=%d stayed running; new events preserved ready=true and content; v1 rollback read v2 data", api.command.Process.Pid, reports.command.Process.Pid)
}
