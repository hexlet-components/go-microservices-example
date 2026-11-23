package lifecycle

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func uniqueName(prefix string) string {
	var bytes [10]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		panic(err)
	}
	return prefix + hex.EncodeToString(bytes[:])
}

func dependencies(t *testing.T) (Store, Config, *kgo.Client) {
	t.Helper()
	config := Config{DSN: os.Getenv("DATABASE_URL"), Brokers: os.Getenv("KAFKA_BROKERS"), Schema: uniqueName("life_"), Topic: uniqueName("life_input_"), OutputTopic: uniqueName("life_output_"), Group: uniqueName("life_group_"), Listen: "127.0.0.1:0", Drain: 4 * time.Second}
	if config.DSN == "" || config.Brokers == "" {
		t.Fatal("DATABASE_URL and KAFKA_BROKERS are required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	db, err := Prepare(ctx, config.DSN, config.Schema)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		db.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := DropSchema(ctx, config.DSN, config.Schema); err != nil {
			t.Error(err)
		}
	})
	if err := Initialize(ctx, config); err != nil {
		t.Fatal(err)
	}
	client, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(config.Brokers, ",")...))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		request := kmsg.NewPtrDeleteTopicsRequest()
		for _, topic := range []string{config.Topic, config.OutputTopic} {
			topic := topic
			request.TopicNames = append(request.TopicNames, topic)
			request.Topics = append(request.Topics, kmsg.DeleteTopicsRequestTopic{Topic: &topic})
		}
		response, err := request.RequestWith(ctx, client)
		if err != nil {
			t.Error(err)
			return
		}
		for _, topic := range response.Topics {
			if err := kerr.ErrorForCode(topic.ErrorCode); err != nil {
				t.Error(err)
			}
		}
	})
	return Store{DB: db}, config, client
}

func publish(t *testing.T, client *kgo.Client, topic string, event any) {
	t.Helper()
	data, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: []byte("audit"), Value: data}).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

type childProcess struct {
	cmd    *exec.Cmd
	done   chan struct{}
	err    error
	output bytes.Buffer
	phase  string
}

func startChild(t *testing.T, config Config) *childProcess {
	t.Helper()
	config.PhaseFile = filepath.Join(t.TempDir(), "phase")
	data, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	child := &childProcess{done: make(chan struct{}), phase: config.PhaseFile}
	child.cmd = exec.Command(os.Args[0], "-test.run=^TestLifecycleChild$", "-test.timeout=45s")
	child.cmd.Env = append(os.Environ(), "LIFECYCLE_CHILD_CONFIG="+string(data))
	child.cmd.Stdout = &child.output
	child.cmd.Stderr = &child.output
	if err := child.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { child.err = child.cmd.Wait(); close(child.done) }()
	t.Cleanup(func() {
		select {
		case <-child.done:
		default:
			_ = child.cmd.Process.Kill()
			<-child.done
		}
	})
	return child
}

func (p *childProcess) awaitPhase(t *testing.T, prefix string) string {
	t.Helper()
	deadline := time.NewTimer(25 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		phaseFile := p.phase
		if strings.HasPrefix(prefix, "ready ") {
			phaseFile += ".ready"
		}
		data, _ := os.ReadFile(phaseFile)
		if strings.HasPrefix(string(data), prefix) {
			return string(data)
		}
		select {
		case <-p.done:
			t.Fatalf("child exited before %s: %v %s", prefix, p.err, p.output.String())
			return ""
		case <-deadline.C:
			t.Fatalf("phase %s not reached", prefix)
			return ""
		case <-tick.C:
		}
	}
}

func (p *childProcess) signal(t *testing.T) {
	t.Helper()
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
}
func (p *childProcess) awaitExit(t *testing.T) error {
	t.Helper()
	select {
	case <-p.done:
		return p.err
	case <-time.After(10 * time.Second):
		t.Fatal("child failed to terminate within a bound")
		return nil
	}
}

func committedOffset(t *testing.T, client *kgo.Client, config Config) int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	request := kmsg.NewPtrOffsetFetchRequest()
	request.Group = config.Group
	request.Topics = []kmsg.OffsetFetchRequestTopic{{Topic: config.Topic, Partitions: []int32{0}}}
	group := kmsg.NewOffsetFetchRequestGroup()
	group.Group = config.Group
	group.Topics = []kmsg.OffsetFetchRequestGroupTopic{{Topic: config.Topic, Partitions: []int32{0}}}
	request.Groups = []kmsg.OffsetFetchRequestGroup{group}
	response, err := request.RequestWith(ctx, client)
	if err != nil {
		t.Fatal(err)
	}
	if response.Version >= 8 {
		if len(response.Groups) != 1 {
			t.Fatal("missing group offset response")
		}
		group := response.Groups[0]
		if err := kerr.ErrorForCode(group.ErrorCode); err != nil {
			t.Fatal(err)
		}
		return group.Topics[0].Partitions[0].Offset
	}
	if err := kerr.ErrorForCode(response.ErrorCode); err != nil {
		t.Fatal(err)
	}
	return response.Topics[0].Partitions[0].Offset
}

func TestSIGTERMDuringCheck(t *testing.T) {
	store, config, client := dependencies(t)
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	fixture := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started <- struct{}{}
		select {
		case <-release:
			io.WriteString(w, "<!doctype html><html><head></head><body>fixture</body></html>")
		case <-r.Context().Done():
		}
	}))
	defer fixture.Close()
	config.Role = "checks"
	config.SiteOrigin = fixture.URL
	publish(t, client, config.Topic, AuditCreated{EventID: "created-a-42", AuditID: "a-42", URL: fixture.URL})
	publish(t, client, config.Topic, AuditCreated{EventID: "created-a-43", AuditID: "a-43", URL: fixture.URL})
	child := startChild(t, config)
	ready := child.awaitPhase(t, "ready ")
	for _, path := range []string{"/live", "/ready"} {
		response, err := http.Get(strings.TrimPrefix(ready, "ready ") + path)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != 200 {
			t.Fatalf("%s status=%d", path, response.StatusCode)
		}
	}
	select {
	case <-started:
	case <-time.After(20 * time.Second):
		t.Fatal("check did not start")
	}
	child.signal(t)
	child.awaitPhase(t, "draining")
	if err := child.cmd.Process.Signal(syscall.Signal(0)); err != nil {
		t.Fatal("accepted check was terminated before draining")
	}
	close(release)
	if err := child.awaitExit(t); err != nil {
		t.Fatalf("drain failed: %v %s", err, child.output.String())
	}
	var results, published int
	if err := store.DB.QueryRow("SELECT count(*) FROM results").Scan(&results); err != nil {
		t.Fatal(err)
	}
	if err := store.DB.QueryRow("SELECT count(*) FROM outbox WHERE published").Scan(&published); err != nil {
		t.Fatal(err)
	}
	if results != 1 || published != 1 || committedOffset(t, client, config) != 1 {
		t.Fatalf("drain admitted extra work or lost accepted work: results=%d published=%d", results, published)
	}
	t.Log("accepted check saved; publication acknowledged; offset=1; second record remains pending")
}

func TestSIGTERMDuringReportPersistence(t *testing.T) {
	store, config, client := dependencies(t)
	config.Role = "reports"
	config.Drain = 500 * time.Millisecond
	config.LockKey = time.Now().UnixNano() & 0x3fffffffffffffff
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	lock, err := store.DB.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	if _, err := lock.ExecContext(ctx, "SELECT pg_advisory_lock($1)", config.LockKey); err != nil {
		t.Fatal(err)
	}
	defer lock.ExecContext(context.Background(), "SELECT pg_advisory_unlock($1)", config.LockKey)
	publish(t, client, config.Topic, AuditChecked{EventID: "e-42", AuditID: "a-42", ResultID: "r-42", Findings: []Finding{{PageURL: "http://fixture/", Code: "missing-content-security-policy"}}})
	child := startChild(t, config)
	child.awaitPhase(t, "report-persist")
	began := time.Now()
	child.signal(t)
	err = child.awaitExit(t)
	exit, ok := err.(*exec.ExitError)
	if !ok || exit.ExitCode() != 2 {
		t.Fatalf("expected bounded exit2: %v %s", err, child.output.String())
	}
	if time.Since(began) > config.Drain+3*time.Second {
		t.Fatal("shutdown exceeded its bounded margin")
	}
	applications, markers, err := store.Statistics(ctx, "a-42")
	if err != nil || applications != 0 || markers != 0 {
		t.Fatalf("unfinished report was confirmed: %d/%d err=%v", applications, markers, err)
	}
	if offset := committedOffset(t, client, config); offset != -1 {
		t.Fatalf("unfinished offset was committed: %d", offset)
	}
	if _, err := lock.ExecContext(ctx, "SELECT pg_advisory_unlock($1)", config.LockKey); err != nil {
		t.Fatal(err)
	}
	config.Drain = 4 * time.Second
	config.LockKey = 0
	restarted := startChild(t, config)
	restarted.awaitPhase(t, "finished-record 0")
	restarted.signal(t)
	if err := restarted.awaitExit(t); err != nil {
		t.Fatalf("retry shutdown failed: %v %s", err, restarted.output.String())
	}
	applications, markers, err = store.Statistics(ctx, "a-42")
	if err != nil || applications != 1 || markers != 1 || committedOffset(t, client, config) != 1 {
		t.Fatalf("retry result=%d/%d err=%v", applications, markers, err)
	}
	t.Log("incomplete report rolled back; offset absent; restart applied once and committed offset1")
}

func TestReadinessRejectsUnsupportedSchema(t *testing.T) {
	store, config, _ := dependencies(t)
	if _, err := store.DB.Exec("UPDATE schema_version SET version=2"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if db, err := Connect(ctx, config.DSN, config.Schema); err == nil {
		db.Close()
		t.Fatal("unsupported schema accepted")
	}
}

func TestLifecycleChild(t *testing.T) {
	data := os.Getenv("LIFECYCLE_CHILD_CONFIG")
	if data == "" {
		return
	}
	var config Config
	if err := json.Unmarshal([]byte(data), &config); err != nil {
		t.Fatal(err)
	}
	stop, release := signalContext()
	defer release()
	if err := Run(stop, config); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	os.Exit(0)
}
