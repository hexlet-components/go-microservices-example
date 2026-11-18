package lifecycle

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

var ErrDrainDeadline = errors.New("shutdown deadline exceeded; unfinished record not acknowledged")

type Config struct {
	DSN, Schema, Brokers, Topic, OutputTopic, Group, Role, SiteOrigin, Listen, PhaseFile string
	Drain                                                                                time.Duration
	LockKey                                                                              int64
}

func Initialize(ctx context.Context, config Config) error {
	db, err := Prepare(ctx, config.DSN, config.Schema)
	if err != nil {
		return err
	}
	db.Close()
	client, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(config.Brokers, ",")...))
	if err != nil {
		return err
	}
	defer client.Close()
	request := kmsg.NewPtrCreateTopicsRequest()
	for _, topic := range []string{config.Topic, config.OutputTopic} {
		request.Topics = append(request.Topics, kmsg.CreateTopicsRequestTopic{Topic: topic, NumPartitions: 1, ReplicationFactor: 1})
	}
	response, err := request.RequestWith(ctx, client)
	if err != nil {
		return err
	}
	for _, topic := range response.Topics {
		if err := kerr.ErrorForCode(topic.ErrorCode); err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
			return err
		}
	}
	return nil
}

func Run(stop context.Context, config Config) error {
	phase := func(value string) {
		slog.Info("lifecycle", "phase", value, "role", config.Role, "pid", os.Getpid())
		if config.PhaseFile != "" {
			_ = os.WriteFile(config.PhaseFile, []byte(value), 0600)
			if strings.HasPrefix(value, "ready ") {
				_ = os.WriteFile(config.PhaseFile+".ready", []byte(value), 0600)
			}
		}
	}
	startup, endStartup := context.WithTimeout(stop, 10*time.Second)
	defer endStartup()
	db, err := Connect(startup, config.DSN, config.Schema)
	if err != nil {
		return err
	}
	client, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(config.Brokers, ",")...), kgo.ConsumerGroup(config.Group), kgo.ConsumeTopics(config.Topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.DisableAutoCommit(), kgo.BlockRebalanceOnPoll(), kgo.SessionTimeout(6*time.Second), kgo.HeartbeatInterval(time.Second), kgo.RequiredAcks(kgo.AllISRAcks()), kgo.RecordDeliveryTimeout(5*time.Second))
	if err != nil {
		db.Close()
		return err
	}
	if err := client.Ping(startup); err != nil {
		client.Close()
		db.Close()
		return err
	}
	var checker *Checker
	if config.Role == "checks" {
		checker, err = NewChecker(config.SiteOrigin)
		if err != nil {
			client.Close()
			db.Close()
			return err
		}
		checker.Client.Timeout = 5 * time.Second
	} else if config.Role != "reports" {
		client.Close()
		db.Close()
		return errors.New("role must be checks or reports")
	}
	listener, err := net.Listen("tcp", config.Listen)
	if err != nil {
		client.Close()
		db.Close()
		return err
	}
	var ready atomic.Bool
	ready.Store(true)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /live", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /ready", func(w http.ResponseWriter, r *http.Request) {
		if !ready.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		probe, cancel := context.WithTimeout(r.Context(), 500*time.Millisecond)
		defer cancel()
		var version int
		if err := db.QueryRowContext(probe, "SELECT version FROM schema_version WHERE id=1").Scan(&version); err != nil || version != 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		if err := client.Ping(probe); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 2 * time.Second}
	go func() { _ = server.Serve(listener) }()
	phase("ready http://" + listener.Addr().String())
	pollCtx, stopPolling := context.WithCancel(context.Background())
	defer stopPolling()
	workCtx, abortWork := context.WithCancel(context.Background())
	defer abortWork()
	store := Store{DB: db, LockKey: config.LockKey, Phase: phase}
	worker := make(chan error, 1)
	go func() { worker <- consume(pollCtx, workCtx, client, store, checker, config, phase) }()
	var workErr error
	finished := false
	select {
	case <-stop.Done():
	case workErr = <-worker:
		finished = true
	}
	ready.Store(false)
	stopPolling()
	phase("draining")
	drain, cancelDrain := context.WithTimeout(context.Background(), config.Drain)
	defer cancelDrain()
	httpDone := make(chan error, 1)
	go func() { httpDone <- server.Shutdown(drain) }()
	if !finished {
		select {
		case workErr = <-worker:
		case <-drain.Done():
			abortWork()
			_ = server.Close()
			return ErrDrainDeadline
		}
	}
	select {
	case err := <-httpDone:
		if err != nil {
			return err
		}
	case <-drain.Done():
		abortWork()
		return ErrDrainDeadline
	}
	if err := client.Flush(drain); err != nil {
		abortWork()
		return ErrDrainDeadline
	}
	closed := make(chan struct{})
	go func() { client.AllowRebalance(); client.Close(); db.Close(); close(closed) }()
	select {
	case <-closed:
		phase("stopped")
		return workErr
	case <-drain.Done():
		abortWork()
		return ErrDrainDeadline
	}
}

func consume(pollCtx, workCtx context.Context, client *kgo.Client, store Store, checker *Checker, config Config, phase func(string)) error {
	for {
		if pollCtx.Err() != nil {
			return nil
		}
		fetches := client.PollRecords(pollCtx, 1)
		records := fetches.Records()
		if len(records) == 0 {
			client.AllowRebalance()
			if pollCtx.Err() != nil {
				return nil
			}
			if errs := fetches.Errors(); len(errs) > 0 {
				return errs[0].Err
			}
			continue
		}
		for _, record := range records {
			recordCtx, cancelRecord := context.WithTimeout(workCtx, 10*time.Second)
			var err error
			if config.Role == "checks" {
				var event AuditCreated
				if err = json.Unmarshal(record.Value, &event); err == nil {
					err = store.check(recordCtx, client, config.OutputTopic, checker, event)
				}
			} else {
				var event AuditChecked
				if err = json.Unmarshal(record.Value, &event); err == nil {
					_, err = store.Apply(recordCtx, "reports-v1", event)
				}
			}
			if err != nil {
				cancelRecord()
				client.AllowRebalance()
				return err
			}
			if err := client.CommitRecords(recordCtx, record); err != nil {
				cancelRecord()
				client.AllowRebalance()
				return err
			}
			cancelRecord()
			phase(fmt.Sprintf("finished-record %d", record.Offset))
		}
		client.AllowRebalance()
	}
}
