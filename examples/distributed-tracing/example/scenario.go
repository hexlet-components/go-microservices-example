package tracedemo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

var propagator = propagation.TraceContext{}

const demoToken = "prepared-local-service-token"
const method = "/audit.Requests/GetAudit"

type RequestsService interface {
	GetAudit(context.Context, *wrapperspb.StringValue) (*wrapperspb.StringValue, error)
}
type requestsServer struct{ auditID string }

func (s requestsServer) GetAudit(ctx context.Context, input *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
	if input.Value != s.auditID {
		return nil, status.Error(codes.NotFound, "audit not found")
	}
	return wrapperspb.String("http://127.0.0.1:8090/"), nil
}

func register(server *grpc.Server, service RequestsService) {
	server.RegisterService(&grpc.ServiceDesc{ServiceName: "audit.Requests", HandlerType: (*RequestsService)(nil), Methods: []grpc.MethodDesc{{MethodName: "GetAudit", Handler: func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
		request := new(wrapperspb.StringValue)
		if err := dec(request); err != nil {
			return nil, err
		}
		handler := func(ctx context.Context, request any) (any, error) {
			return srv.(RequestsService).GetAudit(ctx, request.(*wrapperspb.StringValue))
		}
		if interceptor == nil {
			return handler(ctx, request)
		}
		return interceptor(ctx, request, &grpc.UnaryServerInfo{Server: srv, FullMethod: method}, handler)
	}}}}, service)
}
func diagnostic(ctx context.Context, request any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
	started := time.Now()
	sc := trace.SpanContextFromContext(ctx)
	slog.Info("rpc_started", "service", "api", "operation", "get_audit", "trace_id", sc.TraceID().String(), "span_id", sc.SpanID().String())
	result, err := next(ctx, request)
	outcome := "ok"
	if err != nil {
		outcome = "error"
	}
	logSpan(ctx, "api", "get_audit", request.(*wrapperspb.StringValue).Value, "", outcome, started)
	return result, err
}
func authenticate(ctx context.Context, request any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	values := md.Get("authorization")
	if len(values) != 1 || values[0] != "Bearer "+demoToken {
		return nil, status.Error(codes.Unauthenticated, "caller rejected")
	}
	return next(ctx, request)
}

type Outcome struct {
	TraceID string `json:"trace_id"`
	AuditID string `json:"audit_id"`
	EventID string `json:"event_id"`
	Ready   bool   `json:"ready"`
}

func Run(lifetime context.Context, store Outbox, p Providers, brokers []string, topic, auditID, token string) (Outcome, error) {
	serverCredentials, clientCredentials, err := loopbackCredentials()
	if err != nil {
		return Outcome{}, err
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return Outcome{}, err
	}
	server := grpc.NewServer(grpc.Creds(serverCredentials), grpc.StatsHandler(otelgrpc.NewServerHandler(otelgrpc.WithTracerProvider(p.API), otelgrpc.WithPropagators(propagator))), grpc.ChainUnaryInterceptor(diagnostic, authenticate))
	register(server, requestsServer{auditID: auditID})
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			slog.Error("grpc_server_failed", "error", err)
		}
	}()
	defer server.Stop()
	client, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(clientCredentials), grpc.WithStatsHandler(otelgrpc.NewClientHandler(otelgrpc.WithTracerProvider(p.Checks), otelgrpc.WithPropagators(propagator))))
	if err != nil {
		return Outcome{}, err
	}
	defer client.Close()
	requestCtx, cancel := context.WithTimeout(lifetime, 3*time.Second)
	requestCtx, accepted := p.API.Tracer("audit-demo").Start(requestCtx, "audit.accept")
	accepted.SetAttributes(attribute.String("audit_id", auditID))
	outcome := Outcome{AuditID: auditID, TraceID: accepted.SpanContext().TraceID().String(), EventID: "checked-" + auditID}
	checkCtx, completed := p.Checks.Tracer("audit-demo").Start(requestCtx, "checks.complete")
	var response wrapperspb.StringValue
	err = client.Invoke(metadata.AppendToOutgoingContext(checkCtx, "authorization", "Bearer "+token), method, wrapperspb.String(auditID), &response)
	if err == nil {
		event := AuditChecked{AuditID: auditID, ResultID: "result-" + auditID, EventID: "checked-" + auditID, Findings: []Finding{{PageURL: response.Value, Code: "missing-content-security-policy"}}}
		carrier := propagation.MapCarrier{}
		propagator.Inject(checkCtx, carrier)
		err = store.Save(checkCtx, Envelope{Event: event, Carrier: carrier})
	}
	completed.End()
	accepted.End()
	cancel() // The requester has finished; outbox work must still use its own live context.
	if err != nil {
		return outcome, err
	}
	row, err := store.Load(lifetime, outcome.EventID)
	if err != nil {
		return outcome, err
	}
	senderCtx, senderCancel := context.WithTimeout(lifetime, 5*time.Second)
	defer senderCancel()
	causalCtx := propagator.Extract(senderCtx, row.Carrier)
	publishCtx, publication := p.Sender.Tracer("audit-demo").Start(causalCtx, "outbox.publish", trace.WithSpanKind(trace.SpanKindProducer))
	publication.SetAttributes(attribute.String("audit_id", auditID), attribute.String("event_id", row.Event.EventID))
	carrier := propagation.MapCarrier{}
	propagator.Inject(publishCtx, carrier)
	record := &kgo.Record{Topic: topic, Key: []byte(row.Event.AuditID)}
	for key, value := range carrier {
		record.Headers = append(record.Headers, kgo.RecordHeader{Key: key, Value: []byte(value)})
	}
	record.Value, err = json.Marshal(row.Event)
	if err != nil {
		publication.End()
		return outcome, err
	}
	producer, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.RequiredAcks(kgo.AllISRAcks()))
	if err != nil {
		publication.End()
		return outcome, err
	}
	started := time.Now()
	err = producer.ProduceSync(publishCtx, record).FirstErr()
	producer.Close()
	if err == nil {
		err = store.MarkSent(senderCtx, row.Event.EventID)
	}
	publishOutcome := "ok"
	if err != nil {
		publication.RecordError(err)
		publication.SetStatus(otelcodes.Error, "publication failed")
		publishOutcome = "error"
	}
	logSpan(publishCtx, "outbox", "publish_event", auditID, row.Event.EventID, publishOutcome, started)
	publication.End()
	if err != nil {
		return outcome, err
	}
	consumer, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: {0: kgo.NewOffset().AtStart()}}))
	if err != nil {
		return outcome, err
	}
	defer consumer.Close()
	consumerCtx, consumerCancel := context.WithTimeout(lifetime, 5*time.Second)
	defer consumerCancel()
	fetches := consumer.PollRecords(consumerCtx, 1)
	if err := fetches.Err(); err != nil {
		return outcome, err
	}
	records := fetches.Records()
	if len(records) != 1 {
		return outcome, fmt.Errorf("expected one event")
	}
	received := propagation.MapCarrier{}
	for _, header := range records[0].Headers {
		if header.Key == "traceparent" || header.Key == "tracestate" {
			received[header.Key] = string(header.Value)
		}
	}
	parent := propagator.Extract(consumerCtx, received)
	reportCtx, reportSpan := p.Reports.Tracer("audit-demo").Start(parent, "reports.apply", trace.WithSpanKind(trace.SpanKindConsumer))
	defer reportSpan.End()
	reportSpan.SetAttributes(attribute.String("audit_id", auditID), attribute.String("event_id", row.Event.EventID))
	var checked AuditChecked
	if err := json.Unmarshal(records[0].Value, &checked); err != nil {
		reportSpan.RecordError(err)
		return outcome, err
	}
	if checked.EventID != row.Event.EventID {
		return outcome, fmt.Errorf("unexpected event identity")
	}
	logSpan(reportCtx, "reports", "build_report", checked.AuditID, checked.EventID, "ok", time.Now())
	outcome.Ready = true
	return outcome, nil
}

func Pause(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
