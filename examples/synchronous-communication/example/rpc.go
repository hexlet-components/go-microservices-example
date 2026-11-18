package audit

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	pb "hexlet.local/checksrpc/checks/v1"
)

type RPCServer struct {
	pb.UnimplementedChecksServiceServer
	Reader Reader
}

func (s *RPCServer) GetResult(ctx context.Context, request *pb.GetResultRequest) (*pb.GetResultResponse, error) {
	if request.GetAuditId() == "" {
		return nil, status.Error(codes.InvalidArgument, "audit_id is required")
	}
	result, err := s.Reader.Result(ctx, request.GetAuditId())
	switch {
	case errors.Is(err, ErrNotFound):
		return nil, status.Error(codes.NotFound, "audit not found")
	case errors.Is(err, ErrNotReady):
		return nil, status.Error(codes.FailedPrecondition, "result not ready")
	case errors.Is(err, context.Canceled):
		return nil, status.Error(codes.Canceled, "request canceled")
	case errors.Is(err, context.DeadlineExceeded):
		return nil, status.Error(codes.DeadlineExceeded, "deadline exceeded")
	case err != nil:
		return nil, status.Error(codes.Internal, "read failed")
	}
	findings := make([]*pb.Finding, 0, len(result.Findings))
	for _, f := range result.Findings {
		findings = append(findings, &pb.Finding{PageUrl: f.PageURL, Code: f.Code})
	}
	return &pb.GetResultResponse{ResultId: result.ID, AuditId: result.AuditID, Findings: findings}, nil
}

type RPCReader struct{ Client pb.ChecksServiceClient }

func (c *RPCReader) Result(ctx context.Context, id string) (Result, error) {
	response, err := c.Client.GetResult(ctx, &pb.GetResultRequest{AuditId: id})
	switch status.Code(err) {
	case codes.NotFound:
		return Result{}, fmt.Errorf("%w: %s", ErrNotFound, id)
	case codes.FailedPrecondition:
		return Result{}, fmt.Errorf("%w: %s", ErrNotReady, id)
	}
	if err != nil {
		return Result{}, err
	}
	findings := make([]Finding, 0, len(response.GetFindings()))
	for _, f := range response.GetFindings() {
		findings = append(findings, Finding{PageURL: f.GetPageUrl(), Code: f.GetCode()})
	}
	return Result{ID: response.GetResultId(), AuditID: response.GetAuditId(), Findings: findings}, nil
}
