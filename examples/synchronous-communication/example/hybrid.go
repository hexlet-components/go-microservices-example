package audit

import "context"

type Audit struct {
	ID  string
	URL string
}

type Checks interface {
	Start(context.Context, Audit) error
	State(context.Context, string) (CheckState, error)
	Result(context.Context, string) (Result, error)
}

type StartStateClient interface {
	Start(context.Context, Audit) error
	State(context.Context, string) (CheckState, error)
}

// HTTP supplies the existing Start and State methods.
// RPC changes only the transport used for reading a result.
type ChecksClient struct {
	HTTP StartStateClient
	RPC  Reader
}

func (c *ChecksClient) Start(ctx context.Context, audit Audit) error {
	return c.HTTP.Start(ctx, audit)
}
func (c *ChecksClient) State(ctx context.Context, id string) (CheckState, error) {
	return c.HTTP.State(ctx, id)
}
func (c *ChecksClient) Result(ctx context.Context, id string) (Result, error) {
	return c.RPC.Result(ctx, id)
}

var _ Checks = (*ChecksClient)(nil)
