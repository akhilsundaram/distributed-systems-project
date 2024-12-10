package scheduler

import (
	"context"
	"fmt"
	"rainstorm/stormgrpc"
)

// EchoServer implements the EchoService.
type EchoServer struct {
	stormgrpc.UnimplementedEchoServiceServer
}

// Echo handles the EchoRequest and prints the message to the console.
func (s *EchoServer) Echo(ctx context.Context, req *stormgrpc.EchoRequest) (*stormgrpc.EchoResponse, error) {
	if Echo_toggle {
		fmt.Printf("%s\n", req.GetMessage())
	}
	return &stormgrpc.EchoResponse{Status: "Message received"}, nil
}
