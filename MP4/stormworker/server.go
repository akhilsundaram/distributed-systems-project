package stormworker

import (
	context "context"
	"rainstorm/utility"
)

// Server is the implementation of the StormWorker service.
type StormorkerServer struct {
	UnimplementedStormWorkerServer
}

// PerformOperation implements the PerformOperation RPC method.
func (s *StormorkerServer) PerformOperation(ctx context.Context, req *StormworkerRequest) (*StormworkerResponse, error) {
	// log.Printf("Received request: operation=%s, file=%s, range_start=%d, range_end=%d",
	// 	req.Operation, req.InputFileName, req.RangeStart, req.RangeEnd)

	utility.LogMessage("Request received from leader - op: " + req.Operation + ",input: " + req.InputFileName)

	// Simulated operation
	status := "success"
	message := "Operation started successfully"

	// Handle an invalid range
	if req.RangeStart > req.RangeEnd {
		status = "failure"
		message = "Invalid range: range_start cannot be greater than range_end"
	}

	return &StormworkerResponse{
		Status:  status,
		Message: message,
	}, nil
}
