package stormworker

import (
	context "context"
	"fmt"
	"rainstorm/stormgrpc"
	"rainstorm/utility"
)

// Server is the implementation of the StormWorker service.
type StormworkerServer struct {
	stormgrpc.UnimplementedStormWorkerServer
}

// PerformOperation implements the PerformOperation RPC method.
func (s *StormworkerServer) PerformOperation(ctx context.Context, req *stormgrpc.StormworkerRequest) (*stormgrpc.StormworkerResponse, error) {
	// log.Printf("Received request: operation=%s, file=%s, range_start=%d, range_end=%d",
	// req.Operation, req.InputFileName, req.RangeStart, req.RangeEnd)

	utility.LogMessage("Request received from leader - op: " + req.Operation + ",input: " + req.InputFileName)

	tasks, err := addTask(int(req.Phase), req.Operation, int(req.RangeStart), int(req.RangeEnd), req.OutputFileName, req.InputFileName)
	if err != nil {
		utility.LogMessage(fmt.Sprintf("error trying to add new task - %v ", err))
		status := "failure"
		message := "Operation coulkd not be started because of " + err.Error()
		return &stormgrpc.StormworkerResponse{
			Status:  status,
			Message: message,
		}, nil

	}

	go runTask(tasks[int(req.Phase)]) // to be changed.! phase/stage cannot be the key
	status := "success"
	message := "Operation started successfully"

	// Handle an invalid range
	if req.RangeStart > req.RangeEnd {
		status = "failure"
		message = "Invalid range: range_start cannot be greater than range_end"
	}

	return &stormgrpc.StormworkerResponse{
		Status:  status,
		Message: message,
	}, nil
}
