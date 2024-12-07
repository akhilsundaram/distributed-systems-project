package stormworker

import (
	context "context"
	"fmt"
	"log"
	"rainstorm/membership"
	"rainstorm/stormgrpc"
	"rainstorm/utility"
	"time"

	grpc "google.golang.org/grpc"
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

	task, err := AddTask(int(req.Stage), int(req.TaskId), req.Operation, int(req.RangeStart), int(req.RangeEnd), req.OutputFileName, req.InputFileName, req.AggregateOutput, int(req.NumTasks))
	if err != nil {
		utility.LogMessage(fmt.Sprintf("error trying to add new task - %v ", err))
		status := "failure"
		message := "Operation coulkd not be started because of " + err.Error()
		return &stormgrpc.StormworkerResponse{
			Status:  status,
			Message: message,
		}, nil

	}

	go RunTask(task) // to be changed.! phase/stage cannot be the key
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

/* LEADER CLIENT STUFF */
// from grpc client function template from the internet
func sendCheckpointStatus(stage, task_id, lineProcessed int, intermediate_file, operation string) {
	leader_ip := utility.GetIPAddr(LEADER_HOSTNAME).String() //leader unchanged
	conn, err := grpc.Dial(leader_ip+":6542", grpc.WithInsecure())
	if err != nil {
		fmt.Println("GRPCS CONNECTION DID NOT GO THROUGH")
		utility.LogMessage("CONNECTION DID NOT GO THROUGH")
		return
	}
	defer conn.Close()

	client := stormgrpc.NewCheckpointServiceClient(conn)

	// Create a CheckpointRequest
	request := &stormgrpc.CheckpointRequest{
		Stage:              int32(stage),
		LineRangeProcessed: int32(lineProcessed),
		Filename:           intermediate_file,
		Vmname:             membership.My_hostname,
		TaskId:             int32(task_id), // Example task ID
		Operation:          operation,
	}

	// Call the Checkpoint RPC
	// Call the Checkpoint RPC
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = client.Checkpoint(ctx, request)
	if err != nil {
		log.Fatalf("Checkpoint call failed: %v", err)
	}
}
