package stormworker

import (
	context "context"
	"fmt"
	"rainstorm/membership"
	"rainstorm/stormgrpc"
	"rainstorm/utility"
	"strconv"

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

	task, err := addTask(int(req.Stage), int(req.TaskId), req.Operation, int(req.RangeStart), int(req.RangeEnd), req.OutputFileName, req.InputFileName)
	if err != nil {
		utility.LogMessage(fmt.Sprintf("error trying to add new task - %v ", err))
		status := "failure"
		message := "Operation coulkd not be started because of " + err.Error()
		return &stormgrpc.StormworkerResponse{
			Status:  status,
			Message: message,
		}, nil

	}

	go runTask(task) // to be changed.! phase/stage cannot be the key
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
func sendCheckpointStatus(stage, task_id, lineProcessed int, intermediate_file string) {
	conn, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
	if err != nil {
		fmt.Println("GRPCS CONNECTION DID NOT GO THROUGH")
		utility.LogMessage("CONNECTION DID NOT GO THROUGH")
		return
	}
	defer conn.Close()

	client := stormgrpc.NewCheckpointServiceClient(conn)

	// Create a CheckpointRequest
	request := &stormgrpc.CheckpointRequest{
		Stage:              strconv.Itoa(stage),
		LineRangeProcessed: int64(lineProcessed),
		Filename:           intermediate_file,
		Vmname:             membership.My_hostname,
		TaskId:             strconv.Itoa(task_id), // Example task ID
	}

	// Call the Checkpoint RPC
	stream, err := client.Checkpoint(context.Background(), request)
	if err != nil {
		fmt.Println("CHECKPOINT GRPC FAIL")
		utility.LogMessage("CHECKPOINT GRPC FAIL")
		return
	}

	// Process the server stream
	for {
		ack, err := stream.Recv()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			fmt.Printf("Error receiving stream: %v\n", err)
			utility.LogMessage(fmt.Sprintf("Error receiving stream: %v\n", err))
			return
		}
		utility.LogMessage(fmt.Sprintf("Ack received: %v", ack.LineAcked))
	}
	utility.LogMessage("Client finished.")
}
