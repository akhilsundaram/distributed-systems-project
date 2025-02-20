package stormworker

import (
	context "context"
	"fmt"
	"log"
	"os"
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

	task, err := AddTask(int(req.Stage), int(req.TaskId), req.Operation, int(req.RangeStart), int(req.RangeEnd), req.OutputFileName, req.InputFileName, req.AggregateOutput, int(req.NumTasks), req.CustomParam, req.State)
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

	return &stormgrpc.StormworkerResponse{
		Status:  status,
		Message: message,
	}, nil
}

// Stop server
// PerformOperation implements the PerformOperation RPC method.
func (s *StormworkerServer) StopServer(ctx context.Context, req *stormgrpc.StopStormRequest) (*stormgrpc.StopStormResponse, error) {
	defer func() {
		os.Exit(1)
	}()
	utility.LogMessage("STOPN SIGNAL RECEIVED, STOPPING ALL ACTIVITY __~ Good Bye ~__")
	return &stormgrpc.StopStormResponse{
		Status:  "stop",
		Message: "stopping server",
	}, nil
}

/* LEADER CLIENT STUFF */
// from grpc client function template from the internet
func sendCheckpointStatus(stage, task_id, lineProcessed int, intermediate_file, operation, state string, delete bool) bool {
	leader_ip := utility.GetIPAddr(LEADER_HOSTNAME).String() //leader unchanged
	conn, err := grpc.Dial(leader_ip+":6542", grpc.WithInsecure())
	if err != nil {
		fmt.Println("GRPCS CONNECTION DID NOT GO THROUGH")
		utility.LogMessage("CONNECTION DID NOT GO THROUGH")
		return false
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
		State:              state,
		Completed:          delete,
	}

	// Call the Checkpoint RPC
	// Call the Checkpoint RPC
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	response, err := client.Checkpoint(ctx, request)
	if err != nil {
		log.Fatalf("Checkpoint call failed: %v", err)
	}

	if response.PrevStageCompleted {
		return true
	}
	return false
}

func sendEcho(value string) {
	leader_ip := utility.GetIPAddr(LEADER_HOSTNAME).String() //leader unchanged
	conn, err := grpc.Dial(leader_ip+":6543", grpc.WithInsecure())
	if err != nil {
		fmt.Println("GRPCS CONNECTION DID NOT GO THROUGH")
		utility.LogMessage("CONNECTION DID NOT GO THROUGH")
		return
	}
	defer conn.Close()

	client := stormgrpc.NewEchoServiceClient(conn)

	// Create a CheckpointRequest
	request := &stormgrpc.EchoRequest{
		Message: value,
	}

	// Call the Checkpoint RPC
	// Call the Checkpoint RPC
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = client.Echo(ctx, request)
	if err != nil {
		utility.LogMessage("Echo call failed")
	}
}
