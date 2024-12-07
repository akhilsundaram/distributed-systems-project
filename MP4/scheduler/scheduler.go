package scheduler

import (
	"context"
	"log"
	"math"
	"math/rand"
	"net"
	"rainstorm/membership"
	"rainstorm/stormgrpc"
	"rainstorm/utility"
	"strconv"
	"sync"
	"time"

	grpc "google.golang.org/grpc"
)

const (
	scheduler_port  = "6543"
	checkpoint_port = "6542"
	timeout         = 10 * time.Millisecond
)

type AvailableNodesStruct struct {
	nodes map[string]int
	mutex sync.RWMutex
}

type NodeInUseInfo struct {
	Operation       string
	InputFileName   string
	TotalNumTasks   int
	OutputFileName  string
	AggregateOutput string
	Stage           int
	LineRangeStart  int
	LineRangeEnd    int
	NodeId          int
	LinesProcessed  int
}

type NodeInUseStruct struct {
	nodes map[string][]NodeInUseInfo
	mutex sync.RWMutex
}

type CheckpointStats struct {
	Stage          string
	LinesProcessed int
	TempFilename   string
	VmName         string
	TaskId         string
	Operation      string
	// other fields to add in Checkpointing struct to save in memory
}

type NodeCheckpointStatsStruct struct {
	stats map[string]map[string]CheckpointStats
	mutex sync.RWMutex
}

var (
	AvailableNodes      AvailableNodesStruct
	NodeInUse           NodeInUseStruct
	NodeCheckpointStats NodeCheckpointStatsStruct
)

func InitializeScheduler() {
	utility.LogMessage("Starting up scheduler")

	// initialize list of members
	AvailableNodes = AvailableNodesStruct{
		nodes: make(map[string]int),
	}

	NodeInUse = NodeInUseStruct{
		nodes: make(map[string][]NodeInUseInfo),
	}

	NodeCheckpointStats = NodeCheckpointStatsStruct{
		stats: make(map[string]map[string]CheckpointStats),
	}

	// scheduler sender
	// Start scheduler rpc server for checkpoints
	listener, err := net.Listen("tcp", ":"+checkpoint_port)
	if err != nil {
		utility.LogMessage("Init Scheduler -  start checkpoint server failure")
		log.Fatalf("Failed to listen on port: %v", err)
	}
	checkpointServer := grpc.NewServer()
	// RegisterFileServiceServer(checkpointServer, &FileServer{})
	stormgrpc.RegisterCheckpointServiceServer(checkpointServer, &CheckpointServer{})

	go func() {
		utility.LogMessage("RPC checkpoint server goroutine entered")
		if err := checkpointServer.Serve(listener); err != nil {
			utility.LogMessage("Init Scheduler -  start checkpoint server failure")
		}
	}()

	MonitorMembershipList()
}

func MonitorMembershipList() {
	// this is a blocking call
	// this will run as long as scheduler is running

	for {
		scheduler_change := <-membership.SchedulerMemberchan
		var action string
		if scheduler_change.Event == membership.Add {
			action = "Add"
		} else {
			action = "Delete"
		}
		UpdateSchedulerMemeberList(scheduler_change.NodeName, action)
	}
}

// RainStorm <op1 _exe> <op2 _exe> <hydfs_src_file> <hydfs_dest_filename> <num_tasks>
func StartScheduler(srcFilePath string, numTasks int, destFilePath string, op1Exe string, op2Exe string) error {

	op0Exe := "source"
	ops := []string{op0Exe, op1Exe, op2Exe}
	num_ops := len(ops)
	utility.LogMessage("Starting scheduler with " + strconv.Itoa(num_ops) + " tasks")

	//total lines in source file
	totalLines, err := utility.FileLineCount(srcFilePath)
	if err != nil {
		errMsg := "error counting lines in source file: " + err.Error()
		utility.LogMessage(errMsg)
		return err
	}

	//  choose numTasks nodes at random from AvailableNodes
	rand.Seed(time.Now().UnixNano())
	selectedNodes := make([]string, numTasks)

	// calc lines per task
	linesPerTask := int(math.Ceil(float64(totalLines) / float64(numTasks)))

	// log task information
	utility.LogMessage("Total lines: " + strconv.Itoa(totalLines))
	utility.LogMessage("Lines per task: " + strconv.Itoa(linesPerTask))

	// make this as a function
	// update AvailableNodes, initialize NodesInUse and NodeCheckpointStats
	for i, node := range selectedNodes {
		// Update AvailableNodes
		SetAvailableNode(node, 1)

		line_start := i * linesPerTask
		line_end := (i + 1) * linesPerTask
		if line_end > totalLines {
			line_end = totalLines
		}

		// populate the NodeInUse, get ready to start sending requests to Scheduler Listener on VMs
		/*
			nodeInit := NodeInUseInfo{
				CurrentStage:                1,
				Stage1LineNoStart:           line_start,
				Stage1LineNoEnd:             line_end,
				AggregateNodeId:             i,
				AggregateStageKeyRangeStart: 0, // Can be set to range of values , with start as this
				AggregateStageKeyRangeEnd:   0, // Can be set to range of values , with end as this
			}
			SetNodeInUse(node, nodeInit)

			// initialize NodeCheckpointStats
			checkpointInit := CheckpointStats{
				Stage:          "Stage1",
				LinesProcessed: 0,
				TempFilename:   "",
			}
			UpdateNodeCheckpointStats(node, "Stage1", checkpointInit)
		*/
	}

	// TODO: Implement the logic to process tasks and write to destFilePath

	return nil
}

func SendSchedulerRequest(node string, nodeInstr NodeInUseInfo) {
	// send request to each node in NodeInUse
	// to start processing the task
	serverIP := utility.GetIPAddr(node)
	conn, err := grpc.Dial(serverIP.String()+":"+scheduler_port, grpc.WithInsecure())
	if err != nil {
		utility.LogMessage("Unable to connect to server - ring rpc fserver - " + err.Error())
	}
	defer conn.Close()
	utility.LogMessage("created conn with server: " + node)

	// sedn request to worker node

	client := stormgrpc.NewStormWorkerClient(conn)

	// Prepare the request
	req := &stormgrpc.StormworkerRequest{
		Operation:       "CustomOperation",
		InputFileName:   "input.txt",
		NumTasks:        3,
		OutputFileName:  "output.txt",
		AggregateOutput: true,
		Stage:           1,
		RangeStart:      0,
		RangeEnd:        100,
		TaskId:          0,
		LinesProcessed:  -1,
	}

	// Call the PerformOperation RPC
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	resp, err := client.PerformOperation(ctx, req)
	if err != nil {
		log.Fatalf("Failed to perform operation: %v", err)
	}
	utility.LogMessage("Response from server: status=" + resp.Status + ", message=" + resp.Message)
}
