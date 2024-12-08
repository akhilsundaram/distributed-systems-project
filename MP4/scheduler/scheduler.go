package scheduler

import (
	"context"
	"fmt"
	"log"
	"math"
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
	SCHEDULER_HOST  = "fa24-cs425-5901.cs.illinois.edu"
)

type AvailableNodesStruct struct {
	nodes map[string]int
	mutex sync.RWMutex
}

type NodeInUseInfo struct {
	Operation       string
	InputFileName   string
	TotalNumTasks   int32
	OutputFileName  string
	AggregateOutput bool
	Stage           int32
	LineRangeStart  int32
	LineRangeEnd    int32
	NodeId          int32
	LinesProcessed  int32
	CustomFilter    string
}

type NodeInUseStruct struct {
	nodes map[string][]NodeInUseInfo
	mutex sync.RWMutex
}

type CheckpointStats struct {
	Stage          int32
	LinesProcessed int32
	TempFilename   string
	VmName         string
	TaskId         int32
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

	go MonitorMembershipList()
}

func MonitorMembershipList() {
	// this is a blocking call
	// this will run as long as scheduler is running
	utility.LogMessage("Scheduler starting membership list monitoring")
	for {
		scheduler_change := <-membership.SchedulerMemberchan

		var action string
		if scheduler_change.NodeName == SCHEDULER_HOST {
			utility.LogMessage("Scheduler: Ignoring scheduler node")
			continue
		}
		if scheduler_change.Event == membership.Add {
			utility.LogMessage("Scheduler: Node to be added to available list: " + scheduler_change.NodeName)
			action = "Add"
		} else if scheduler_change.Event == membership.Delete {
			utility.LogMessage("Scheduler: Node added to be removed from list: " + scheduler_change.NodeName)
			action = "Delete"
		}
		UpdateSchedulerMemeberList(scheduler_change.NodeName, action)
	}
}

// RainStorm <op1 _exe> <op2 _exe> <hydfs_src_file> <hydfs_dest_filename> <num_tasks>
func StartScheduler(srcFilePath string, numTasks int, destFilePath string, op1Exe string, op2Exe string, filters ...string) error {

	op0Exe := "source"
	ops := []string{op0Exe, op1Exe, op2Exe}

	//total lines in source file
	totalLines, err := utility.FileLineCount(srcFilePath)
	if err != nil {
		errMsg := "error counting lines in source file: " + err.Error()
		utility.LogMessage(errMsg)
		return err
	}

	//  choose numTasks nodes at random from AvailableNodes
	// calc lines per task
	linesPerTask := int(math.Ceil(float64(totalLines) / float64(numTasks)))

	// log task information
	utility.LogMessage("Total lines: " + strconv.Itoa(totalLines))
	utility.LogMessage("Lines per task: " + strconv.Itoa(linesPerTask))

	filter_op1 := ""
	filter_op2 := ""
	customFilter := ""
	if len(filters) > 0 {
		filter_op1 = filters[0]
		if len(filters) > 1 {
			filter_op2 = filters[1]
		}
	}
	// make this as a function
	// update AvailableNodes, initialize NodesInUse and NodeCheckpointStats
	for stageIndex, operation := range ops {
		utility.LogMessage(fmt.Sprintf("Starting stage %d with operation: %s", stageIndex, operation))

		// Select nodes for this operation
		selectedNodes, err := SelectNodesWithLeastTasks(numTasks)
		if err != nil {
			return fmt.Errorf("error selecting nodes for operation %s: %v", operation, err)
		}
		utility.LogMessage(fmt.Sprintf("Selected nodes for operation %s: %v", operation, selectedNodes))
		if stageIndex == 1 {
			customFilter = filter_op1
		} else if stageIndex == 2 {
			customFilter = filter_op2
		}
		utility.LogMessage(fmt.Sprintf("Selected nodes for operation %s: %v", operation, selectedNodes))

		for taskIndex, node := range selectedNodes {
			// Calculate line range for this task
			lineStart := taskIndex * linesPerTask
			lineEnd := (taskIndex + 1) * linesPerTask
			if lineEnd > totalLines {
				lineEnd = totalLines
			}

			checkHashForInputProcessing := false
			inputFilePath := srcFilePath
			outputFilePath := destFilePath
			if stageIndex == 0 {
				// source operation
				outputFilePath = srcFilePath + "_" + strconv.Itoa(stageIndex) + "_" + strconv.Itoa(taskIndex) + "_output"
			} else if stageIndex == 1 {
				// op1 operation
				inputFilePath = srcFilePath + "_" + strconv.Itoa(stageIndex) + "_" + strconv.Itoa(taskIndex) + "_input"
				outputFilePath = srcFilePath + "_" + strconv.Itoa(stageIndex) + "_" + strconv.Itoa(taskIndex) + "_output"
				checkHashForInputProcessing = true
			} else {
				// op2 operation
				inputFilePath = srcFilePath + "_" + strconv.Itoa(stageIndex) + "_" + strconv.Itoa(taskIndex) + "_input"
			}

			// Prepare NodeInUseInfo
			nodeInfo := NodeInUseInfo{
				Operation:       operation,
				InputFileName:   inputFilePath,
				TotalNumTasks:   int32(numTasks),
				OutputFileName:  outputFilePath,
				Stage:           int32(stageIndex),
				LineRangeStart:  int32(lineStart),
				LineRangeEnd:    int32(lineEnd),
				NodeId:          int32(taskIndex),
				AggregateOutput: checkHashForInputProcessing,
				LinesProcessed:  -1,
				CustomFilter:    customFilter,
			}

			checkpointInit := CheckpointStats{
				Stage:          int32(stageIndex),
				LinesProcessed: 0,
				TempFilename:   "",
				VmName:         node,
				TaskId:         int32(taskIndex),
				Operation:      operation,
			}
			stageTaskId := strconv.FormatInt(int64(stageIndex), 10) + "_" + strconv.FormatInt(int64(taskIndex), 10)

			// Update node usage information
			IncrementNodeTaskCount(node)
			SetNodeInUse(node, nodeInfo)
			// start a checkpoint data structure for this task
			UpdateNodeCheckpointStats(node, stageTaskId, checkpointInit)
			// Send scheduler request to the node
			go SendSchedulerRequest(node, nodeInfo)
		}

	}
	// TODO: Implement the logic to process tasks and write to destFilePath

	return nil
}

func SendSchedulerRequest(node string, nodeInstr NodeInUseInfo) error {
	// send request to each node in NodeInUse
	// to start processing the task
	serverIP := utility.GetIPAddr(node)
	conn, err := grpc.Dial(serverIP.String()+":"+scheduler_port, grpc.WithInsecure())
	if err != nil {
		utility.LogMessage("Unable to connect to server - ring rpc fserver - " + err.Error())
		return err
	}
	defer conn.Close()
	utility.LogMessage("created conn with server: " + node)

	// sedn request to worker node
	client := stormgrpc.NewStormWorkerClient(conn)

	// Prepare the request
	req := &stormgrpc.StormworkerRequest{
		Operation:       nodeInstr.Operation,
		InputFileName:   nodeInstr.InputFileName,
		NumTasks:        nodeInstr.TotalNumTasks,
		OutputFileName:  nodeInstr.OutputFileName,
		AggregateOutput: nodeInstr.AggregateOutput,
		Stage:           nodeInstr.Stage,
		RangeStart:      nodeInstr.LineRangeStart,
		RangeEnd:        nodeInstr.LineRangeEnd,
		TaskId:          nodeInstr.NodeId,
		LinesProcessed:  nodeInstr.LinesProcessed,
		CustomParam:     nodeInstr.CustomFilter,
	}
	utility.LogMessage("Sending request to server: " + node + " with (operation, taskid): " + nodeInstr.Operation + "," + strconv.FormatInt(int64(nodeInstr.NodeId), 10))
	// Call the PerformOperation RPC
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	resp, err := client.PerformOperation(ctx, req)
	if err != nil {
		log.Fatalf("Failed to perform operation: %v", err)
		return err
	}
	utility.LogMessage("Response from server: status=" + resp.Status + ", message=" + resp.Message)
	return nil
}
