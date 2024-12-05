package scheduler

import (
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
	nodes map[string]bool
	mutex sync.RWMutex
}

type NodeInUseInfo struct {
	CurrentStage                int
	Stage1LineNoStart           int
	Stage1LineNoEnd             int
	AggregateNodeId             int
	AggregateStageKeyRangeStart int
	AggregateStageKeyRangeEnd   int
}

type NodeInUseStruct struct {
	nodes map[string]NodeInUseInfo
	mutex sync.RWMutex
}

type CheckpointStats struct {
	Stage          string
	LinesProcessed int
	TempFilename   string
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
		nodes: make(map[string]bool),
	}
	NodeInUse = NodeInUseStruct{
		nodes: make(map[string]NodeInUseInfo),
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

func StartScheduler(srcFilePath string, numTasks int, destFilePath string) error {

	//total lines in source file
	totalLines, err := utility.FileLineCount(srcFilePath)
	if err != nil {
		errMsg := "error counting lines in source file: " + err.Error()
		utility.LogMessage(errMsg)
		return err
	}

	//  choose numTasks nodes at random from AvailableNodes
	rand.Seed(time.Now().UnixNano())

	// get available nodes
	selectedNodes, err := SelectRandomNodes(numTasks)
	if err != nil {
		errMsg := "error selecting random nodes: " + err.Error()
		utility.LogMessage(errMsg)
	}

	utility.LogMessage("Selected Nodes:")
	for i, node := range selectedNodes {
		utility.LogMessage("  Node  " + strconv.Itoa(i+1) + " : " + node)
	}

	// calc lines per task
	linesPerTask := int(math.Ceil(float64(totalLines) / float64(numTasks)))

	// log task information
	utility.LogMessage("Total lines: " + strconv.Itoa(totalLines))
	utility.LogMessage("Lines per task: " + strconv.Itoa(linesPerTask))

	// make this as a function
	// update AvailableNodes, initialize NodesInUse and NodeCheckpointStats
	for i, node := range selectedNodes {
		// Update AvailableNodes
		SetAvailableNode(node, false)

		line_start := i * linesPerTask
		line_end := (i + 1) * linesPerTask
		if line_end > totalLines {
			line_end = totalLines
		}

		// populate the NodeInUse, get ready to start sending requests to Scheduler Listener on VMs

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
}
