package scheduler

import (
	"fmt"
	"rainstorm/utility"
)

/* ------------------- AvailableNodes helper methods -------------------- */
func SetAvailableNode(nodeName string, taskCount int) {
	AvailableNodes.mutex.Lock()
	defer AvailableNodes.mutex.Unlock()
	AvailableNodes.nodes[nodeName] = taskCount
}

func GetAvailableNode(nodeName string) (int, bool) {
	AvailableNodes.mutex.RLock()
	defer AvailableNodes.mutex.RUnlock()
	taskCount, exists := AvailableNodes.nodes[nodeName]
	return taskCount, exists
}

func IncrementNodeTaskCount(nodeName string) {
	AvailableNodes.mutex.Lock()
	defer AvailableNodes.mutex.Unlock()
	AvailableNodes.nodes[nodeName]++
}

func DecrementNodeTaskCount(nodeName string) {
	AvailableNodes.mutex.Lock()
	defer AvailableNodes.mutex.Unlock()
	if AvailableNodes.nodes[nodeName] > 0 {
		AvailableNodes.nodes[nodeName]--
	}
}

/* -------------------- NodesInUse helper methods -------------------- */
func SetNodeInUse(nodeName string, info NodeInUseInfo) {
	NodeInUse.mutex.Lock()
	defer NodeInUse.mutex.Unlock()
	NodeInUse.nodes[nodeName] = append(NodeInUse.nodes[nodeName], info)
}

func RemoveNodeTask(nodeName string, operation string, nodeId int) {
	NodeInUse.mutex.Lock()
	defer NodeInUse.mutex.Unlock()
	if tasks, exists := NodeInUse.nodes[nodeName]; exists {
		updatedTasks := make([]NodeInUseInfo, 0)
		for _, task := range tasks {
			if task.Operation != operation || task.NodeId != int32(nodeId) {
				updatedTasks = append(updatedTasks, task)
			}
		}
		NodeInUse.nodes[nodeName] = updatedTasks
	}
}

func GetNodeTasks(nodeName string, operation string, nodeId int) []NodeInUseInfo {
	NodeInUse.mutex.RLock()
	defer NodeInUse.mutex.RUnlock()
	matchingTasks := make([]NodeInUseInfo, 0)
	if tasks, exists := NodeInUse.nodes[nodeName]; exists {
		for _, task := range tasks {
			if task.Operation == operation && task.NodeId == int32(nodeId) {
				matchingTasks = append(matchingTasks, task)
			}
		}
	}
	return matchingTasks
}

/* -------------------- Checkpointing helper methods ------------------*/
func UpdateNodeCheckpointStats(nodeName string, stageTaskid string, stats CheckpointStats) {
	NodeCheckpointStats.mutex.Lock()
	defer NodeCheckpointStats.mutex.Unlock()
	if _, exists := NodeCheckpointStats.stats[nodeName]; !exists {
		NodeCheckpointStats.stats[nodeName] = make(map[string]CheckpointStats)
	}
	NodeCheckpointStats.stats[nodeName][stageTaskid] = stats
}

/* ------------------- Printing lists of each - helpers ------------------*/
func PrintAvailableNodes() {
	fmt.Println("=== Available Nodes ===")
	AvailableNodes.mutex.RLock()
	defer AvailableNodes.mutex.RUnlock()
	NodeInUse.mutex.RLock()
	defer NodeInUse.mutex.RUnlock()

	for node, count := range AvailableNodes.nodes {
		fmt.Printf("Node: %s, Task Count: %d\n", node, count)
		if tasks, exists := NodeInUse.nodes[node]; exists {
			for i, task := range tasks {
				fmt.Printf(" %d - Operation: %s, Stage: %d, NodeId: %d\n", i+1, task.Operation, task.Stage, task.NodeId)
			}
		}
	}
}

func PrintNodesInUse() {
	utility.LogMessage("=== Nodes In Use ===")
	NodeInUse.mutex.RLock()
	defer NodeInUse.mutex.RUnlock()

	for node, infos := range NodeInUse.nodes {
		utility.LogMessage(fmt.Sprintf("Node: %s", node))
		for i, info := range infos {
			utility.LogMessage(fmt.Sprintf("  Task %d:", i+1))
			utility.LogMessage(fmt.Sprintf("    Operation: %s", info.Operation))
			utility.LogMessage(fmt.Sprintf("    InputFileName: %s", info.InputFileName))
			utility.LogMessage(fmt.Sprintf("    TotalNumTasks: %d", info.TotalNumTasks))
			utility.LogMessage(fmt.Sprintf("    OutputFileName: %s", info.OutputFileName))
			utility.LogMessage(fmt.Sprintf("    AggregateOutput: %v", info.AggregateOutput))
			utility.LogMessage(fmt.Sprintf("    Stage: %d", info.Stage))
			utility.LogMessage(fmt.Sprintf("    LineRangeStart: %d", info.LineRangeStart))
			utility.LogMessage(fmt.Sprintf("    LineRangeEnd: %d", info.LineRangeEnd))
			utility.LogMessage(fmt.Sprintf("    NodeId: %d", info.NodeId))
			utility.LogMessage(fmt.Sprintf("    LinesProcessed: %d", info.LinesProcessed))
		}
	}
	utility.LogMessage("=== End of Nodes In Use ===")
}

func PrintNodeCheckpointStats() {
	utility.LogMessage("=== Node Checkpoint Stats ===")
	NodeCheckpointStats.mutex.RLock()
	defer NodeCheckpointStats.mutex.RUnlock()

	for node, checkpoints := range NodeCheckpointStats.stats {
		utility.LogMessage(fmt.Sprintf("Node: %s", node))
		for stageTaskId, stats := range checkpoints {
			utility.LogMessage(fmt.Sprintf("  Stage_TaskId: %s", stageTaskId))
			utility.LogMessage(fmt.Sprintf("    Stage: %d", stats.Stage))
			utility.LogMessage(fmt.Sprintf("    LinesProcessed: %d", stats.LinesProcessed))
			utility.LogMessage(fmt.Sprintf("    TempFilename: %s", stats.TempFilename))
			utility.LogMessage(fmt.Sprintf("    VmName: %s", stats.VmName))
			utility.LogMessage(fmt.Sprintf("    TaskId: %d", stats.TaskId))
			utility.LogMessage(fmt.Sprintf("    Operation: %s", stats.Operation))
		}
	}
	utility.LogMessage("=== End of Node Checkpoint Stats ===")
}
