package scheduler

import (
	"fmt"
	"rainstorm/utility"
	"sort"
	"strings"
)

// ------------------------------------- Failure detection updates, handover of tasks
func UpdateSchedulerMemeberList(node string, action string) error {
	switch action {
	case "Add":

		_, exists := GetAvailableNode(node)
		if exists {
			return fmt.Errorf("error - Node %v already in List of Available Nodes ! cannot add", node)
		}

		utility.LogMessage("Signal to add New node -" + node)
		SetAvailableNode(node, 0) // Initialize with 0 tasks
		utility.LogMessage("Node added to AvailableNodes")

		return nil

	case "Delete": // remove from ring
		taskCount, exists := GetAvailableNode(node)
		if !exists {
			return fmt.Errorf("error - Node %v not in List of Available Nodes ! cannot delete", node)
		}

		if taskCount == 0 {
			utility.LogMessage("Node can directly be removed from AvailableNodes, has no tasks assigned to it")
			// del from AvailableNodes
			AvailableNodes.mutex.Lock()
			delete(AvailableNodes.nodes, node)
			AvailableNodes.mutex.Unlock()
			utility.LogMessage("Node removed from AvailableNodes")
			return nil
		} else {
			utility.LogMessage("Node has tasks assigned to it. Handling deletion...")
			NodeInUse.mutex.Lock()
			// Before number removing nodes from NodeInUse, save the values it has
			// can be multiple tasks on this node, so number in AvailableNodes == num of Tasks in NodesInUse,
			// and also equal to CheckpointStats data structure

			delete(NodeInUse.nodes, node)
			NodeInUse.mutex.Unlock()
			utility.LogMessage("Node removed from NodeInUse")
			// TODO: implement logic to handle deletion of an in-use node

			// 1. Redistributing the node's current work to other available nodes
			// 1.5 Check whether checkpoint has the latest data by fetching the file and seeing line count matches
			// 2. Ensuring processing lines only once (Exactly-once delivery semantics) and continuity of operations

			// TODO add check delete
			// del from NodeCheckpointStats
			NodeCheckpointStats.mutex.Lock()
			delete(NodeCheckpointStats.stats, node)
			NodeCheckpointStats.mutex.Unlock()
			utility.LogMessage("Node removed from NodeCheckpointStats")

			return nil
		}
	}
	return fmt.Errorf("invalid action: %s", action)

}

// ------------------------------------- AvailableNodes helper methods
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

// ------------------------------------- NodesInUse helper methods
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
			if task.Operation != operation || task.NodeId != nodeId {
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
			if task.Operation == operation && task.NodeId == nodeId {
				matchingTasks = append(matchingTasks, task)
			}
		}
	}
	return matchingTasks
}

// ------------------------------------- Checkpointing helper methods
func UpdateNodeCheckpointStats(nodeName, checkpointName string, stats CheckpointStats) {
	NodeCheckpointStats.mutex.Lock()
	defer NodeCheckpointStats.mutex.Unlock()
	if _, exists := NodeCheckpointStats.stats[nodeName]; !exists {
		NodeCheckpointStats.stats[nodeName] = make(map[string]CheckpointStats)
	}
	NodeCheckpointStats.stats[nodeName][checkpointName] = stats
}

// ------------------------------------- Printing lists of each - helpers
func PrintAvailableNodes() {
	AvailableNodes.mutex.RLock()
	defer AvailableNodes.mutex.RUnlock()

	var nodeList []string
	for node, taskCount := range AvailableNodes.nodes {
		nodeList = append(nodeList, fmt.Sprintf("%s (%d tasks)", node, taskCount))
	}

	utility.LogMessage("Nodes and their task counts: " + strings.Join(nodeList, ", "))
}

func PrintNodeCheckpointStats() {
	NodeCheckpointStats.mutex.RLock()
	defer NodeCheckpointStats.mutex.RUnlock()

	for node, checkpoints := range NodeCheckpointStats.stats {
		utility.LogMessage(fmt.Sprintf("Node: %s", node))
		for checkpoint, stats := range checkpoints {
			utility.LogMessage(fmt.Sprintf("  Checkpoint: %s", checkpoint))
			utility.LogMessage(fmt.Sprintf("    Stage: %s", stats.Stage))
			utility.LogMessage(fmt.Sprintf("    Lines Processed: %d", stats.LinesProcessed))
			utility.LogMessage(fmt.Sprintf("    Temp Filename: %s", stats.TempFilename))
		}
	}
}

// Method to select nodes with the least number of tasks
func SelectNodesWithLeastTasks(numNodes int) ([]string, error) {
	AvailableNodes.mutex.RLock()
	defer AvailableNodes.mutex.RUnlock()

	if len(AvailableNodes.nodes) < numNodes {
		return nil, fmt.Errorf("not enough nodes available")
	}

	type nodeLoad struct {
		name  string
		tasks int
	}

	nodeLoads := make([]nodeLoad, 0, len(AvailableNodes.nodes))
	for name, tasks := range AvailableNodes.nodes {
		nodeLoads = append(nodeLoads, nodeLoad{name, tasks})
	}

	// Sort nodes by task count
	sort.Slice(nodeLoads, func(i, j int) bool {
		return nodeLoads[i].tasks < nodeLoads[j].tasks
	})

	selectedNodes := make([]string, numNodes)
	for i := 0; i < numNodes; i++ {
		selectedNodes[i] = nodeLoads[i].name
	}

	return selectedNodes, nil
}
