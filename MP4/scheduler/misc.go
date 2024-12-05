package scheduler

import (
	"fmt"
	"math/rand"
	"rainstorm/utility"
	"strings"
)

func UpdateSchedulerMemeberList(node string, action string) error {
	switch action {
	case "Add":
		if GetAvailableNode(node) {
			return fmt.Errorf("error - Node %v already in List of Available Nodes ! cannot add", node)
		}

		//add to hashmap
		utility.LogMessage("Signal to add New node -" + node)
		SetAvailableNode(node, true)
		utility.LogMessage("Node added to AvailableNodes")

		// init NodeInUse entry
		// NodeInUse.mutex.Lock()
		// NodeInUse.nodes[node] = NodeInUseInfo{
		// 	CurrentStage:                0,
		// 	Stage1LineNoStart:           0,
		// 	Stage1LineNoEnd:             0,
		// 	AggregateStageKeyRangeStart: 0,
		// 	AggregateStageKeyRangeEnd:   0,
		// }
		// NodeInUse.mutex.Unlock()
		// utility.LogMessage("Node initialized in NodeInUse")

		// init NodeCheckpointStats entry
		// NodeCheckpointStats.mutex.Lock()
		// NodeCheckpointStats.stats[node] = make(map[string]CheckpointStats)
		// NodeCheckpointStats.mutex.Unlock()
		// utility.LogMessage("Node initialized in NodeCheckpointStats")

		return nil

	case "Delete": // remove from ring
		if !GetAvailableNode(node) {
			return fmt.Errorf("error - Node %v not in List of Available Nodes ! cannot delete", node)
		}

		// del from AvailableNodes
		SetAvailableNode(node, false)
		utility.LogMessage("Node removed from AvailableNodes")

		// handle Node which is being used
		NodeInUse.mutex.Lock()
		if _, inUse := NodeInUse.nodes[node]; inUse {
			utility.LogMessage("Node is currently in use. Handling deletion...")
			// TODO: implement logic to handle deletion of an in-use node

			// 1. Redistributing the node's current work to other available nodes
			// 1.5 Check whether checkpoint has the latest data by fetching the file and seeing line count matches
			// 2. Ensuring processing lines only once (Exactly-once delivery semantics) and continuity of operations
		}

		// TODO add check delete
		delete(NodeInUse.nodes, node)
		NodeInUse.mutex.Unlock()
		utility.LogMessage("Node removed from NodeInUse")

		// TODO add check delete
		// del from NodeCheckpointStats
		NodeCheckpointStats.mutex.Lock()
		delete(NodeCheckpointStats.stats, node)
		NodeCheckpointStats.mutex.Unlock()
		utility.LogMessage("Node removed from NodeCheckpointStats")

		return nil
	}
	return fmt.Errorf("invalid action: %s", action)

}

func SetAvailableNode(nodeName string, available bool) {
	AvailableNodes.mutex.Lock()
	defer AvailableNodes.mutex.Unlock()
	AvailableNodes.nodes[nodeName] = available
}

func GetAvailableNode(nodeName string) bool {
	AvailableNodes.mutex.RLock()
	defer AvailableNodes.mutex.RUnlock()
	return AvailableNodes.nodes[nodeName]
}

func SetNodeInUse(nodeName string, info NodeInUseInfo) {
	NodeInUse.mutex.Lock()
	defer NodeInUse.mutex.Unlock()
	NodeInUse.nodes[nodeName] = info
}

func UpdateNodeCheckpointStats(nodeName, checkpointName string, stats CheckpointStats) {
	NodeCheckpointStats.mutex.Lock()
	defer NodeCheckpointStats.mutex.Unlock()
	if _, exists := NodeCheckpointStats.stats[nodeName]; !exists {
		NodeCheckpointStats.stats[nodeName] = make(map[string]CheckpointStats)
	}
	NodeCheckpointStats.stats[nodeName][checkpointName] = stats
}

func PrintAvailableNodes() {
	AvailableNodes.mutex.RLock()
	defer AvailableNodes.mutex.RUnlock()

	var availableList, unavailableList []string
	for node, available := range AvailableNodes.nodes {
		if available {
			availableList = append(availableList, node)
		} else {
			unavailableList = append(unavailableList, node)
		}
	}

	utility.LogMessage("Available Nodes: " + strings.Join(availableList, ", "))
	utility.LogMessage("Unavailable Nodes: " + strings.Join(unavailableList, ", "))
}

func PrintNodeInUse() {
	NodeInUse.mutex.RLock()
	defer NodeInUse.mutex.RUnlock()

	for node, info := range NodeInUse.nodes {
		utility.LogMessage(fmt.Sprintf("Node in use: %s", node))
		utility.LogMessage(fmt.Sprintf("  Current Stage: %d", info.CurrentStage))
		utility.LogMessage(fmt.Sprintf("  Stage 1 Line Range: %d - %d", info.Stage1LineNoStart, info.Stage1LineNoEnd))
		utility.LogMessage(fmt.Sprintf("  Aggregate Node ID : %d", info.AggregateNodeId))
		utility.LogMessage(fmt.Sprintf("  Aggregate Stage Key Range: %d - %d", info.AggregateStageKeyRangeStart, info.AggregateStageKeyRangeEnd))
	}
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

func SelectRandomNodes(numTasks int) ([]string, error) {
	AvailableNodes.mutex.RLock()
	availableNodesList := make([]string, 0, len(AvailableNodes.nodes))
	for node, available := range AvailableNodes.nodes {
		if available {
			availableNodesList = append(availableNodesList, node)
		}
	}
	AvailableNodes.mutex.RUnlock()

	// Check if we have enough available nodes
	if len(availableNodesList) < numTasks {

		// not sure if we need to handle this here,
		// for now thinking to just handle it in UpdateSchedulerMemeberList
		return nil, fmt.Errorf("not enough available nodes: %d available, %d required", len(availableNodesList), numTasks)
	}

	// Choose random nodes
	selectedNodes := make([]string, numTasks)
	for i := 0; i < numTasks; i++ {
		index := rand.Intn(len(availableNodesList))
		selectedNodes[i] = availableNodesList[index]
		availableNodesList = append(availableNodesList[:index], availableNodesList[index+1:]...)
	}

	return selectedNodes, nil
}
