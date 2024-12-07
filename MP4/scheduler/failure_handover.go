package scheduler

import (
	"fmt"
	"rainstorm/utility"
	"sort"
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
