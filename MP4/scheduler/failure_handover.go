package scheduler

import (
	"fmt"
	"rainstorm/utility"
	"sort"
)

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

/* -------------- Membership changes, Failure detection updates, handover of tasks -------------- */
func UpdateSchedulerMemeberList(node string, action string) error {
	switch action {
	case "Add":

		_, exists := GetAvailableNode(node)
		if exists {
			return fmt.Errorf("error - Node %v already in List of Available Nodes ! cannot add", node)
		}

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
			savedTasksToReassign := make([]NodeInUseInfo, 0)
			savedTasksToReassign = append(savedTasksToReassign, NodeInUse.nodes[node]...)
			utility.LogMessage(fmt.Sprintf("Saved %d tasks for reassignment", len(NodeInUse.nodes[node])))
			delete(NodeInUse.nodes, node)
			NodeInUse.mutex.Unlock()
			utility.LogMessage("Node removed from NodeInUse")

			// and also equal to CheckpointStats data structure

			NodeCheckpointStats.mutex.Lock()
			savedCheckpoints := make(map[string]CheckpointStats)
			if checkpoints, exists := NodeCheckpointStats.stats[node]; exists {
				for checkpointName, checkpointData := range checkpoints {
					savedCheckpoints[checkpointName] = checkpointData
				}
				utility.LogMessage(fmt.Sprintf("Saved %d checkpoints for reassignment", len(savedCheckpoints)))
			}
			delete(NodeCheckpointStats.stats, node)
			NodeCheckpointStats.mutex.Unlock()
			utility.LogMessage("Node removed from NodeCheckpointStats")

			// Reassign the tasks to other available nodes
			// update the nodes in use details with the checkpoint stats
			tasksToRestart := UpdateSavedTasksCheckpointStats(savedTasksToReassign, savedCheckpoints)

			err := RestartFailedTasks(tasksToRestart)
			if err != nil {
				return fmt.Errorf("error restarting failed tasks: %v", err)
			}
			// 1. Redistributing the node's current work to other available nodes
			// 1.5 Check whether checkpoint has the latest data by fetching the file and seeing line count matches
			// 2. Ensuring processing lines only once (Exactly-once delivery semantics) and continuity of operations

			return nil
		}
	}
	return fmt.Errorf("invalid action: %s", action)

}

func UpdateSavedTasksCheckpointStats(savedTasksToReassign []NodeInUseInfo, savedCheckpoints map[string]CheckpointStats) []NodeInUseInfo {
	updatedTasks := make([]NodeInUseInfo, len(savedTasksToReassign))

	for i, task := range savedTasksToReassign {
		checkpointName := fmt.Sprintf("%d_%d", task.Stage, task.NodeId)

		if checkpoint, exists := savedCheckpoints[checkpointName]; exists {
			task.LinesProcessed = checkpoint.LinesProcessed
			task.LineRangeStart = checkpoint.LinesProcessed
			task.State = checkpoint.State
		}

		updatedTasks[i] = task
	}

	return updatedTasks
}

func RestartFailedTasks(tasksToRestart []NodeInUseInfo) error {

	selectedNodes, err := SelectNodesWithLeastTasks(len(tasksToRestart))
	if err != nil {
		return fmt.Errorf("error selecting nodes for operation: %v", err)
	}

	utility.LogMessage(fmt.Sprintf("Attempting to restart %d failed tasks", len(tasksToRestart)))

	for i, task := range tasksToRestart {
		if i < len(selectedNodes) {
			node := selectedNodes[i]
			utility.LogMessage(fmt.Sprintf("Attempting to restart Stage,taskId : %d, %d  on node: %s", task.Stage, task.NodeId, node))

			// if check if needed by leader to verify the restart point
			// then ADD CHECK HERE, UPDATE before calling SendSchedulerRequest
			err := SendSchedulerRequest(node, task)

			if err != nil {

				// Update AvailableNodes
				IncrementNodeTaskCount(node)

				UpdateTaskNode(task.Stage, task.NodeId, node)

				// Update NodesInUse
				SetNodeInUse(node, task)

				// Update NodeCheckpointStats
				checkpointName := fmt.Sprintf("%d_%d", task.Stage, task.NodeId)
				checkpointStats := CheckpointStats{
					Stage:          task.Stage,
					LinesProcessed: task.LinesProcessed,
					TempFilename:   "", // Set appropriate temp filename if available
					VmName:         node,
					TaskId:         task.NodeId,
					Operation:      task.Operation,
					State:          task.State,
				}
				UpdateNodeCheckpointStats(node, checkpointName, checkpointStats)

				utility.LogMessage(fmt.Sprintf("Task successfully restarted on %s", node))
			} else {
				utility.LogMessage(fmt.Sprintf("Failed to restart task on %s", node))
				return fmt.Errorf("error restarting task on %s: %v", node, err)
			}
		} else {
			utility.LogMessage(fmt.Sprintf("Not enough available nodes to restart task %d", i+1))
			return fmt.Errorf("error: not enough available nodes to restart task")
		}
	}

	utility.LogMessage("Finished attempting to restart failed tasks")
	return nil
}
