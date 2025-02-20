package scheduler

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"rainstorm/utility"
	"strings"
	"time"
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

/* ------------------- StageTask helper methods ------------------- */
func AddTask(stageID int32, taskID int32, node string) {
	StageTasks.mutex.Lock()
	defer StageTasks.mutex.Unlock()
	StageTasks.stages[stageID] = append(StageTasks.stages[stageID], RunningTaskInfo{TaskID: taskID, Node: node})
}

func GetTasksForStage(stageID int32) []RunningTaskInfo {
	StageTasks.mutex.RLock()
	defer StageTasks.mutex.RUnlock()
	return StageTasks.stages[stageID]
}

func RemoveTask(stageID int32, taskID int32, node string) {
	StageTasks.mutex.Lock()
	defer StageTasks.mutex.Unlock()
	tasks := StageTasks.stages[stageID]
	for i, task := range tasks {
		if (taskID != 0 && task.TaskID == taskID) || (node != "" && task.Node == node) {
			StageTasks.stages[stageID] = append(tasks[:i], tasks[i+1:]...)
			break
		}
	}
}

func UpdateTaskNode(stageID int32, taskID int32, newNodeID string) bool {
	StageTasks.mutex.Lock()
	defer StageTasks.mutex.Unlock()
	utility.LogMessage("INSIDE UPDATE TASK NODE, trying to reasign node value")
	tasks, exists := StageTasks.stages[stageID]
	if !exists {
		utility.LogMessage(fmt.Sprintf("Stage %d does not exist", stageID))
		return false
	}
	utility.LogMessage(fmt.Sprintf(" Tasks for  stage %d are %v", stageID, tasks))
	for i, task := range tasks {
		if task.TaskID == taskID {
			StageTasks.stages[stageID][i].Node = newNodeID
			utility.LogMessage(fmt.Sprintf("Stage %d Task %d updated to node %s", stageID, taskID, newNodeID))
			return true
		}
	}

	return false
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

// Cleanup Tasks
func CleanUpTaskCompletion(nodeName string, stage int32, taskId int32, operation string) {
	utility.LogMessage(fmt.Sprintf("Cleaning up Stage %d task %d on node %s", stage, taskId, nodeName))
	fmt.Printf("Time elapsed for Stage %d task %d on : %v\n", stage, taskId, time.Since(TimerStart))
	RemoveNodeTask(nodeName, operation, int(taskId))
	DecrementNodeTaskCount(nodeName)
	// remove the checkpoint stats
	RemoveTask(stage, taskId, nodeName)
	if len(GetTasksForStage(int32(2))) == 0 {
		utility.LogMessage("All tasks completed , finishing time calculation")
		elapsed := time.Since(TimerStart)
		fmt.Printf("Time taken for all streaming tasks to complete: %v\n", elapsed)
		if operation == "count" {
			savedPath := GetFileFromHydfs(DestinationFile)
			writeToPath := savedPath + "_condense"
			err := ProcessFile(savedPath, writeToPath)
			if err != nil {
				utility.LogMessage(fmt.Sprintf("Error processing file: %v", err))
			} else {
				utility.LogMessage(fmt.Sprintf("Processed file saved to %s", writeToPath))
			}
		}
	} else {
		utility.LogMessage("Tasks in stage 2 still running")
	}
	stageTaskId := fmt.Sprintf("%d_%d", stage, taskId)
	NodeCheckpointStats.mutex.Lock()
	defer NodeCheckpointStats.mutex.Unlock()
	delete(NodeCheckpointStats.stats[nodeName], stageTaskId)
}

// process output data for aggregate tasks

type MetaData struct {
	LineProcessed int `json:"lineProcessed"`
	Stage         int `json:"stage"`
	Task          int `json:"task"`
}

type JsonLine struct {
	Meta MetaData `json:"meta"`
	Data string   `json:"data"`
}

func ProcessFile(inputPath, outputPath string) error {
	inputFile, err := os.Open(inputPath)
	if err != nil {
		return fmt.Errorf("error opening input file: %w", err)
	}
	defer inputFile.Close()

	lastLines := make(map[string]string)
	scanner := bufio.NewScanner(inputFile)

	for scanner.Scan() {
		line := scanner.Text()
		var jsonLine JsonLine
		err := json.Unmarshal([]byte(line), &jsonLine)
		if err != nil {
			continue // Skip invalid JSON lines
		}

		if jsonLine.Meta.Stage == 2 && (jsonLine.Meta.Task == 0 || jsonLine.Meta.Task == 1 || jsonLine.Meta.Task == 2) {
			key := fmt.Sprintf("stage:%d,task:%d", jsonLine.Meta.Stage, jsonLine.Meta.Task)
			lastLines[key] = line
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading input file: %w", err)
	}

	outputFile, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("error creating output file: %w", err)
	}
	defer outputFile.Close()

	writer := bufio.NewWriter(outputFile)
	for key, line := range lastLines {
		parts := strings.Split(key, ",")
		writer.WriteString(fmt.Sprintf("%s\n%s\n", strings.Join(parts, ", "), line))
		writer.WriteString("\n")
	}

	if err := writer.Flush(); err != nil {
		return fmt.Errorf("error writing to output file: %w", err)
	}
	// Write to stdout instead of a file
	for key, line := range lastLines {
		parts := strings.Split(key, ",")
		fmt.Printf("%s\n%s\n\n", strings.Join(parts, ", "), line)
	}

	return nil
}
