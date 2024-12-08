package stormworker

import (
	"errors"
	"fmt"
	"log"
	"net"
	"path/filepath"
	"rainstorm/file_transfer"
	"rainstorm/stormgrpc"
	"rainstorm/utility"
	"sync"
	"time"

	grpc "google.golang.org/grpc"
)

type Task struct {
	Operation            string
	StartRange           int
	EndRange             int
	CurrentProcessedLine int
	OutputHydfsFile      string
	InputHydfsFile       string
	LocalFilepath        string
	Completed            bool
	Running              bool
	Failed               bool
	Message              string
	Stage                int
	TASK_ID              int
	pagelineoffset       int
	aggregate_output     bool
	num_tasks            int
	processedMap         map[taskKey]int
	customParams         string
	buffer               map[string][]string
	state                string
	linesOut             int
}

type liveness struct {
	alive bool
	mu    sync.Mutex
}

type taskKey struct {
	stage, task int
}

var (
	port                  string           = "4001"
	tasks                 map[taskKey]Task //key is phase
	taskHealth            map[string]liveness
	STORM_LOCAL_FILE_PATH string = "/home/hydfs"
	LEADER_HOSTNAME       string = "fa24-cs425-5901.cs.illinois.edu"
	BUFFER_SIZE           int    = 10
)

// Waits for leader to -
// => Assign tasks, Run tasks (PULL file from Hydfs, APPEND/CREATE File in Hydfs)
// => Checkpoints + Commit to output file in Hydfs

func InitStormworker() {
	tasks = make(map[taskKey]Task)
	taskHealth = make(map[string]liveness)

	// Start file rpc server for stormworker
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("Failed to listen on port: %v", err)
	}
	server := grpc.NewServer()
	stormgrpc.RegisterStormWorkerServer(server, &StormworkerServer{})
	go func() {
		utility.LogMessage("RPC server goroutine entered for stormworker")
		if err := server.Serve(listener); err != nil {
			utility.LogMessage("Init Stormworker - server failure")
		}
	}()

	// go stormworker()
}

// To get a file in local
// to append value to file in hydfs
// func test() {
// 	file_transfer.SendAppends()
// }

// function to add task. Return error/ if a task exists of same phase/stage.
func AddTask(stage int, task_id int, operation string, startRange, endRange int, outputHydfsFileName, inputHydfsFilename string, aggregate_output bool, num_tasks int, customParam string, state string) (Task, error) {
	// Error if phase/stage exists
	tkey := taskKey{stage: stage, task: task_id}
	if _, exists := tasks[tkey]; exists {
		return Task{}, errors.New("task for this phase already exists")
	}

	// DIR created
	local_file_location := filepath.Join(STORM_LOCAL_FILE_PATH, fmt.Sprintf("%s_local_%d_%d", inputHydfsFilename, stage, task_id))

	//new task
	newTask := Task{
		Operation:            operation,
		StartRange:           startRange,
		EndRange:             endRange,
		CurrentProcessedLine: startRange, // Default value
		OutputHydfsFile:      outputHydfsFileName,
		InputHydfsFile:       inputHydfsFilename,
		LocalFilepath:        local_file_location,
		Completed:            false,
		Running:              false,
		Failed:               false,
		Stage:                stage,
		TASK_ID:              task_id,
		aggregate_output:     aggregate_output,
		num_tasks:            num_tasks,
		processedMap:         make(map[taskKey]int),
		customParams:         customParam,
		buffer:               make(map[string][]string),
		state:                state,
		linesOut:             0,
	}
	// Add the task to the in-mem dict
	tasks[tkey] = newTask
	return newTask, nil
}

// function to delete task after completion
func deleteTask(stage, task_id int) error {
	if tasks == nil {
		return errors.New("no tasks available to delete")
	}
	// Check if the phase/key exists in the map
	tkey := taskKey{stage: stage, task: task_id}
	if _, exists := tasks[tkey]; !exists {
		return errors.New("task with the given phase does not exist")
	}
	delete(tasks, tkey)
	return nil
}

// function to update the current processed line
func updateCurrentProcessedLine(stage, task_id, currentLine int) error {
	// error check
	if tasks == nil {
		return errors.New("no tasks available to update")
	}
	tkey := taskKey{stage: stage, task: task_id}
	task, exists := tasks[tkey]
	if !exists {
		return errors.New("task with the given phase does not exist")
	}

	//update
	utility.LogMessage(fmt.Sprintf("START RANGE UPDATED TO %d", currentLine))
	task.CurrentProcessedLine = currentLine
	task.StartRange = currentLine
	tasks[tkey] = task
	return nil
}

func setTaskRunning(stage, task_id int, status bool) {
	if tasks == nil {
		utility.LogMessage(fmt.Sprintf("NO TASKS EXIST, but Task of %d stage's Run status was requested to be changed!!", stage))
		return
	}
	tkey := taskKey{stage: stage, task: task_id}
	task, exists := tasks[tkey]
	if !exists {
		utility.LogMessage(fmt.Sprintf("Task with the given stage- %d and task id - %d - does not exist", stage, task_id))
	}
	//update
	task.Running = status
	tasks[tkey] = task
}

func setLinesout(stage, task_id int, value int) {
	if tasks == nil {
		utility.LogMessage(fmt.Sprintf("NO TASKS EXIST, but Task of %d stage's Run status was requested to be changed!!", stage))
		return
	}
	tkey := taskKey{stage: stage, task: task_id}
	task, exists := tasks[tkey]
	if !exists {
		utility.LogMessage(fmt.Sprintf("Task with the given stage- %d and task id - %d - does not exist", stage, task_id))
	}
	//update
	task.linesOut += value
	tasks[tkey] = task
}

func getLinesout(stage, task_id int) int {
	if tasks == nil {
		utility.LogMessage(fmt.Sprintf("NO TASKS EXIST, but Task of %d stage's Run status was requested to be changed!!", stage))
		return -1
	}
	tkey := taskKey{stage: stage, task: task_id}
	task, exists := tasks[tkey]
	if !exists {
		utility.LogMessage(fmt.Sprintf("Task with the given stage- %d and task id - %d - does not exist", stage, task_id))
	}
	return task.linesOut
}

func setState(stage, task_id int, state string) {
	if tasks == nil {
		utility.LogMessage(fmt.Sprintf("NO TASKS EXIST, but Task of %d stage's Run status was requested to be changed!!", stage))
		return
	}
	tkey := taskKey{stage: stage, task: task_id}
	task, exists := tasks[tkey]
	if !exists {
		utility.LogMessage(fmt.Sprintf("Task with the given stage- %d and task id - %d - does not exist", stage, task_id))
	}
	//update
	task.state = state
	tasks[tkey] = task
}

func getState(stage, task_id int) string {
	tkey := taskKey{stage: stage, task: task_id}
	task, exists := tasks[tkey]
	if !exists {
		utility.LogMessage(fmt.Sprintf("Task with the given stage- %d and task id - %d - does not exist", stage, task_id))
	}
	return task.state
}

func setTaskCompletion(stage, task_id int, status bool, message string) {
	if tasks == nil {
		utility.LogMessage(fmt.Sprintf("NO TASKS EXIST, but Task of %d tasks's Run status was requested to be changed!!", stage))
		return
	}
	tkey := taskKey{stage: stage, task: task_id}
	task, exists := tasks[tkey]
	if !exists {
		utility.LogMessage(fmt.Sprintf("Task with the given stage- %d and task id - %d - does not exist", stage, task_id))
	}
	//update
	task.Running = status
	task.Message = message
	tasks[tkey] = task
}

func setTaskFailure(stage, task_id int, status bool, message string) {
	if tasks == nil {
		utility.LogMessage(fmt.Sprintf("NO TASKS EXIST, but Task of %d phase's Run status was requested to be changed!!", stage))
		return
	}
	tkey := taskKey{stage: stage, task: task_id}
	task, exists := tasks[tkey]
	if !exists {
		utility.LogMessage(fmt.Sprintf("Task with the given stage- %d and task id - %d - does not exist", stage, task_id))
	}
	//update
	task.Failed = status
	task.Message = message
	tasks[tkey] = task
}

// function to get current processed line
func getCurrentProcessedLine(stage, task_id int) map[taskKey]int {
	if tasks == nil {
		utility.LogMessage(fmt.Sprintf("NO TASKS EXIST, but Task of %d phase's Run status was requested to be changed!!", stage))
	}
	tkey := taskKey{stage: stage, task: task_id}
	task, exists := tasks[tkey]
	if !exists {
		utility.LogMessage(fmt.Sprintf("Task with the given stage- %d and task id - %d - does not exist", stage, task_id))
	}

	return task.processedMap
	//update
}

func SetProccessedLine(stage, task_id int, pkey taskKey, line int) {
	if tasks == nil {
		utility.LogMessage(fmt.Sprintf("NO TASKS EXIST, but Task of %d phase's Run status was requested to be changed!!", stage))
		return
	}
	tkey := taskKey{stage: stage, task: task_id}
	task, exists := tasks[tkey]
	if !exists {
		utility.LogMessage(fmt.Sprintf("Task with the given stage- %d and task id - %d - does not exist", stage, task_id))
	}
	//update
	task.processedMap[pkey] = line
	tasks[tkey] = task
}

/*
MAIN STORMWORKER FUNCTIONS
*/

// func stormworker() {
// 	for {
// 		for tkey, task := range tasks {
// 			// send a message to leader with status
// 			sendCheckpointStatus(tkey.stage, tkey.task, task.CurrentProcessedLine, task.OutputHydfsFile, task.Operation)

// 			//IF failed, send status, then delete task.

// 			//IF process current line has not changed since last X checks, mark failed, send signal to stop. Tell leader (? or restart)
// 		}
// 		time.Sleep(time.Millisecond * 100)
// 	}
// }

func RunTask(task Task) {
	utility.LogMessage(fmt.Sprint("Starting Worker Node for stage: %d => ID:%d, operation: %v, inputfile: %v, outputfile: %v", task.state, task.TASK_ID, task.Operation, task.InputHydfsFile, task.OutputHydfsFile))

	for {
		tkey := taskKey{stage: task.Stage, task: task.TASK_ID}
		task, exists := tasks[tkey]
		if !exists {
			utility.LogMessage("Task not found")
		}
		// utility.LogMessage(fmt.Sprintf("NEW LOOP STARTED for %d.%d Line processed - %d, StartRange - %d, EndRange - %d", task.Stage, task.TASK_ID, task.CurrentProcessedLine, task.StartRange, task.EndRange))
		if task.Completed { // only can be set by command from leader
			// Send signal of ending task to leader!!!!!!!!!!!

			//Delete task of the phase
			deleteTask(task.Stage, task.TASK_ID)
			return
		} // ELSE, keep running

		if task.Failed {
			// Send signal to leader on stall issue => task.Message
			deleteTask(task.Stage, task.TASK_ID)
			return // change behaviour to retry later, for now fail
		}

		if task.StartRange == task.EndRange {
			utility.LogMessage("task deleted after start range == end range")
			deleteTask(task.Stage, task.TASK_ID)
			return
		}

		// Get req from hydfs
		var request file_transfer.ClientData
		request.Operation = "get"
		request.LocalFilePath = task.LocalFilepath
		request.Filename = task.InputHydfsFile

		// Get the file from hydfs
		fileID, senderIPs, _ := file_transfer.GetSuccesorIPsForFilename(task.InputHydfsFile)
		request.RingID = fileID
		responses := file_transfer.GetFileFromReplicas(senderIPs, request)
		// REPLACES THE LOCAL FILE ?
		latestResponse := file_transfer.ParseGetRespFromReplicas(responses, senderIPs[0], task.LocalFilepath)
		utility.LogMessage("Filename : " + task.InputHydfsFile + "retrieved for RainStorm => timestamp : " + latestResponse.TimeStamp.String())

		// Start op_exe in go routine for existing lines of code
		// RunOp_exe(task.LocalFilepath, task.OutputHydfsFile, task.Operation, task.CurrentProcessedLine, task.EndRange, task.Stage, task.TASK_ID, task.aggregate_output, task.num_tasks)
		RunOperation(task)
		// Wait for 1.5 seconds (? test with different times, we wait for more inputs to come in ?)
		time.Sleep(time.Millisecond * 1500)

	} //infinite loop, while checking the task's status until completion.
}
