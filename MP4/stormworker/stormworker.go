package stormworker

import (
	"errors"
	"fmt"
	"log"
	"net"
	"path/filepath"
	"rainstorm/file_transfer"
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
	Phase                int
}

type liveness struct {
	alive bool
	mu    sync.Mutex
}

var (
	port                  string       = "4001"
	tasks                 map[int]Task //key is phase
	taskHealth            map[string]liveness
	STORM_LOCAL_FILE_PATH string = "/home/rainstorm"
)

// Waits for leader to -
// => Assign tasks, Run tasks (PULL file from Hydfs, APPEND/CREATE File in Hydfs)
// => Checkpoints + Commit to output file in Hydfs

func InitStormworker() {
	tasks = make(map[int]Task)
	taskHealth = make(map[string]liveness)

	// Start file rpc server for stormworker
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("Failed to listen on port: %v", err)
	}
	server := grpc.NewServer()
	RegisterStormWorkerServer(server, &StormorkerServer{})
	go func() {
		utility.LogMessage("RPC server goroutine entered for stormworker")
		if err := server.Serve(listener); err != nil {
			utility.LogMessage("Init Stormworker - server failure")
		}
	}()
}

// To get a file in local
// to append value to file in hydfs
// func test() {
// 	file_transfer.SendAppends()
// }

// function to add task. Return error/ if a task exists of same phase/stage.
func addTask(phase int, operation string, startRange, endRange int, outputHydfsFileName, inputHydfsFilename string) (map[int]Task, error) {
	// Error if phase/stage exists
	if _, exists := tasks[phase]; exists {
		return nil, errors.New("task for this phase already exists")
	}

	// DIR created
	local_file_location := filepath.Join(STORM_LOCAL_FILE_PATH, fmt.Sprintf("%s_local_%d", inputHydfsFilename, phase))

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
		Phase:                phase,
	}
	// Add the task to the in-mem dict
	tasks[phase] = newTask
	return tasks, nil
}

// function to delete task after completion
func deleteTask(phase int) error {
	if tasks == nil {
		return errors.New("no tasks available to delete")
	}
	// Check if the phase/key exists in the map
	if _, exists := tasks[phase]; !exists {
		return errors.New("task with the given phase does not exist")
	}
	delete(tasks, phase)
	return nil
}

// function to update the current processed line
func updateCurrentProcessedLine(phase int, currentLine int) error {
	// error check
	if tasks == nil {
		return errors.New("no tasks available to update")
	}
	task, exists := tasks[phase]
	if !exists {
		return errors.New("task with the given phase does not exist")
	}

	//update
	task.CurrentProcessedLine = currentLine
	tasks[phase] = task
	return nil
}

func setTaskRunning(phase int, status bool) {
	if tasks == nil {
		utility.LogMessage(fmt.Sprintf("NO TASKS EXIST, but Task of %d phase's Run status was requested to be changed!!", phase))
		return
	}
	task, exists := tasks[phase]
	if !exists {
		utility.LogMessage(fmt.Sprintf("Task with the given phase- %d - does not exist", phase))
	}
	//update
	task.Running = status
	tasks[phase] = task
}

func setTaskCompletion(phase int, status bool, message string) {
	if tasks == nil {
		utility.LogMessage(fmt.Sprintf("NO TASKS EXIST, but Task of %d phase's Run status was requested to be changed!!", phase))
		return
	}
	task, exists := tasks[phase]
	if !exists {
		utility.LogMessage(fmt.Sprintf("Task with the given phase- %d - does not exist", phase))
	}
	//update
	task.Running = status
	task.Message = message
	tasks[phase] = task
}

func setTaskFailure(phase int, status bool, message string) {
	if tasks == nil {
		utility.LogMessage(fmt.Sprintf("NO TASKS EXIST, but Task of %d phase's Run status was requested to be changed!!", phase))
		return
	}
	task, exists := tasks[phase]
	if !exists {
		utility.LogMessage(fmt.Sprintf("Task with the given phase- %d - does not exist", phase))
	}
	//update
	task.Failed = status
	task.Message = message
	tasks[phase] = task
}

// function to get current processed line
func GetCurrentProcessedLine(phase int) (int, error) {
	// error check
	if tasks == nil {
		return -1, errors.New("no tasks available to update")
	}
	task, exists := tasks[phase]
	if !exists {
		return -1, errors.New("task with the given phase does not exist")
	}

	return task.CurrentProcessedLine, nil
}

/*
MAIN STORMWORKER FUNCTIONS
*/

func stormworker() {
	for {
		for phase, task := range tasks {
			// send a message to leader with status

			//IF failed, send status, then delete task.

			//IF process current line has not changed since last X checks, mark failed, send signal to stop. Tell leader (? or restart)
		}
		time.Sleep(time.Millisecond * 100)
	}
}

func runTask(task Task) {

	for {
		if task.Completed { // only can be set by command from leader
			// Send signal of ending task to leader!!!!!!!!!!!

			//Delete task of the phase
			deleteTask(task.Phase)
			return
		} // ELSE, keep running

		if task.Failed {
			// Send signal to leader on stall issue => task.Message
			deleteTask((task.Phase))
			return // change behaviour to retry later, for now fail
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
		RunOp_exe(task.LocalFilepath, task.Operation, task.CurrentProcessedLine, task.EndRange, task.Phase)

		// Wait for 1.5 seconds (? test with different times, we wait for more inputs to come in ?)
		time.Sleep(time.Millisecond * 1500)

	} //infinite loop, while checking the task's status until completion.
}
