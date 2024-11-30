package stormworker

import (
	"errors"
	"fmt"
	"log"
	"net"
	"rainstorm/file_transfer"
	"rainstorm/utility"
	"sync"

	grpc "google.golang.org/grpc"
)

type Task struct {
	Operation            string
	StartRange           int
	EndRange             int
	CurrentProcessedLine int
	OutputHydfsFile      string
	LocalFilename        string
}

type liveness struct {
	alive bool
	mu    sync.Mutex
}

var (
	port       string       = "4001"
	tasks      map[int]Task //key is phase
	taskHealth map[string]liveness
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
func addTask(phase int, operation string, startRange, endRange int, hydfsFileName string) (map[int]Task, error) {
	// Error if phase/stage exists
	if _, exists := tasks[phase]; exists {
		return nil, errors.New("task for this phase already exists")
	}
	//new task
	newTask := Task{
		Operation:            operation,
		StartRange:           startRange,
		EndRange:             endRange,
		CurrentProcessedLine: -1, // Default value
		OutputHydfsFile:      hydfsFileName,
		LocalFilename:        fmt.Sprintf("%s_local_%d", hydfsFileName, phase),
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

func runTask(task Task, inputHydfsFile string) {

	// Get req from hydfs
	var request file_transfer.ClientData
	request.Operation = "get"
	request.LocalFilePath = task.LocalFilename
	request.Filename = inputHydfsFile

	// Get the file from hydfs
	fileID, senderIPs, _ := file_transfer.GetSuccesorIPsForFilename(inputHydfsFile)
	request.RingID = fileID
	responses := file_transfer.GetFileFromReplicas(senderIPs, request)
	latestResponse := file_transfer.ParseGetRespFromReplicas(responses, senderIPs[0], task.LocalFilename)
	utility.LogMessage("Filename : " + inputHydfsFile + "retrieved for RainStorm => timestamp : " + latestResponse.TimeStamp.String())

	// Start op_exe in go routine.
	RunOp_exe(task.LocalFilename, task.Operation, task.StartRange, task.EndRange)

	//infinite loop, while checking the task's status until completion.
}
