package stormworker

import (
	"bufio"
	"bytes"
	"embed"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"rainstorm/file_transfer"
	"rainstorm/utility"
	"strconv"
	"strings"
)

//go:embed operators/op_exe/*
var operatorFS embed.FS

func RunOperation(task Task) {
	//Set status
	setTaskRunning(task.Stage, task.TASK_ID, true)
	defer setTaskRunning(task.Stage, task.TASK_ID, false)

	binaryPath := filepath.Join("operators/op_exe", task.Operation)
	binaryData, err := operatorFS.ReadFile(binaryPath)
	if err != nil {
		utility.LogMessage("Error locating binary: " + err.Error() + ", EXITING OP_EXE for op: " + task.Operation)
		return
	}

	// Create temp file for run
	exe_file_name := task.Operation + "_" + strconv.Itoa(task.Stage) + "_" + strconv.Itoa(task.TASK_ID)
	temp_exe, err := os.CreateTemp(os.TempDir(), exe_file_name)
	if err != nil {
		utility.LogMessage("Error creating temp EXE")
	}
	//Defer deletion of time
	defer os.Remove(temp_exe.Name())

	// Write to temp file, close, chmod
	if _, err := temp_exe.Write(binaryData); err != nil {
		utility.LogMessage(fmt.Sprintf("Error writing to temporary file: %v\n", err))
		return
	}
	if err := temp_exe.Close(); err != nil {
		utility.LogMessage(fmt.Sprintf("Error closing temporary file: %v\n", err))
		return
	}
	if err := os.Chmod(temp_exe.Name(), 0755); err != nil {
		utility.LogMessage(fmt.Sprintf("Error making temporary file executable: %v\n", err))
		return
	}

	// Open the local file for reading
	inputFile, err := os.Open(task.LocalFilepath)
	if err != nil {
		utility.LogMessage(fmt.Sprintf("Error opening input file: %v\n", err))
		return
	}
	defer inputFile.Close()

	// Process lines starting from the offset
	scanner := bufio.NewScanner(inputFile)
	lineNumber := 0
	bufferSize := 0
	outputFilename := task.OutputHydfsFile
	for scanner.Scan() {
		lineNumber++
		if lineNumber < task.StartRange {
			continue
		}
		if task.EndRange != -1 && lineNumber > task.EndRange {
			break
		}

		line := scanner.Text()
		inputLine := line
		pkey := taskKey{}

		//Process Text into meta and data.
		if task.Operation == "source" { // Then there will not be any meta for first stage.
			var inputData struct {
				Filename   string `json:"filename"`
				Linenumber int    `json:"linenumber"`
				Content    string `json:"content"`
			}

			// Initialize the fields
			inputData.Linenumber = lineNumber
			inputData.Filename = task.InputHydfsFile
			inputData.Content = line

			// Convert the struct to JSON
			inputJson, err := json.Marshal(inputData) //Get line to be fed to operators.
			if err != nil {
				utility.LogMessage(fmt.Sprintf("Error marshalling JSON: %v\n", err))
				return
			}
			inputLine = string(inputJson)
		} else if task.Operation == "count" {
			var lineData struct {
				Meta struct {
					LineProcessed int `json:"lineProcessed"`
					Stage         int `json:"stage"`
					Task          int `json:"task"`
				} `json:"meta"`
				Data string `json:"data"`
			}

			err = json.Unmarshal([]byte(line), &lineData)
			if err != nil {
				continue
			}

			procKey := taskKey{task: lineData.Meta.Task, stage: lineData.Meta.Stage}
			processed := getCurrentProcessedLine(task.Stage, task.TASK_ID)
			if value, exists := processed[procKey]; exists {
				if value >= lineNumber {
					utility.LogMessage("DUPLICATE ENTRFY FOUND - skipping line => " + line)
					continue
				}
				pkey = procKey
			}

			var inputData struct {
				State  string `json:"state"`
				Params string `json:"params"`
				Data   string `json:"data"`
			}
			inputData.Data = lineData.Data
			inputData.State = getState(task.Stage, task.TASK_ID)
			inputData.Params = task.customParams
			inputJson, err := json.Marshal(inputData) //Get line to be fed to operators.
			if err != nil {
				utility.LogMessage(fmt.Sprintf("Error marshalling JSON:%v\n", err))
				return
			}
			inputLine = string(inputJson)
		} else {
			var lineData struct {
				Meta struct {
					LineProcessed int `json:"lineProcessed"`
					Stage         int `json:"stage"`
					Task          int `json:"task"`
				} `json:"meta"`
				Data string `json:"data"`
			}

			err = json.Unmarshal([]byte(line), &lineData)
			if err != nil {
				continue
			}

			procKey := taskKey{task: lineData.Meta.Task, stage: lineData.Meta.Stage}
			if value, exists := getCurrentProcessedLine(task.Stage, task.TASK_ID)[procKey]; exists {
				if value >= lineNumber {
					utility.LogMessage(fmt.Sprintf("DUPLICATE ENTRFY FOUND - skipping line =>, INMEMLINE PROCESSED for Stage-%d Task-%d, is %d. Currently seen line value - %d ", lineData.Meta.Task, lineData.Meta.Stage, value, lineNumber))
					continue
				}
				pkey = procKey
			}

			var inputData struct {
				Params string `json:"params"`
				Data   string `json:"data"`
			}
			inputData.Data = lineData.Data
			inputData.Params = task.customParams
			inputJson, err := json.Marshal(inputData) //Get line to be fed to operators.
			if err != nil {
				utility.LogMessage(fmt.Sprintf("Error marshalling JSON:%v\n", err))
				return
			}
			inputLine = string(inputJson)
			utility.LogMessage(fmt.Sprintf("Parsing line - %d from input, operation %v =>"+inputLine, lineNumber, task.Operation))
		}

		// Execute the binary with the line as input
		cmd := exec.Command(temp_exe.Name())
		cmd.Stdin = strings.NewReader(inputLine)
		var output bytes.Buffer
		cmd.Stdout = &output
		cmd.Stderr = &output
		err := cmd.Run()
		if err != nil {
			utility.LogMessage(fmt.Sprintf("Error executing binary on line %d: %v\nOutput: %s\n", lineNumber, err, output.String()))
			continue
		}

		//update processed with pkey
		SetProccessedLine(task.Stage, task.TASK_ID, pkey, lineNumber)

		//process output content
		var outputContent struct {
			Meta struct {
				LineProcessed int `json:"lineProcessed"`
				Stage         int `json:"stage"`
				Task          int `json:"task"`
			} `json:"meta"`
			Data string `json:"data"`
		}
		outputContent.Data = output.String()
		outputContent.Meta.LineProcessed = lineNumber
		outputContent.Meta.Stage = task.Stage
		outputContent.Meta.Task = task.TASK_ID
		outputContentJson, err := json.Marshal(outputContent)
		if err != nil {
			utility.LogMessage(fmt.Sprintf("Error marshalling JSON: %v\n", err))
			return
		}

		if task.aggregate_output {
			aggregate_val := utility.KeyMurmurHash(outputContent.Data, task.num_tasks)
			vals := strings.Split(outputFilename, "_")
			vals[len(vals)-1] = strconv.FormatUint(uint64(aggregate_val), 10)
			outputFilename = strings.Join(vals, "_")
		}

		setState(task.Stage, task.TASK_ID, outputContent.Data)

		// Only every N batches
		task.buffer[outputFilename] = append(task.buffer[outputFilename], string(outputContentJson))
		bufferSize += 1
		if bufferSize == BUFFER_SIZE {
			for outfile, values := range task.buffer {
				err = writeToHydfs(values, outfile, task.Stage, task.TASK_ID)
				if err != nil {
					utility.LogMessage("batch write errored - err:>>>> " + err.Error())
				}
				sendCheckpointStatus(task.Stage, task.TASK_ID, lineNumber, outfile, task.Operation, getState(task.Stage, task.TASK_ID))
			}
			setLinesout(task.Stage, task.TASK_ID, bufferSize)
			bufferSize = 0
			for key := range task.buffer {
				task.buffer[key] = []string{}
			}

		}
		updateCurrentProcessedLine(task.Stage, task.TASK_ID, lineNumber)
		utility.LogMessage(fmt.Sprintf("Line %d output: %s\n", lineNumber, output.String()))
	}

	if err := scanner.Err(); err != nil {
		utility.LogMessage(fmt.Sprintf("Error reading input file: %v\n", err))
	}

	if bufferSize > 0 {
		for outfile, values := range task.buffer {
			err = writeToHydfs(values, outfile, task.Stage, task.TASK_ID)
			if err != nil {
				utility.LogMessage("batch write errored - err:>>>> " + err.Error())
			}
			sendCheckpointStatus(task.Stage, task.TASK_ID, lineNumber, outfile, task.Operation, getState(task.Stage, task.TASK_ID))
		}
		setLinesout(task.Stage, task.TASK_ID, bufferSize)
		bufferSize = 0
		for key := range task.buffer {
			task.buffer[key] = []string{}
		}
	}

}

// calculate output file name - for aggregate
func writeToHydfs(values []string, outputfile string, stage, task_id int) error {
	local_temp_file := strconv.Itoa(stage) + "_" + strconv.Itoa(task_id) + "_" + "temp"
	tempFile, err := os.CreateTemp("", local_temp_file)
	if err != nil {
		utility.LogMessage(fmt.Sprintf("Error creating temporary file: %v\n", err))
		return err
	}
	defer tempFile.Close()
	defer func() {
		if err := os.Remove(tempFile.Name()); err != nil {
			utility.LogMessage(fmt.Sprintf("Error deleting temporary file: %v", err))
		}
	}()

	utility.LogMessage(fmt.Sprintf("Temporary file created: %s", tempFile.Name()))

	// Write each value to the temporary file
	writer := bufio.NewWriter(tempFile)
	for _, value := range values {
		_, err := writer.WriteString(value + "\n")
		if err != nil {
			utility.LogMessage(fmt.Sprintf("Error writing to temporary file: %v", err))
			return err
		}
	}

	// Flush any buffered data to the file
	if err := writer.Flush(); err != nil {
		utility.LogMessage(fmt.Sprintf("Error flushing to temporary file: %v", err))
		return err
	}

	utility.LogMessage("Data written to temporary file successfully.")

	//req to HYDFS
	req := file_transfer.ClientData{Operation: "append", LocalFilePath: tempFile.Name(), Filename: outputfile}
	file_transfer.SendAppends(req, outputfile)
	return nil
}
