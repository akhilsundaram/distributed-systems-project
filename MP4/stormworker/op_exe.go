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

var local_temp_file = "/home/rainstorm/local/temp"

//go:embed operators/op_exe/*
var operators embed.FS

func RunOp_exe(inputFilename, outputFilename, operation string, offset int, end int, stage, task_id int, aggregate_output bool, num_tasks int) {
	//Set status
	setTaskRunning(stage, task_id, true)
	defer setTaskRunning(stage, task_id, false)

	// Get OP_EXE
	binaryPath := filepath.Join("operators/op_exe", operation)
	binaryFile, err := operators.Open(binaryPath)
	if err != nil {
		utility.LogMessage("Error locating binary: " + err.Error() + ", EXITING OP_EXE for op: " + operation)
		return
	}
	defer binaryFile.Close()

	//Run in a temp location
	tempBinaryPath := fmt.Sprintf("/tmp/%s_%d_%d", operation, stage, task_id)
	tempBinary, err := os.Create(tempBinaryPath)
	if err != nil {
		fmt.Printf("Error creating temp binary file: %v\n", err)
		return
	}
	defer func() {
		tempBinary.Close()
		os.Remove(tempBinaryPath)
	}()

	// Copy the binary to temp/FS
	if _, err := tempBinary.ReadFrom(binaryFile); err != nil {
		fmt.Printf("Error writing to temp binary file: %v\n", err)
		return
	}
	if err := os.Chmod(tempBinaryPath, 0755); err != nil {
		fmt.Printf("Error setting executable permissions: %v\n", err)
		return
	}

	// Open the local file for reading
	inputFile, err := os.Open(inputFilename)
	if err != nil {
		fmt.Printf("Error opening input file: %v\n", err)
		return
	}
	defer inputFile.Close()

	// Process lines starting from the offset
	scanner := bufio.NewScanner(inputFile)
	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		if lineNumber < offset {
			continue
		}
		if end != -1 && lineNumber > end {
			break
		}

		line := scanner.Text()

		// Execute the binary with the line as input
		cmd := exec.Command(tempBinaryPath)
		cmd.Stdin = strings.NewReader(line)
		var output bytes.Buffer
		cmd.Stdout = &output
		cmd.Stderr = &output
		err := cmd.Run()
		if err != nil {
			utility.LogMessage(fmt.Sprintf("Error executing binary on line %d: %v\nOutput: %s\n", lineNumber, err, output.String()))
			continue
		}

		// For now, leave storing the output commented out
		if aggregate_output {
			cmd_output := output.String()
			// Parse Content as JSON
			var contentData struct {
				Meta struct {
					Columns       string `json:"columns"`
					LineProcessed int    `json:"lineProcessed"`
				} `json:"meta"`
				Data map[string]string `json:"data"`
			}

			err = json.Unmarshal([]byte(cmd_output), &contentData)
			if err != nil {
				continue
			}
			key_value := ""
			for _, value := range contentData.Data {
				key_value = value
				break
			}
			aggregate_val := utility.KeyMurmurHash(key_value, num_tasks)
			vals := strings.Split(outputFilename, "_")
			vals[len(vals)-1] = strconv.FormatUint(uint64(aggregate_val), 10)
			outputFilename = strings.Join(vals, "_")

		}

		writeToFile(local_temp_file, output.String())
		req := file_transfer.ClientData{Operation: "append", LocalFilePath: local_temp_file, Filename: outputFilename}
		file_transfer.SendAppends(req, outputFilename)
		utility.LogMessage(fmt.Sprintf("Line %d output: %s\n", lineNumber, output.String()))
	}

	if err := scanner.Err(); err != nil {
		utility.LogMessage(fmt.Sprintf("Error reading input file: %v\n", err))
	}
}

func writeToFile(filePath, content string) error {
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("error creating file: %w", err)
	}
	defer file.Close()
	_, err = file.WriteString(content)
	if err != nil {
		return fmt.Errorf("error writing to file: %w", err)
	}

	return nil
}

/*
To change
- keep track of offset size, so we can jump everytime we read it. Keepp updating this as we read
- Read meta data from file.
- Keep track of last processed from each node, from meta and we don't duplicate it.
*/
