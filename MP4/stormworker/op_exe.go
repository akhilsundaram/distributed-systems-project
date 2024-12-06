package stormworker

import (
	"bufio"
	"bytes"
	"embed"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"rainstorm/utility"
	"strings"
)

//go:embed operators/op_exe/*
var operators embed.FS

func RunOp_exe(inputFilename, operation string, offset int, end int, phase int) {
	//Set status
	setTaskRunning(phase, true)
	defer setTaskRunning(phase, false)

	// Get OP_EXE
	binaryPath := filepath.Join("operators/op_exe", operation)
	binaryFile, err := operators.Open(binaryPath)
	if err != nil {
		utility.LogMessage("Error locating binary: " + err.Error() + ", EXITING OP_EXE for op: " + operation)
		return
	}
	defer binaryFile.Close()

	//Run in a temp location
	tempBinaryPath := fmt.Sprintf("/tmp/%s", operation)
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
		// Example: Store output in a file
		// WRITE TO INTERMEDIATE FILE/APPEND
		// outputFile.WriteString(fmt.Sprintf("Line %d output: %s\n", lineNumber, output.String()))

		utility.LogMessage(fmt.Sprintf("Line %d output: %s\n", lineNumber, output.String()))
	}

	if err := scanner.Err(); err != nil {
		utility.LogMessage(fmt.Sprintf("Error reading input file: %v\n", err))
	}
}
