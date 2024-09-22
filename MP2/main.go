package main

import (
	"failure_detection/utility"
	"fmt"
	"os"
)

var LOGGER_FILE = "/home/log/machine.log"

func main() {
	// args := os.Args
	// clearing the machine.log file
	file, err := os.OpenFile(LOGGER_FILE, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		fmt.Println("Error Opening file : " + err.Error())
	}

	file.Close()

	utility.LogMessage("Starting execution")
	// typeArg is either "machine" or "caller", otherwise invalid

}
