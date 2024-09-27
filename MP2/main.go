package main

import (
	"failure_detection/utility"
	"fmt"
	"os"
)

var LOGGER_FILE = "/home/log/machine.log"
var INTRODUCER_HOST = "fa24-cs425-5901.cs.illinois.edu"

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

	// check if node is introducer
	// if yes then start introducer listener
	// else, send request to introducer to join the membership list
	// start ping pong programs
	// daemonize the program
	// add signal to ensure that we handle changing of logic from ping to pingS

}
