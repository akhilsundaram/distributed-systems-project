package main

import (
	"failure_detection/introducer"
	"failure_detection/pingpong"
	"failure_detection/utility"
	"fmt"
	"os"
	"time"
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

	hostname, err := os.Hostname()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	utility.LogMessage("Starting execution on host:" + hostname)

	node_id := "0"

	if hostname == INTRODUCER_HOST {
		go introducer.IntroducerListener()
		// adds itself to membership list, saves it to send to other nodes

	} else {
		introducer.InitiateIntroducerRequest(INTRODUCER_HOST, "7070", node_id)
		// by now hoping that we have updated membership list
	}

	time.Sleep(time.Second * 2)

	// starting ping listener on every node
	go pingpong.PingAck()

	ping_count := 0
	// sending pings
	pingpong.SendPing(false, ping_count)
}
