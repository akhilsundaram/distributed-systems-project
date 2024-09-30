package buffer

import (
	"failure_detection/membership"
	"failure_detection/utility"
	"fmt"
	"maps"
	"strings"
	"sync"
	"time"
)

type BufferData struct {
	Message           string
	Node_id           string
	TimesSent         int
	IncarnationNumber int
}

var shared_buffer = map[string]BufferData{} //Key is hostname
var bufferLock sync.RWMutex
var maxTimesSent = 4

func WriteToBuffer(Message, Node_id, Hostname string, incarnation_number ...int) {
	bufferLock.Lock()
	defer bufferLock.Unlock()

	bval := BufferData{
		Message:   Message,
		Node_id:   Node_id,
		TimesSent: 0,
	}
	if len(incarnation_number) > 0 {
		bval = BufferData{
			Message:           Message,
			Node_id:           Node_id,
			TimesSent:         0,
			IncarnationNumber: incarnation_number[0],
		}
	}

	if _, ok := shared_buffer[Hostname]; ok {
		// TODO timestamp check,
		if Node_id != shared_buffer[Hostname].Node_id {
			timestamp_new := strings.Split(Node_id, "_")[3]                         //incoming timestamp
			timestamp_old := strings.Split(shared_buffer[Hostname].Node_id, "_")[3] //existing timestamp
			switch compareTimeStamps(timestamp_new, timestamp_old) {
			case 0:
				// timestamp new is older than existing timestamp in buffer
				break
			case 1:
				// Timestamp new is newer than existing timestamp
				shared_buffer[Hostname] = bval
				// replace existing buffer message
			case -1:
				// invalid case
				break
			case 2:
				// we will never get this case, this is being handled in else
				break
			}
		} else {
			// Check message type and priority
			// Node id 1 == node id 2
			if membership.SuspicionEnabled {
				state, _ := membership.GetSuspicion(Hostname)
				switch Message {
				case "f":
					shared_buffer[Hostname] = bval
				case "s":
					if (state == membership.Alive) && incarnation_number[0] >= shared_buffer[Hostname].IncarnationNumber {
						utility.LogMessage("alive / inc>")
						shared_buffer[Hostname] = bval
					} else if state == -2 { // if member exists & 0 sus so far
						utility.LogMessage("-2")
						shared_buffer[Hostname] = bval
					} else if (state == membership.Suspicious) && (incarnation_number[0] > shared_buffer[Hostname].IncarnationNumber) {
						utility.LogMessage("sus / inc >")
						shared_buffer[Hostname] = bval
					}
					utility.LogMessage("NONEEE ?")

				case "a":
					if incarnation_number[0] > shared_buffer[Hostname].IncarnationNumber { //Unless the current disemination is Faulty-confirm, alive >>
						shared_buffer[Hostname] = bval
					}

				case "n":
					if shared_buffer[Hostname].Message == "n" {
						// new node join gets no priority over other messages
						shared_buffer[Hostname] = bval
					}
				case "ping":
					//default
					break

				}

			} else {
				switch Message {
				case "f":
					// fail always gets priority
					shared_buffer[Hostname] = bval
				case "n":
					// new node
					if shared_buffer[Hostname].Message == "n" {
						// new node join gets priority
						shared_buffer[Hostname] = bval
					}
				case "ping":
					// default, dont do anything.
					break
				}
			}

		}

	} else {
		bval := BufferData{
			Message:   Message,
			Node_id:   Node_id,
			TimesSent: 0,
		}
		if Message != "ping" {
			shared_buffer[Hostname] = bval
		}
	}
}

// func CheckBufferMessage(Hostname, Message, Node_id string) (bool, error) {

// 	if _, ok := shared_buffer[Hostname]; ok {

// 		switch compareTimeStamps(timestamp_new, timestamp_old) {
// 		case 0:
// 			return false, nil
// 		case 1:
// 			writeToBuffer(Hostname, Message, Node_id)
// 			return true, nil
// 		case -1:
// 			return true, fmt.Errorf("checkBufferMessage error - could not compare timestamps - invalid format")
// 		case 2:
// 			break
// 		}
// 		//Check for priority in messages
// 	}
// 	writeToBuffer(Hostname, Message, Node_id)
// 	return true, nil
// }

func compareTimeStamps(timestamp1, timestamp2 string) int {

	time1, err := time.Parse(time.RFC3339, timestamp1)
	if err != nil {
		fmt.Println("Error parsing timestamp1:", err)
		return -1
	}

	time2, err := time.Parse(time.RFC3339, timestamp2)
	if err != nil {
		fmt.Println("Error parsing timestamp2:", err)
		return -1
	}

	if time1.Before(time2) {
		return 0
	} else if time1.After(time2) {
		return 1
	} else {
		return 2
	}
}

func GetBuffer() map[string]BufferData {
	bufferLock.RLock()
	defer bufferLock.RUnlock()

	return maps.Clone(shared_buffer)
}

func UpdateBufferGossipCount() {
	bufferLock.Lock()
	defer bufferLock.Unlock()
	var toDelete []string

	for key, data := range shared_buffer {
		data.TimesSent++
		shared_buffer[key] = data
		if data.TimesSent >= maxTimesSent {
			toDelete = append(toDelete, key)
		}
	}

	for i := 0; i < len(toDelete); i++ {
		delete(shared_buffer, toDelete[i])
	}
}
