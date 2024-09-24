package introducer

import (
	"encoding/json"
	"failure_detection/utility"
	"fmt"
	"net"
	"os"
	"time"
)

const (
	port = "7070"
)

var LOGGER_FILE = "/home/log/machine.log"

type IntroducerData struct {
	NodeID    string `json:"node_id"`
	Timestamp string `json:"timestamp"`
}

//Timestamp: time.Now().Format(time.RFC3339),

func IntroducerListener() {
	hostname, err := os.Hostname()
	if err != nil {
		utility.LogMessage("Error: " + err.Error())
		return
	}

	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		utility.LogMessage("Error: " + err.Error())
		return
	}
	defer listener.Close()

	utility.LogMessage("Introducer Listener created on machine: " + hostname)

	for {
		conn, err := listener.Accept()
		if err != nil {
			utility.LogMessage("Error accepting joining connection on introducer: " + err.Error())
			continue
		}

		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	// Read incoming data
	buffer := make([]byte, 1024)
	n, err := conn.Read(buffer)
	serverAddr := conn.RemoteAddr().String()

	if err != nil {
		utility.LogMessage("Error reading from connection: " + err.Error())
		return
	}

	var parsedData IntroducerData

	data := buffer[:n]
	jsonErr := json.Unmarshal(data, &parsedData)
	if jsonErr != nil {
		utility.LogMessage("Error parsing JSON: " + jsonErr.Error())
	}

	nodeID := parsedData.NodeID
	timestamp := parsedData.Timestamp

	// Process the ping data here
	utility.LogMessage("Received connection  from " + serverAddr + " - Node ID: " + nodeID + ", Timestamp: " + timestamp)

	// Process the data here, prepare and send response (add it to membership list)
	response := prepareResponse(serverAddr, nodeID, timestamp)
	// Send a response back
	_, err = conn.Write([]byte(response))
	if err != nil {
		utility.LogMessage("Error sending response: " + err.Error())
		return
	}

	/*
		bufW := bufio.NewWriter(c)
		Write Membership list in buffer and send it back on same connection buffer

	*/

}

func prepareResponse(serverHostname, nodeID, timestamp string) string {
	// Add node to membership list and also add membership list to buffer, and send
	// need a different buffer for this, or should we directly read membership buffer, append this data and send
	// and write the entry in the buffer after this ? (to ensure the node gets data quickly)

	return "Welcome, Machine " + serverHostname + "! Your version number is : " + nodeID + ". Your connection time was " + timestamp + ". Here's some config data: ..."
}

func InitiateIntroducerRequest(hostname, port, node_id string) {

	//Go routine wait group

	senderData := IntroducerData{
		NodeID:    node_id,
		Timestamp: time.Now().Format(time.RFC3339),
	}

	requestData, jsonErr := json.Marshal(senderData)
	if jsonErr != nil {
		fmt.Printf("json marshall err for req data - %v", jsonErr)
	}

	ip_addr := utility.GetIPAddr(hostname)
	conn, err := net.DialTimeout("tcp", ip_addr.String()+":"+port, time.Millisecond*2000)
	if err != nil {
		utility.LogMessage("Error connecting to introducer: " + err.Error())
		return
	}
	defer conn.Close()

	// Send the JSON data
	_, err = conn.Write(requestData)
	if err != nil {
		utility.LogMessage("Error sending message: " + err.Error())
		return
	}

	utility.LogMessage("Sent message to introducer: " + string(requestData))

	// Wait for response
	buffer := make([]byte, 1024)
	n, err := conn.Read(buffer)
	if err != nil {
		utility.LogMessage("Error reading response: " + err.Error())
		return
	}

	response := string(buffer[:n])
	utility.LogMessage("Received response from introducer: " + response)

	// Process the response
	// Response will be the membership list in the buffer
	// what about the messages of that node ? should we send that as well ?
}
