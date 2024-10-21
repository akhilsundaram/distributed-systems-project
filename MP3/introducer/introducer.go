package introducer

import (
	"encoding/json"
	"fmt"
	"hydfs/buffer"
	"hydfs/membership"
	"hydfs/utility"
	"net"
	"os"
	"strings"
	"time"
)

const (
	port = "7070"
)

var LOGGER_FILE = "/home/log/machine.log"

type IntroducerData struct {
	NodeID    string `json:"node_id"`
	Hostname  string `json:"hostname"`
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
	buffer := make([]byte, 4096)
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

	/* Add new node to membership list */
	err = AddNewMember(serverAddr, nodeID, timestamp, parsedData.Hostname)
	if err != nil {
		utility.LogMessage("error from adding new member - " + err.Error())
	}

	memList := membership.GetMembershipList()

	if membership.SuspicionEnabled {
		memList["EnableSus"] = membership.Member{
			IncarnationNumber: -1,
		}
	}

	json_bytes_membership_list, err := json.Marshal(memList)
	if err != nil {
		utility.LogMessage("error handleconnection: converting membership list - " + err.Error())
	}

	// Send a response back
	_, err = conn.Write(json_bytes_membership_list)
	if err != nil {
		utility.LogMessage("Error sending response: " + err.Error())
		return
	}

}

func AddNewMember(serverAddr, nodeID, timestamp, hostname string) error {
	// Add node to membership list and also add membership list to buffer, and send
	// need a different buffer for this, or should we directly read membership buffer, append this data and send
	// and write the entry in the buffer after this ? (to ensure the node gets data quickly)

	// Add new node to membership list
	serverAddr = strings.Split(serverAddr, ":")[0]
	new_node_id := serverAddr + "_" + "9090" + "_" + nodeID + "_" + timestamp
	// getHostname, err := net.LookupAddr(serverAddr)
	// if err != nil {
	// 	return fmt.Errorf("NewMemb error - getting hostname from ip due to - %v", err)
	// } else {
	// 	new_hostname = getHostname[0]
	// }
	membership.AddMember(new_node_id, hostname)

	//Add membership to buffer for dissemination
	buffer.WriteToBuffer("n", new_node_id, hostname)

	return nil // "Welcome, Machine " + new_hostname + "! Your version number is : " + nodeID + ". Your connection time was " + timestamp + ". Here's some config data: ...", nil
}

func InitiateIntroducerRequest(hostname, port, node_id string) {

	//Go routine wait group
	sender_hostname, err := os.Hostname()
	if err != nil {
		utility.LogMessage("Error: " + err.Error())
		return
	}

	senderData := IntroducerData{
		NodeID:    node_id,
		Timestamp: time.Now().Format(time.RFC3339),
		Hostname:  sender_hostname,
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
	buffer := make([]byte, 4096)
	n, err := conn.Read(buffer)
	if err != nil {
		utility.LogMessage("Error reading response: " + err.Error())
		return
	}

	response := buffer[:n]
	utility.LogMessage("Received response from introducer")

	var membershipList map[string]membership.Member
	err = json.Unmarshal(response, &membershipList)
	if err != nil {
		utility.LogMessage("InitiateIntroducerReq error: unmarshal membershiplist from introducer - " + err.Error())
	}

	for host_name, value := range membershipList {
		// utility.LogMessage("Printing key value of membershipList : " + keys[i])
		if host_name == "EnableSus" {
			membership.SuspicionEnabled = true
		} else {
			membership.AddMember(value.Node_id, host_name)
		}
	}
	utility.LogMessage("Printing membership list recieved from machine 1")
	membership.PrintMembershipList()

	// Process the response
	// Response will be the membership list in the buffer
	// what about the messages of that node ? should we send that as well ?
}
