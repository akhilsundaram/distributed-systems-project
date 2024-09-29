package ping

import (
	"encoding/json"
	"failure_detection/buffer"
	"failure_detection/membership"
	"failure_detection/suspicion"
	"failure_detection/utility"
	"math/rand"
	"net"
	"os"
	"time"
)

const (
	port    = "9090"
	timeout = 10 * time.Millisecond
)

var LOGGER_FILE = "/home/log/machine.log"

type InputData struct {
	Msg      string `json:"msg"`
	Node_id  string `json:"node_id"`
	Hostname string `json:"hostname"`
}

func Listener() {
	// opening the port for UDP connections
	addr, err := net.ResolveUDPAddr("udp", ":"+port)
	if err != nil {
		utility.LogMessage("Error resolving address:" + err.Error())
		return
	}
	//Listening on port,addr
	conn, err := net.ListenUDP("udp", addr)
	utility.LogMessage("Ping Ack is up")
	if err != nil {
		utility.LogMessage("Error starting PingAck : " + err.Error())
	}
	defer conn.Close()

	buf := make([]byte, 1024)

	for {
		n, remoteAddr, err := conn.ReadFromUDP(buf) // Accept blocks conn, go routine to process the message
		if err != nil {
			utility.LogMessage("Not able to accept incoming ping : " + err.Error())
		}

		go HandleIncomingConnectionData(conn, remoteAddr, buf[:n])

	}

}

// INPUT -> connection, data from conn. TODO-> sends data back
func HandleIncomingConnectionData(conn *net.UDPConn, addr *net.UDPAddr, data []byte) {
	// buffer to send
	bufferData := BufferSent()
	// utility.LogMessage(string(data) + ":  " + addr.String())
	// membership.PrintMembershipList()

	// parse the incoming buffer in data, add it to your buffer
	AddToNodeBuffer(data, addr.IP.String())

	conn.SetWriteDeadline(time.Now().Add(1200 * time.Millisecond))
	// Send the request
	_, err := conn.WriteToUDP(bufferData, addr)
	if err != nil {
		utility.LogMessage("error sending UDP request: " + err.Error())
	}

}

func Sender(suspect bool, ping_id int) {
	// store the 10 vms in a array
	hostArray := []string{
		"fa24-cs425-5901.cs.illinois.edu",
		"fa24-cs425-5902.cs.illinois.edu",
		"fa24-cs425-5903.cs.illinois.edu",
		"fa24-cs425-5904.cs.illinois.edu",
		"fa24-cs425-5905.cs.illinois.edu",
		"fa24-cs425-5906.cs.illinois.edu",
		"fa24-cs425-5907.cs.illinois.edu",
		"fa24-cs425-5908.cs.illinois.edu",
		"fa24-cs425-5909.cs.illinois.edu",
		"fa24-cs425-5910.cs.illinois.edu",
	}
	// randomize the order of vms to send the pings to
	randomizeHostArray := shuffleStringArray(hostArray)

	//My hostname
	my_hostname, e := os.Hostname()
	if e != nil {
		utility.LogMessage("os host name failed")
	}

	for _, host := range randomizeHostArray {
		if membership.IsMember(host) && !(my_hostname == host) {
			go sendUDPRequest(host)
			time.Sleep(300 * time.Millisecond)
		}

	}

}

func sendUDPRequest(host string) {

	//Data to be sent along with conn
	nodeBuffer := BufferSent()

	ipAddr := utility.GetIPAddr(host)

	serverAddr, err := net.ResolveUDPAddr("udp", ipAddr.String()+":"+port)
	if err != nil {
		utility.LogMessage("Error resolving address:" + err.Error())
		return
	}

	conn, err := net.DialUDP("udp", nil, serverAddr)
	if err != nil {
		utility.LogMessage("Error in connection to " + host + ": " + err.Error())
		return
	}

	defer conn.Close()

	// Send the request
	_, err = conn.Write(nodeBuffer)
	if err != nil {
		utility.LogMessage("error sending UDP request: " + err.Error())
	}

	// Set a timeout for receiving the response
	err = conn.SetReadDeadline(time.Now().Add(120 * time.Millisecond))
	if err != nil {
		utility.LogMessage("error setting read deadline: " + err.Error())
	}

	// Read the response
	response := make([]byte, 1024)
	n, err := conn.Read(response)
	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			utility.LogMessage("Timeout waiting for response from " + host)
			// IF NO PING RESPONSE
			// Either mark node as FAIL or raise Suspicion message
			if suspicion.Enabled {
				suspicion.DeclareSuspicion(host)
			} else {
				node_id := membership.GetMemberID(host)
				utility.LogMessage("Reached here && Hostname :" + host)
				if node_id != "-1" {
					membership.DeleteMember(node_id, host)
					buffer.WriteToBuffer("f", node_id, host)
					utility.LogMessage(" node declares ping timeout & deleted host - " + host)
				} else {
					membership.PrintMembershipList()
				}

			}
		} else {
			utility.LogMessage("Error reading response from " + host + ": " + err.Error())
		}
	} else {
		//IF PING / RESPONSE RECEIVED
		AddToNodeBuffer(response[:n], host)
	}

}

func BufferSent() []byte {
	//Get Buffer
	buff := buffer.GetBuffer()

	//Append Ping
	buffArray := buffer.BufferData{
		Message: "ping",
		Node_id: "-1",
	}
	buff["MP2"] = buffArray

	//to bytes
	output, err := json.Marshal(buff)
	if err != nil {
		utility.LogMessage("err in marshalling mpap to bytes - send ping - " + err.Error())
	}

	//update gossip after
	buffer.UpdateBufferGossipCount()

	return output
}

func AddToNodeBuffer(data []byte, remoteAddr string) {
	var parsedData map[string]buffer.BufferData
	jsonErr := json.Unmarshal(data, &parsedData)
	if jsonErr != nil {
		utility.LogMessage("Error parsing JSON: " + jsonErr.Error())
	}

	//Create a ping map to delete and check if ping data was back /??

	// Process the ping data here

	// directly check each key value pair for parsedData, and send it to WriteBuffer
	for hostname, buffData := range parsedData {
		if !membership.IsMember(hostname) {
			// member does not exist and buffer data for it not a new join.
			if buffData.Message != "n" {
				continue
			}
		}
		switch buffData.Message {
		case "ping":
			continue
		case "f":
			membership.DeleteMember(buffData.Node_id, hostname)
			buffer.WriteToBuffer("f", buffData.Node_id, remoteAddr)
			continue
		case "n":
			membership.AddMember(buffData.Node_id, hostname)
			buffer.WriteToBuffer("n", buffData.Node_id, remoteAddr)
			continue
		default:
			continue
		}

	}

}

func shuffleStringArray(arr []string) []string {
	shuffled := make([]string, len(arr))
	copy(shuffled, arr)

	// Create a new source of randomness with the current time as seed
	source := rand.NewSource(time.Now().UnixNano())
	r := rand.New(source)

	// Using Fisher-Yates shuffle algorithm for random permuatation
	for i := len(shuffled) - 1; i > 0; i-- {
		j := r.Intn(i + 1)
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	}

	return shuffled
}
