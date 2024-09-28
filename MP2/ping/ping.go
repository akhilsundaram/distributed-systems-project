package ping

import (
	"encoding/json"
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
	ID        string `json:"id"`
	Piggyback string `json:"data"`
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
	bufferData := BufferSent()
	// utility.LogMessage(string(data) + ":  " + addr.String())
	// membership.PrintMembershipList()
	AddToNodeBuffer(data, addr.IP.String())

	conn.SetWriteDeadline(time.Now().Add(120 * time.Millisecond))
	// Send the request
	_, err := conn.WriteToUDP(bufferData, addr)
	if err != nil {
		utility.LogMessage("error sending UDP request: " + err.Error())
	}

}

func Sender(suspect bool, ping_id int) {
	// store the 10 vms in a array
	hostArray := []string{
		"fa24-cs425-5901.cs.illinois.edu.",
		"fa24-cs425-5902.cs.illinois.edu.",
		"fa24-cs425-5903.cs.illinois.edu.",
		"fa24-cs425-5904.cs.illinois.edu.",
		"fa24-cs425-5905.cs.illinois.edu.",
		"fa24-cs425-5906.cs.illinois.edu.",
		"fa24-cs425-5907.cs.illinois.edu.",
		"fa24-cs425-5908.cs.illinois.edu.",
		"fa24-cs425-5909.cs.illinois.edu.",
		"fa24-cs425-5910.cs.illinois.edu.",
	}
	// randomize the order of vms to send the pings to
	randomizeHostArray := shuffleStringArray(hostArray)

	//My hostname
	my_hostname, e := os.Hostname()
	if e != nil {
		utility.LogMessage("os host name failed")
	}
	my_hostname += "."

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
				membership.DeleteMember(host)
				membership.WriteToBuffer("f", host)
				utility.LogMessage(" node declares ping timeout & deleted host - " + host)
			}
		} else {
			utility.LogMessage("Error reading response from " + host + ": " + err.Error())
		}
	}

	//IF PING / RESPONSE RECEIVED
	AddToNodeBuffer(response[:n], host)

}

func BufferSent() []byte {
	buff := membership.GetBufferElements()
	var buffArray []InputData
	//Append Ping
	pingBuff := InputData{
		ID:        "ping",
		Piggyback: "",
	}

	//Array to be sent
	buffArray = append(buffArray, pingBuff)

	//For every buffer element, append to array
	for i := 0; i < len(buff); i++ {
		tp_io := InputData{}
		err := json.Unmarshal(buff[i].Data, &tp_io)
		if err == nil {
			buffArray = append(buffArray, tp_io)
		}
	}

	membership.UpdateBufferGossipCounts()

	output, err := json.Marshal(buffArray)
	if err != nil {
		utility.LogMessage("another error for marshall - send ping -" + err.Error())
	}
	return output
}

func AddToNodeBuffer(data []byte, remoteAddr string) {
	var parsedData []InputData
	jsonErr := json.Unmarshal(data, &parsedData)
	if jsonErr != nil {
		utility.LogMessage("Error parsing JSON: " + jsonErr.Error())
	}

	//Create a ping map to delete and check if ping data was back /??

	// Process the ping data here
	for i := 0; i < len(parsedData); i++ {
		if parsedData[i].ID == "ping" {
			// membership.PrintMembershipList()
			continue
		}

		//Get hostname of value
		hostname, err := membership.GetMemberHostname(parsedData[i].Piggyback)
		if err != nil {
			utility.LogMessage(err.Error())
		} else if !membership.IsMember(hostname) {
			// member does not exist and buffer data for it not a new join.
			if parsedData[i].ID != "n" {
				continue
			}
		}

		//Do a check to see if in Buffer or not
		buffer_value_bytes, err := json.Marshal(parsedData[i])
		if err != nil {
			utility.LogMessage("Handle ping and Ack - buffer value to bytes error - " + err.Error())
		}
		//If buffer value exists already, do nothing
		if membership.CheckBuffer(buffer_value_bytes) {
			continue
		}

		if suspicion.Enabled {
			suspicion.SuspicionHandler(parsedData[i].ID, parsedData[i].Piggyback)
		} else {
			// if membership.BufferMap[parsedData[i]] // check here

			switch parsedData[i].ID {
			case "n":
				membership.AddMember(parsedData[i].Piggyback, hostname)
				// membership.PrintMembershipList()
				utility.LogMessage("New member added to Membership list, hostname :" + hostname + ", and member_id: " + parsedData[i].Piggyback)
				membership.WriteToBuffer(parsedData[i].ID, hostname)
				continue
			case "f":
				// if membership.IsMember(hostname){     //ADD CHECKKKKKKKKKKK
				// 	parsedData[i].Piggyback
				// }
				membership.DeleteMember(parsedData[i].Piggyback)
				utility.LogMessage("Deleted member added to Membership list, :" + hostname + "Deleted member_id: " + parsedData[i].Piggyback)
				// membership.PrintMembershipList()
				membership.WriteToBuffer(parsedData[i].ID, hostname)
				continue

			}
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
