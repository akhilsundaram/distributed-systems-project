package pingpong

import (
	"bufio"
	"encoding/json"
	"failure_detection/membership"
	"failure_detection/suspicion"
	"failure_detection/utility"
	"math/rand"
	"net"
	"strconv"
	"sync"
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

/* Handles ping that come to host */
func PingAck() {

	// ip := net.IPv4(127, 0, 0, 1)

	// opening the port for UDP connections
	ack, err := net.Listen("udp", port)

	utility.LogMessage("Ping Ack is up")

	if err != nil {
		utility.LogMessage("Error starting PingAck : " + err.Error())
	}

	defer func() { _ = ack.Close() }()

	// Looping until we get a ping
	for {

		conn, err := ack.Accept() // Accept blocks ack, go routine to process the message

		if err != nil {
			utility.LogMessage("Not able to accept incoming ping : " + err.Error())
		}

		go func(c net.Conn) {
			defer c.Close() // closes when the go routine ends
			buf := make([]byte, 1024)
			serverAddr := c.RemoteAddr().String()

			for {
				//Check data
				n, err := c.Read(buf)
				if err != nil {
					utility.LogMessage("Error in reading from connection buffer : " + err.Error())
					return
				}
				//Parse data
				data := buf[:n]
				handlePingAndSendAck(data, serverAddr, c)
			}

		}(conn)
	}

}

/*
- Send back ACK
- Send back buffer data
*/
func handlePingAndSendAck(data []byte, remoteAddr string, c net.Conn) bool {
	var parsedData []InputData
	jsonErr := json.Unmarshal(data, &parsedData)
	if jsonErr != nil {
		utility.LogMessage("Error parsing JSON: " + jsonErr.Error())
		return false
	}

	//Create a ping map to delete and check if ping data was back /??

	// Process the ping data here
	for i := 0; i < len(parsedData); i++ {
		if parsedData[i].ID == "ping" {
			utility.LogMessage("Received ping from " + remoteAddr + ", ping id : " + string(parsedData[i].Piggyback))
			continue
		}

		//Get hostname of value
		hostname := membership.GetMemberHostname(parsedData[i].Piggyback)

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
				
				continue
			case "f":
				// if membership.IsMember(hostname){     //ADD CHECKKKKKKKKKKK
				// 	parsedData[i].Piggyback
				// }
				continue

			}
		}

	}

	// Send a response back
	bufW := bufio.NewWriter(c)

	// var piggyback_data_elements []map[string]interface{}
	// err := json.Unmarshal(parsedData.Piggyback, &piggyback_data_elements)
	// if err != nil {
	// 	utility.LogMessage("piggyback data unmardshall error 1 -" + err.Error())
	// }
	// for i, piggyback_data := range piggyback_data_elements {
	// 	if suspicion.Enabled {
	// 		suspicion.SuspicionHandler(piggyback_data)
	// 	}
	// 	continue
	// }

	// _, err = bufW.WriteString(parsedData.PingID)
	// if err != nil {
	// 	utility.LogMessage("Error writing ping id to conn buffer :" + err.Error())
	// 	return false
	// }

	//Flush
	err := bufW.Flush()
	if err != nil {
		utility.LogMessage("Error on conn buffer flush :" + err.Error())
		return false
	}
	return true
}

func SendPing(suspect bool, ping_id int) {

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

	buff := membership.GetBufferElements()
	var buffArray []InputData
	//Append Ping
	pingBuff := InputData{
		ID:        strconv.Itoa(ping_id),
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

	bytes_buffArray, err := json.Marshal(buffArray)
	if err != nil {
		utility.LogMessage("another error for marshall - send ping -" + err.Error())
	}

	var wg sync.WaitGroup
	for _, host := range randomizeHostArray {
		if membership.IsMember(host) {
			wg.Add(1)
			go func(host string) {
				defer wg.Done()
				sendUDPRequest(host, bytes_buffArray)
			}(host)
		}

	}
	// check if the machine is up first then do send Pings

	// wait for ack, if no ack then report it as Sus
	wg.Wait()
}

func sendUDPRequest(host string, requestData []byte) {
	ipAddr := utility.GetIPAddr(host)

	conn, err := net.DialTimeout("udp", ipAddr.String()+":"+(port), timeout)
	if err != nil {
		utility.LogMessage("Error in connection to " + host + ": " + err.Error())
		return
	}
	defer conn.Close()

	_, err = conn.Write(requestData)
	if err != nil {
		utility.LogMessage("Error sending request to " + host + ": " + err.Error())
		return
	}

	// Set read deadline
	err = conn.SetReadDeadline(time.Now().Add(timeout))
	if err != nil {
		utility.LogMessage("Error setting read deadline: " + err.Error())
		return
	}

	response := make([]byte, 1024)
	_, err = conn.Read(response)
	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			utility.LogMessage("Timeout waiting for response from " + host)

			// Either mark node as FAIL or raise Suspicion message

			if suspicion.Enabled {
				suspicion.DeclareSuspicion(host)
			}
		} else {
			utility.LogMessage("Error reading response from " + host + ": " + err.Error())
		}
		return
	}
	// utility.LogMessage("Received response from " + host + ": " + string(response[:n]))

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
