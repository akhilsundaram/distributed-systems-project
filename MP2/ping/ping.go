package ping

import (
	"encoding/json"
	"failure_detection/buffer"
	"failure_detection/membership"
	"failure_detection/utility"
	"fmt"
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

	buf := make([]byte, 4096)

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

	// Send the request
	// if (checkRandomDrop(drop_rate_%)){
	// utility.LogMessage("Inducing random drops in sending ack to requests")
	//} // --> higher the number, higher chance of drop
	_, err := conn.WriteToUDP(bufferData, addr)
	if err != nil {
		utility.LogMessage("error sending UDP request: " + err.Error())
	}

	// parse the incoming buffer in data, add it to your buffer
	AddToNodeBuffer(data, addr.IP.String())

}

func Sender(suspect bool) {
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

	//My hostname
	my_hostname, e := os.Hostname()
	if e != nil {
		utility.LogMessage("os host name failed")
	}

	for {
		// randomize the order of vms to send the pings to
		randomizeHostArray := shuffleStringArray(hostArray)

		for _, host := range randomizeHostArray {
			if membership.IsMember(host) && !(my_hostname == host) {
				sendUDPRequest(host, my_hostname)
				time.Sleep(2 * time.Second)
			}
		}
		//time.Sleep(2 * time.Second)
	}
}

func sendUDPRequest(host string, self_name string) {

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
	err = conn.SetReadDeadline(time.Now().Add(2000 * time.Millisecond))
	if err != nil {
		utility.LogMessage("error setting read deadline: " + err.Error())
	}

	// Read the response
	response := make([]byte, 4096)
	n, err := conn.Read(response)
	if err != nil {
		node_id := membership.GetMemberID(host)
		if node_id != "-1" {
			if membership.SuspicionEnabled {
				DeclareSuspicion(host, node_id)
			} else {
				utility.LogMessage("Member? " + host + " to be deleted because it didnt reply to ping from " + self_name)
				membership.DeleteMember(node_id, host)
				buffer.WriteToBuffer("f", node_id, host)
				utility.LogMessage(" node declares ping timeout & deleted host - " + host)
			}

		} else {
			membership.PrintMembershipList()
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
		if membership.SuspicionEnabled {
			SuspicionHandler(buffData.Message, buffData.Node_id, hostname, buffData.IncarnationNumber) // Same kind of Switch statements but in SUS Handler
		} else {
			switch buffData.Message {
			case "ping":
				continue
			case "f":
				utility.LogMessage("Fail signal seen in buffer for hostname " + hostname)
				membership.DeleteMember(buffData.Node_id, hostname)
				buffer.WriteToBuffer("f", buffData.Node_id, hostname)
				continue
			case "n":
				membership.AddMember(buffData.Node_id, hostname)
				buffer.WriteToBuffer("n", buffData.Node_id, hostname)
				continue
			case "sus":
				if !membership.SuspicionEnabled {
					membership.SuspicionEnabled = true
					buffer.WriteToBuffer("sus", buffData.Node_id, hostname)
				}
				continue
			default:
				continue
			}
		}

	}

}

func SuspicionHandler(Message, Node_id, hostname string, incarnation int) {
	switch Message {
	case "ping":
		return
	case "nsus":
		if membership.SuspicionEnabled {
			membership.SuspicionEnabled = false
			buffer.WriteToBuffer("nsus", Node_id, hostname, incarnation)
		}
		return
	case "n":
		membership.AddMember(Node_id, hostname)
		buffer.WriteToBuffer("n", Node_id, hostname, incarnation)
		return
	case "f":
		utility.LogMessage("Fail signal seen in buffer for hostname " + hostname)
		membership.UpdateSuspicion(hostname, membership.Faulty)
		membership.DeleteMember(Node_id, hostname)
		buffer.WriteToBuffer("f", Node_id, hostname, incarnation)
		return
	case "s":
		utility.LogMessage("Fail signal seen in buffer for hostname " + hostname)
		inc := membership.GetMemberIncarnation(hostname)
		sus_state, _ := membership.GetSuspicion(hostname)
		if membership.My_hostname == hostname {
			utility.LogMessage("SELF SUSSED OUT!! Send Alive")
			if inc == incarnation {
				utility.LogMessage("Incarnation matches, will def add alive and increase inc")
				membership.UpdateSuspicion(hostname, membership.Alive)
				membership.SetMemberIncarnation(hostname)
				buffer.WriteToBuffer("a", Node_id, hostname, inc+1)
			}
			return

		} else if inc <= incarnation {
			if sus_state == -2 || sus_state == membership.Alive {
				membership.UpdateSuspicion(hostname, membership.Suspicious)
				membership.SetMemberIncarnation(hostname, incarnation)
				buffer.WriteToBuffer("s", Node_id, hostname, incarnation)
				fmt.Printf("\nSUSPICIOUS :: Host %s with MemberID %s\n", hostname, Node_id)
				time.AfterFunc(membership.SuspicionTimeout, func() { stateTransitionOnTimeout(hostname, Node_id) })
			}
			if sus_state == membership.Suspicious {
				membership.SetMemberIncarnation(hostname, incarnation)
			}

		}
		return
	case "a":
		utility.LogMessage("Alive signal seen in buffer for hostname " + hostname)
		sus_state, _ := membership.GetSuspicion(hostname)
		inc := membership.GetMemberIncarnation(hostname)
		if (sus_state == membership.Suspicious) && inc < incarnation {
			membership.UpdateSuspicion(hostname, membership.Alive)
			membership.SetMemberIncarnation(hostname, incarnation)
			buffer.WriteToBuffer("a", Node_id, hostname, incarnation)
		} else if inc < incarnation {
			membership.UpdateSuspicion(hostname, membership.Alive)
			membership.SetMemberIncarnation(hostname, incarnation)
			buffer.WriteToBuffer("a", Node_id, hostname, incarnation)
		}

		// time.AfterFunc(suspicionTimeout, func() { stateTransitionOnTimeout(hostname) })
		return
	default:
		return

	}

}

func stateTransitionOnTimeout(hostname string, node_id string) {
	utility.LogMessage("state transition called for host : " + hostname)
	state, _ := membership.GetSuspicion(hostname)
	incarnation := membership.GetMemberIncarnation(hostname)
	if state == membership.Suspicious {
		membership.UpdateSuspicion(hostname, membership.Faulty)
		membership.DeleteMember(node_id, hostname)
		buffer.WriteToBuffer("f", node_id, hostname, incarnation)
		//WriteToBuffer("f", hostname) ///
	}
}

// Declare a host as suspicious //
func DeclareSuspicion(hostname string, node_id string) error {
	// Only time our ping/ server ever reqs sus data.
	// Declares aftertimer to handle states internally. Maybe even callable from the Handler

	state, _ := membership.GetSuspicion(hostname)
	if state == -2 || state == membership.Alive { //No suspicion exists, but host does
		fmt.Printf("\nSUSPICIOUS :: Host %s with MemberID %s\n", hostname, node_id)
		inc := membership.GetMemberIncarnation(hostname)
		membership.UpdateSuspicion(hostname, membership.Suspicious)
		time.AfterFunc(membership.SuspicionTimeout, func() { stateTransitionOnTimeout(hostname, node_id) })
		buffer.WriteToBuffer("s", node_id, hostname, inc) //Need to decide format for string/data output. Or handle it in membership ?
		return nil
	}
	return nil
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

func checkRandomDrop(numPassed int) bool {
	randomNumber := rand.Intn(101)
	//fmt.Printf("Generated random number: %d\n", randomNumber)  // Added for testing
	return randomNumber <= numPassed
}
