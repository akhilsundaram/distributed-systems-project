package pingpong

import (
	"bufio"
	"encoding/json"
	"bufio"
	"encoding/json"
	"failure_detection/utility"
	"net"
	"math/rand"
    "time"
)

const (
	port = "9090"
)

var LOGGER_FILE = "/home/log/machine.log"

type InputData struct {
	PingID string `json:"ping_id"`
}

func PingAck() {
type InputData struct {
	PingID string `json:"ping_id"`
}

func PingAck() {

	// ip := net.IPv4(127, 0, 0, 1)

	// opening the port for UDP connections
	ack, err := net.Listen("udp", port)
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

func handlePingAndSendAck(data []byte, remoteAddr string, c net.Conn) bool {
	var parsedData InputData
	jsonErr := json.Unmarshal(data, &parsedData)
	if jsonErr != nil {
		utility.LogMessage("Error parsing JSON: " + jsonErr.Error())
		return false
	}

	// Process the ping data here
	utility.LogMessage("Received ping from " + remoteAddr + ", ping id : " + string(data))

	// Send a response back
	bufW := bufio.NewWriter(c)

	_, err := bufW.WriteString(parsedData.PingID)
	if err != nil {
		utility.LogMessage("Error writing ping id to conn buffer :" + err.Error())
		return false
	}

	//Flush
	err = bufW.Flush()
	if err != nil {
		utility.LogMessage("Error on conn buffer flush :" + err.Error())
		return false
	}
	return true
}

func SendPing(suspect bool) {

	// store the 10 vms in a array
	var host_array [10]string = ["fa24-cs425-5901.cs.illinois.edu", "fa24-cs425-5902.cs.illinois.edu", 
					"fa24-cs425-5903.cs.illinois.edu", "fa24-cs425-5904.cs.illinois.edu", 
					"fa24-cs425-5905.cs.illinois.edu", "fa24-cs425-5906.cs.illinois.edu", 
					"fa24-cs425-5907.cs.illinois.edu", "fa24-cs425-5908.cs.illinois.edu", 
					"fa24-cs425-5909.cs.illinois.edu", "fa24-cs425-5910.cs.illinois.edu"]
	
	// randomize the order of vms to send the pings to
	hosthost_array = shuffleStringArray(hoshost_array)

	
	// check if the machine is up first then do send Pings

	// wait for ack, if no ack then report it as Sus
}

// 
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