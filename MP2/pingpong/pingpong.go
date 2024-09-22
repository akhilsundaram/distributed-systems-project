package pingpong

import (
	"failure_detection/utility"
	"net"
)

var LOGGER_FILE = "/home/log/machine.log"

func PingAck(portNo string) {

	// ip := net.IPv4(127, 0, 0, 1)

	ack, err := net.Listen("udp", ":9090")
	utility.LogMessage("Ping Ack is up")

	if err != nil {
		utility.LogMessage("Error starting PingAck : " + err.Error())
	}

	defer func() { _ = ack.Close() }() // will get called at the end of HostListener.
}
