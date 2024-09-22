package pingpong

import (
	"fmt"
	"log"
	"net"
	"os"
	"sync"
)

var (
	logFile *os.File
	logger  *log.Logger
	once    sync.Once
	mu      sync.Mutex
)

var LOGGER_FILE = "/home/log/machine.log"

func initLogger() {
	once.Do(func() {
		var err error
		logFile, err = os.OpenFile("app.log", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
		if err != nil {
			log.Fatal(err)
		}
		logger = log.New(logFile, "", log.LstdFlags)
	})
}

func LogMessage(message string) {
	initLogger()
	mu.Lock()
	defer mu.Unlock()
	logger.Println(message)
}

func PingReceiver(portNo string) {
	// ip_addr := getIPAddr(host)
	// ip_addr := net.IPv4(127, 0, 0, 1)

	listener, err := net.Listen("udp", ":9090")
	LogMessage("Listener created")

	if err != nil {
		fmt.Print(err)
	}

	defer func() { _ = listener.Close() }() // will get called at the end of HostListener.
}
