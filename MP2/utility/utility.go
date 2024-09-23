package utility

import (
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
		logFile, err = os.OpenFile(LOGGER_FILE, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
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

func GetIPAddr(host string) net.IP {
	ips, err := net.LookupIP(host) // Can give us a string of IPs.

	if err != nil {
		LogMessage("Error on IP lookup for : " + host)
	}

	for _, ip := range ips { //iterate through and get first IP.
		if ipv4 := ip.To4(); ipv4 != nil {
			return ipv4
		}
	}
	return net.IPv4(127, 0, 0, 1) // return loopback as default
}
