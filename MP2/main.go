package main

import (
	"bufio"
	"failure_detection/introducer"
	"failure_detection/membership"
	"failure_detection/ping"
	"failure_detection/suspicion"
	"failure_detection/utility"
	"strings"

	//"failure_detection/suspicion"
	"fmt"
	"os"
	"sync"
	"time"
)

var (
	LOGGER_FILE     = "/home/log/machine.log"
	INTRODUCER_HOST = "fa24-cs425-5901.cs.illinois.edu"
	status_sus      = false //suspicion.DeclareSuspicion
	ping_count      = 0
)

func main() {
	// args := os.Args
	// clearing the machine.log file
	file, err := os.OpenFile(LOGGER_FILE, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		fmt.Println("Error Opening file : " + err.Error())
	}

	file.Close()

	hostname, err := os.Hostname()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	utility.LogMessage("Starting execution on host:" + hostname)

	node_id := "0"

	if hostname == INTRODUCER_HOST {
		introducer.AddNewMember(utility.GetIPAddr(INTRODUCER_HOST).String(), "0", time.Now().Format(time.RFC3339), hostname)
		go introducer.IntroducerListener()
		// adds itself to membership list, saves it to send to other nodes

	} else {
		introducer.InitiateIntroducerRequest(INTRODUCER_HOST, "7070", node_id)
		// by now hoping that we have updated membership list
	}

	// time.Sleep(time.Second * 2)

	// starting ping listener on every node
	// go pingpong.PingAck()
	go ping.Listener()

	// sending pings
	go ContinouslySendPings()

	// Create channel to receive signals
	// sigChan := make(chan os.Signal, 1)
	// signal.Notify(sigChan, syscall.SIGUSR1)

	fmt.Println("Program running. PID:", os.Getpid())

	var wg sync.WaitGroup
	wg.Add(1)

	// Start a goroutine to handle CLI input
	go func() {
		defer wg.Done()
		scanner := bufio.NewScanner(os.Stdin)
		for {
			fmt.Print("Enter command: ")
			if !scanner.Scan() {
				break // Exit the loop if there's an error or EOF
			}
			cmd := strings.TrimSpace(scanner.Text())
			if cmd == "exit" {
				fmt.Println("Exiting program...")
				return
			}
			switch cmd {
			case "list_self":
				fmt.Println("This Node's ID is : ")
			case "list_mem":
				fmt.Println("Current Membership list is : ")
				membership.PrintMembershipList()
			case "leave":
				fmt.Println("Node xyz is leaving the membership list")
			case "toggle_sus":
				fmt.Println("Current value of PingSus is : , change it to __")
			case "status_sus":
				fmt.Println("Status of PingSus : ", suspicion.Enabled)
			case "sus_list":
				fmt.Println("List of all nodes which are marked as Suspicious for the current node :")
			default:
				fmt.Printf("Unknown command: %s\n", cmd)
			}
		}
	}()

	wg.Wait()
}

func ContinouslySendPings() {
	// pingpong.SendPing(status_sus, ping_count)
	ping.Sender(status_sus)
	// time.Sleep(300 * time.Millisecond)
}
