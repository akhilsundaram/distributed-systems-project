package main

import (
	"bufio"
	"fmt"
	"hydfs/introducer"
	"hydfs/membership"
	"hydfs/ping"
	"hydfs/utility"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

var (
	INTRODUCER_HOST = "fa24-cs425-5901.cs.illinois.edu"
	status_sus      = false //suspicion.DeclareSuspicion
	LOGGER_FILE     = "/home/log/hydfs.log"
	HYDFS_DIR       = "/home/hydfs/files"
	HYDFS_CACHE     = "/home/hydfs/cache"
)

func main() {
	// args := os.Args
	// clearing the machine.log file
	file, err := os.OpenFile(LOGGER_FILE, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		fmt.Println("Error Opening file : " + err.Error())
	}

	file.Close()

	utility.SetupDirectories(HYDFS_DIR, HYDFS_CACHE)
	if err != nil {
		fmt.Printf("Error setting up directories: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("Directories setup completed successfully")

	sigChannel := make(chan os.Signal, 1)
	signal.Notify(sigChannel, syscall.SIGUSR1)
	go func() {
		// Block until a signal is received
		for {
			sig := <-sigChannel
			switch sig {
			case syscall.SIGUSR1:
				fmt.Println("Received signal from VM to change suspicion state")
				membership.SuspicionEnabled = !membership.SuspicionEnabled
				utility.LogMessage("Suspicion set to : " + strconv.FormatBool(membership.SuspicionEnabled))
			}
		}
	}()

	hostname, err := os.Hostname()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	membership.My_hostname = hostname
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

	// starting ping listener on every node
	go ping.Listener()

	// sending pings
	go ContinouslySendPings()

	// Call HyDFS ?
	membership.RingMemberchan = make(chan membership.RingMemberMessage)

	fmt.Println("Program running. PID:", os.Getpid())

	var wg sync.WaitGroup
	wg.Add(1)

	// Start a goroutine to handle CLI input
	go func() {
		defer wg.Done()

		fmt.Println("Available commands for Membership List:")
		fmt.Println("  list_self   - Display this node's ID")
		fmt.Println("  list_mem    - Display current membership list")
		fmt.Println("  leave       - Leave the membership list")
		fmt.Println("  enable_sus  - enable suspicion mode")
		fmt.Println("  disable_sus - disable suspicion mode")
		fmt.Println("  status_sus  - Show status of suspicion mode")
		fmt.Println("  sus_list    - List suspicious nodes")
		fmt.Println("************************************************")
		fmt.Println("************************************************")
		fmt.Println("Available commands for HyDFS management:")
		fmt.Println("  get               - fetches file from HyDFS to Local FS ") // fetches the entire file from HyDFS to localfilename on local dir
		fmt.Println("  get_from_replica  - fetches a file from a HyDFS node to Local FS")
		fmt.Println("  create            - push a local file to HyDFS")
		fmt.Println("  append            - append contents of a local file to file in HyDFS")
		fmt.Println("  merge             - merge all replicas of a file in HyDFS ")
		fmt.Println("  delete            - delete a file completely from HyDFS")
		fmt.Println("  ls                - list VM addresses where a file being stored")  // (along with the VMs’ IDs on the ring)
		fmt.Println("  store             - list all files (with ids) being stored on VM") // also the VM ID
		fmt.Println("  list_mem_ids      - Display current membership list along with Node ID on ring")
		fmt.Println("  exit              - Exit the program")
		fmt.Println("************************************************")
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
				fmt.Println("Current Node ID is : ", membership.GetMemberID(hostname))
			case "list_mem":
				fmt.Println("Current Membership list is : ")
				membership.PrintMembershipListStdOut()
			case "leave":
				fmt.Println("Node xyz is leaving the membership list")
				return
			case "enable_sus":
				curr_val := membership.SuspicionEnabled
				if curr_val {
					fmt.Println("Suspicion is already enabled !!! ")
				} else {
					membership.SuspicionEnabled = true
					fmt.Println("Suspicion is set to = ", membership.SuspicionEnabled)
				}
			case "disable_sus":
				curr_val := membership.SuspicionEnabled
				if !curr_val {
					fmt.Println("Suspicion is already disabled !!! ")
				} else {
					membership.SuspicionEnabled = false
					fmt.Println("Suspicion is set to = ", membership.SuspicionEnabled)
				}
			case "status_sus":
				fmt.Println("Status of PingSus : ", membership.SuspicionEnabled)
			case "sus_list":
				fmt.Println("List of all nodes which are marked as Suspicious for the current node :")
			case "get":
				fmt.Println("Enter HyDFS filename to fetch, and store into local file.")
				fmt.Print("Usage - get HyDFSfilename /path/to/localfilename : ")
				scanner.Scan()
				args := strings.Fields(scanner.Text())
				if len(args) != 3 || args[0] != "get" {
					fmt.Println("Invalid input. Usage: get <HyDFS_filename> <local_filename>")
				} else {
					fmt.Printf("Fetching %s from HyDFS to %s\n", args[1], args[2])
					// get function here with args[0] and args[1]
				}
			case "get_from_replica":
				fmt.Println("Enter VMaddress, HyDFS filename, and local filename to write the file to.")
				fmt.Print("Usage - get_from_replica VMaddress HyDFSfilename /path/to/localfilename : ")
				scanner.Scan()
				args := strings.Fields(scanner.Text())
				if len(args) != 4 || args[0] != "get_from_replica" {
					fmt.Println("Invalid input. Usage: get_from_replica <VM_address> <HyDFS_filename> <local_filename> ")
				} else {
					fmt.Printf("Fetching %s from HyDFS node %s to %s\n", args[2], args[1], args[3])
					// get_from_replica function here with args[0], args[1], and args[2]
				}
			case "create":
				fmt.Println("Enter local filename  to upload to HyDFS file.")
				fmt.Print("Usage - create localfilename HyDFSfilename : ")
				scanner.Scan()
				args := strings.Fields(scanner.Text())
				if len(args) != 3 || args[0] != "create" {
					fmt.Println("Invalid input. Usage: create <local_filename> <HyDFS_filename>")
				} else {
					fmt.Printf("Pushing %s to HyDFS as %s\n", args[0], args[1])
					// create function here with args[0] and args[1]
				}
			case "append":
				fmt.Println("Enter local filename to append to HyDFS file.")
				fmt.Print("Usage - append localfilename HyDFSfilename : ")
				scanner.Scan()
				args := strings.Fields(scanner.Text())
				if len(args) != 3 || args[0] != "append" {
					fmt.Println("Invalid input. Usage: append <local_filename> <HyDFS_filename>")
				} else {
					fmt.Printf("Appending %s to %s in HyDFS\n", args[0], args[1])
					// append function here with args[0] and args[1]
				}
			case "merge":
				fmt.Print("Enter HyDFS file name to merge across all replicas: ")
				scanner.Scan()
				filename := strings.TrimSpace(scanner.Text())
				fmt.Printf("Merging all replicas of %s in HyDFS\n", filename)
				// merge function here with filename
			case "delete":
				fmt.Print("Enter filename to delete from HyDFS: ")
				scanner.Scan()
				filename := strings.TrimSpace(scanner.Text())
				fmt.Printf("Deleting %s from HyDFS\n", filename)
				// delete function here with filename
			case "ls":
				fmt.Print("Fetch details of HyDFS filename : ")
				scanner.Scan()
				filename := strings.TrimSpace(scanner.Text())
				fmt.Printf("Listing VM addresses storing %s\n", filename)
				// ls functionality
			case "store":
				fmt.Println("Listing all files being stored on this VM")
				// store function here
			case "list_mem_ids":
				fmt.Println("Displaying current membership list along with Node ID on ring")
				// list_mem_ids function here
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
