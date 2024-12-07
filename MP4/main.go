package main

import (
	"bufio"
	"fmt"
	"os"
	"os/signal"
	"rainstorm/file_transfer"
	"rainstorm/introducer"
	"rainstorm/membership"
	"rainstorm/ping"
	"rainstorm/ring"
	"rainstorm/scheduler"
	"rainstorm/utility"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

var (
	INTRODUCER_HOST = "fa24-cs425-5901.cs.illinois.edu"
	status_sus      = false //suspicion.DeclareSuspicion
	LOGGER_FILE     = "/home/log/rainstorm.log"
)

func main() {
	// args := os.Args
	// clearing the machine.log file
	file, err := os.OpenFile(LOGGER_FILE, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		fmt.Println("Error Opening file : " + err.Error())
	}

	file.Close()

	utility.SetupDirectories(utility.HYDFS_APPEND, utility.HYDFS_CACHE, utility.HYDFS_DIR, utility.HYDFS_TMP)
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
	go ring.StartRing()

	go file_transfer.HyDFSServer()

	go scheduler.InitializeScheduler()

	// RainStorm <op1 _exe> <op2 _exe> <hydfs_src_file> <hydfs_dest_filename> <num_tasks>
	// StartScheduler("/home/hydfs/files/1.txt", 3, "/home/hydfs/files/1.txt")

	// hash1, _ := utility.GetMD5("rainstorm1.txt")
	// fmt.Println(" Hash of first file : " + hash1)
	// hash2, _ := utility.GetMD5("copy_of_rainstorm1.txt")
	// fmt.Println(" Hash of second file : " + hash2)

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
		fmt.Println("Available commands for HyDFS management:")
		fmt.Println("  get               - fetches file from HyDFS to Local FS ") // fetches the entire file from HyDFS to localfilename on local dir
		fmt.Println("  get_from_replica  - fetches a file from a HyDFS node to Local FS")
		fmt.Println("  create            - push a local file to HyDFS")
		fmt.Println("  append            - append contents of a local file to file in HyDFS")
		fmt.Println("  merge             - merge all replicas of a file in HyDFS ")
		fmt.Println("  multiappend       - run concurrent appends from VMs to one file")
		fmt.Println("  ls                - list VM addresses where a file being stored")  // (along with the VMs’ IDs on the ring)
		fmt.Println("  store             - list all files (with ids) being stored on VM") // also the VM ID
		fmt.Println("  list_mem_ids      - Display current membership list along with Node ID on ring")
		fmt.Println("  exit              - Exit the program")
		fmt.Println("************************************************")
		fmt.Println("************************************************")
		fmt.Println("Available commands for RainStorm Stream Processing :")
		fmt.Println("  cluster_availibility - show status of all nodes in the rainstrom cluster")
		fmt.Println("  rainstorm            - Start RainStorm Stream Processing")
		fmt.Println("************************************************")
		scanner := bufio.NewScanner(os.Stdin)
		for {
			fmt.Print("Enter command: ")
			if !scanner.Scan() {
				break // Exit the loop if there's an error or EOF
			}
			var requestData file_transfer.ClientData
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
			// MP3 Commands
			case "get":
				fmt.Println("Enter HyDFS filename to fetch, and store into local file.")
				fmt.Print("Usage - HyDFSfilename /path/to/localfilename : ")
				scanner.Scan()
				args := strings.Fields(scanner.Text())
				if len(args) != 2 {
					fmt.Println("Invalid input. Usage: get <HyDFS_filename> <local_filename>")
				} else {
					fmt.Printf("Fetching %s from HyDFS to %s\n", args[0], args[1])
					// get here with args[0] and args[1]
					requestData.Operation = "get"
					requestData.Filename = args[0]
					requestData.LocalFilePath = args[1]
					file_transfer.HyDFSClient(requestData)
				}
			case "get_from_replica":
				fmt.Println("Enter VMaddress, HyDFS filename, and local filename to write the file to.")
				fmt.Print("Usage - VMaddress HyDFSfilename /path/to/localfilename : ")
				scanner.Scan()
				args := strings.Fields(scanner.Text())
				if len(args) != 3 {
					fmt.Println("Invalid input. Usage: get_from_replica <VM_address> <HyDFS_filename> <local_filename> ")
				} else {
					// get_from_replica here with args[0], args[1], and args[2]
					requestData.Operation = "get_from_replica"
					requestData.NodeAddr = args[0]
					requestData.Filename = args[1]
					requestData.LocalFilePath = args[2]
					file_transfer.HyDFSClient(requestData)
				}
			case "create":
				fmt.Println("Enter local filename  to upload to HyDFS file.")
				fmt.Print("Usage - localfilename HyDFSfilename : ")
				scanner.Scan()
				args := strings.Fields(scanner.Text())
				if len(args) != 2 {
					fmt.Println("Invalid input. Usage: <local_filename> <HyDFS_filename>")
				} else {
					// create here with args[0] and args[1]
					requestData.Operation = "create"
					requestData.LocalFilePath = args[0]
					requestData.Filename = args[1]
					file_transfer.HyDFSClient(requestData)
				}
			case "append":
				fmt.Println("Enter local filename to append to HyDFS file.")
				fmt.Print("Usage - localfilename HyDFSfilename : ")
				scanner.Scan()
				args := strings.Fields(scanner.Text())
				if len(args) != 2 {
					fmt.Println("Invalid input. Usage: append <local_filename> <HyDFS_filename>")
				} else {
					fmt.Printf("Appending %s to %s in HyDFS\n", args[0], args[1])
					// append function here with args[0] and args[1]
					requestData.Operation = "append"
					requestData.LocalFilePath = args[0]
					requestData.Filename = args[1]
					file_transfer.HyDFSClient(requestData)
				}
			case "merge":
				fmt.Print("Enter HyDFS file name to merge across all replicas: ")
				scanner.Scan()
				filename := strings.TrimSpace(scanner.Text())
				fmt.Printf("Merging all replicas of %s in HyDFS\n", filename)
				// merge function here with filename
				requestData.Operation = "merge"
				requestData.Filename = filename
				file_transfer.HyDFSClient(requestData)
			case "multiappend":
				fmt.Print("Enter HyDFS file name, VMs, and local file names to append. ")
				fmt.Print("Usage - filename; VMi, … VMj ; localfilenamei,....localfilenamej :")
				scanner.Scan()
				input := scanner.Text()

				// Split the input into parts
				parts := strings.Split(input, ";")
				if len(parts) != 3 {
					fmt.Println("Invalid input format")
				} else {
					// Extract filename
					filename := strings.TrimSpace(parts[0])

					// Extract VM IPs and local filenames
					vmIPs := strings.Split(strings.TrimSpace(parts[1]), ",")
					localFiles := strings.Split(strings.TrimSpace(parts[2]), ", ")

					for i, ip := range vmIPs {
						vmIPs[i] = strings.TrimSpace(ip)
					}

					// For localFiles
					for i, file := range localFiles {
						localFiles[i] = strings.TrimSpace(file)
					}

					requestData.Operation = "multiappend"
					requestData.Filename = filename

					file_transfer.HyDFSClient(requestData, vmIPs, localFiles)
				}
			case "delete":
				fmt.Print("Enter filename to delete from HyDFS: ")
				scanner.Scan()
				filename := strings.TrimSpace(scanner.Text())
				fmt.Printf("Deleting %s from HyDFS\n", filename)
				// delete function here with filename
				// TODO
			case "ls":
				fmt.Print("Fetch details of HyDFS filename : ")
				scanner.Scan()
				filename := strings.TrimSpace(scanner.Text())
				fmt.Printf("Listing VM addresses storing %s\n", filename)
				// ls functionality
				requestData.Operation = "ls"
				requestData.Filename = filename
				file_transfer.HyDFSClient(requestData)
			case "store":
				fmt.Println("Listing all files being stored on this VM")
				// store function here
				// requestData.Operation = "store"
				// file_transfer.HyDFSClient(requestData)
				hydfsFS := utility.GetAllHyDFSMetadata()
				ring.PrintVMRingID()
				for filename, v := range hydfsFS {
					fmt.Printf("Filename: %s, Ring ID: %d, md5 hash: %s, timestamp: %s\n", filename, v.RingId, v.Hash, v.Timestamp)
				}
			case "list_mem_ids":
				fmt.Println("Displaying current membership list along with Node ID on ring")
				ring.PrintRing()
				// list_mem_ids function here
			case "rainstorm":
				fmt.Println("RainStorm Stream Processing. Usage: <op1_exe> <op2_exe> <hydfs_src_file> <hydfs_dest_filename> <num_tasks>")
				fmt.Print("Enter command: ")
				scanner.Scan()
				args := strings.Fields(scanner.Text())
				if len(args) != 5 {
					fmt.Println("Invalid input. Usage: <op1_exe> <op2_exe> <hydfs_src_file> <hydfs_dest_filename> <num_tasks>")
				} else {
					// create here with args[0] and args[1]
					op1Exe := args[0]
					op2Exe := args[1]
					srcFilePath := args[2]
					destFilePath := args[3]
					numTasks, err := strconv.Atoi(args[4])
					if err != nil {
						fmt.Println("Invalid input. Usage: <op1_exe> <op2_exe> <hydfs_src_file> <hydfs_dest_filename> <num_tasks>")
					} else {
						scheduler.StartScheduler(srcFilePath, numTasks, destFilePath, op1Exe, op2Exe)
					}
				}
			case "cluster_availibility":
				fmt.Println("Showing status of all nodes in the rainstrom cluster")
				scheduler.PrintAvailableNodes()
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
