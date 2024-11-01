package file_transfer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hydfs/hydfs"
	"hydfs/utility"
	"io"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	port    = "6060"
	timeout = 10 * time.Millisecond
)

var LOGGER_FILE = "/home/log/hydfs.log"

type ClientData struct {
	Operation     string    `json:"operation"`
	Filename      string    `json:"filename,omitempty"`
	LocalFilePath string    `json:"local_path,omitempty"`
	NodeAddr      string    `json:"node_addr,omitempty"`
	Data          []byte    `json:"data,omitempty"`
	RingID        uint32    `json:"ringId,omitempty"`
	TimeStamp     time.Time `json:"timestamp,omitempty"`
}

type Response struct {
	IP   string
	Data []byte
	Err  error
}

type FileMetaData struct {
	Hash      string
	Timestamp time.Time
	RingId    uint32
}

var hydfsFileStore = map[string]FileMetaData{} //key is filename

// This file will contain file fetching ,
// file writing/appending
// file merging methods

// Client and Server code

// Client puts in request for file fetching , file append

func HyDFSServer() {

	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		utility.LogMessage("Error listening: " + err.Error())
		return
	}
	defer listener.Close()

	utility.LogMessage("HyDFS Server is listening on port = " + port)

	for {
		conn, err := listener.Accept()
		if err != nil {
			utility.LogMessage("Error accepting connection : " + err.Error())
			continue
		}
		go handleIncomingFileConnection(conn)
	}
}

func handleIncomingFileConnection(conn net.Conn) {
	defer conn.Close()

	// Add logic to do action based on type of input being sent

	data, err := io.ReadAll(conn)
	if err != nil {
		utility.LogMessage("Error reading data: " + err.Error())
		return
	}

	var parsedData ClientData
	jsonErr := json.Unmarshal(data, &parsedData)
	if jsonErr != nil {
		utility.LogMessage("Error parsing JSON: " + jsonErr.Error())
	}

	cmd := parsedData.Operation

	switch cmd {
	case "get", "get_from_replica":
		hydfsPath := parsedData.Filename
		// ipAddr := parsedData.NodeAddr
		fmt.Printf("Fetching file %s from this HyDFS node \n", hydfsPath)

		if !utility.FileExists(hydfsPath) {
			errorMsg := "File does not exist on this replica"
			utility.LogMessage(errorMsg)
			conn.Write([]byte(errorMsg))
			return
		}

		fileData, err := os.ReadFile(hydfsPath)
		if err != nil {
			errorMsg := "Error reading file: " + err.Error()
			utility.LogMessage(errorMsg)
			conn.Write([]byte(errorMsg))
			return
		}

		//sending file data back to the client
		_, err = conn.Write(fileData)
		if err != nil {
			utility.LogMessage("Error sending file data: " + err.Error())
			return
		}

		utility.LogMessage("File " + hydfsPath + " sent successfully")

	case "create":
		hydfsPath := parsedData.Filename

		fmt.Printf("Creating file %s in HyDFS \n", hydfsPath)
		if utility.FileExists(hydfsPath) {
			fmt.Println("File exists")
		} else {
			fmt.Println("File does not exist")
		}

		// file write
		err = os.WriteFile(hydfsPath, parsedData.Data, 0644)
		if err != nil {
			utility.LogMessage("Error writing file: " + err.Error())
			return
		}

		// write to virtual representation
		filehash, _ := utility.GetMD5(hydfsPath)
		hydfsFileStore[hydfsPath] = FileMetaData{
			Hash:      filehash,
			Timestamp: parsedData.TimeStamp,
			RingId:    parsedData.RingID,
		}

		utility.LogMessage("File created successfully: " + hydfsPath)

		// Send response back to client
		response := "File created successfully"
		_, err = conn.Write([]byte(response))
		if err != nil {
			utility.LogMessage("Error sending response: " + err.Error())
		}

		// won't be sending create req for the same file
	case "append":
		hydfsPath := parsedData.Filename
		if utility.FileExists(hydfsPath) {
			fmt.Println("File exists")
		} else {
			fmt.Println("File does not exist")
		}

		// handleAppend(filename, localPath)

	case "merge":
		hydfsPath := parsedData.Filename
		fmt.Printf("Merging all replicas of %s in HyDFS\n", hydfsPath)

		// Check with hash value being sent, if hash is diff then
		// ask for file and make changes to hydfs file

	case "delete":
		hydfsPath := parsedData.Filename
		fmt.Printf("Deleting %s from HyDFS\n", hydfsPath)

		// handleDelete(filename)

	case "ls":
		hydfsPath := parsedData.Filename
		fmt.Printf("Listing VM addresses storing %s\n", hydfsPath)

		// handleLs(filename)

	case "store":
		fmt.Println("Listing all files being stored on this VM")

		// handleStore()

	case "list_mem_ids":
		fmt.Println("Displaying current membership list along with Node ID on ring")

		// handleListMemIds()

	default:
		utility.LogMessage("Unknown command: " + cmd)
	}
	/*
		filename := string(buffer[:n])
		utility.LogMessage("Receiving file: " + filename)

		file, err := os.Create(filepath.Join("received", filename))
		if err != nil {
			utility.LogMessage("Error creating file: " + err.Error())
			return
		}
		defer file.Close()

		_, err = io.Copy(file, conn)
		if err != nil {
			utility.LogMessage("Error receiving file data : " + err.Error())
			return
		}

		utility.LogMessage("File " + filename + " received successfully")
	*/
}

func HyDFSClient(request ClientData) {

	cmd := request.Operation
	filename := request.Filename

	// preprocess data recv from cli -- done
	// do checks for filename  -- done
	// get the nodes to which you have to send the requests to -- hashing -- method akhil will provide
	// pass all the data to a sender method -- ip , data and response
	// sender method will send the data and wait for response (?)
	// each switch case will need to handle response in a different way (?)
	// we need this to be blocking to the main.go code
	// once the response is done, print to output and continue listening for new commands

	switch cmd {
	case "get":
		localPath := request.LocalFilePath
		if utility.FileExists(localPath) {
			fmt.Println("File exists")
		} else {
			fmt.Println("File does not exist")
		}
		// add a check for checking if the file is in cache
		// if yes, then ensure we pull from there and update its LRU counter in the cache
		// can implement LRU in a log file, read and write to that file
		// handleGet(filename, localPath)

	case "get_from_replica":
		localPath := request.LocalFilePath
		nodeAddr := request.NodeAddr
		fmt.Printf("Fetching file %s from HyDFS node %s to local path %s\n", filename, nodeAddr, localPath)
		if utility.FileExists(localPath) {
			fmt.Println("File exists")
		} else {
			fmt.Println("File does not exist")
		}

		// channel to receive the response
		responses := make(chan Response, 1)

		// Call sendRequest with the node address
		go sendRequest(nodeAddr, request, responses)

		// wait for the response on channel
		response := <-responses
		close(responses)

		if response.Err != nil {
			utility.LogMessage("Error fetching file from replica: " + response.Err.Error())
		} else {
			utility.LogMessage("File fetched successfully from replica")
			err := writeResponseToFile(response.Data, localPath)
			if err != nil {
				utility.LogMessage("Error writing file to local path: " + err.Error())
			} else {
				utility.LogMessage("File saved to " + localPath)
			}
		}

		utility.LogMessage("File fetched and saved to " + localPath)

		// handleGetFromReplica(filename, localPath, nodeID)

	case "create":
		localPath := request.LocalFilePath

		fmt.Printf("Creating file %s in HyDFS from local file %s\n", filename, localPath)
		if utility.FileExists(localPath) {
			fmt.Println("File exists")
		} else {
			fmt.Println("File does not exist")
		}
		fileData, err := os.ReadFile(localPath)
		if err != nil {
			utility.LogMessage("Error reading local file: " + err.Error())
			return
		}

		request.Data = fileData
		fileID, senderIPs := GetSuccesorIPsForFilename(filename)
		request.RingID = fileID
		utility.LogMessage("Successor IPs assigned - " + senderIPs[0] + ", " + senderIPs[1] + ", " + senderIPs[2])

		responses := SendRequestToNodes(senderIPs, request)
		for _, response := range responses {
			if response.Err != nil {
				utility.LogMessage("Error from " + response.IP + ": " + response.Err.Error())
			} else {
				utility.LogMessage("File created successfully on " + response.IP)
			}
		}

		// handleCreate(filename, localPath)
		// we won't be sending create req for the same file
	case "append":
		localPath := request.LocalFilePath
		if utility.FileExists(localPath) {
			fmt.Println("File exists")
		} else {
			fmt.Println("File does not exist")
		}

		// handleAppend(filename, localPath)

	case "merge":
		fmt.Printf("Merging all replicas of %s in HyDFS\n", filename)

		// handleMerge(filename)

	case "delete":
		fmt.Printf("Deleting %s from HyDFS\n", filename)

		// handleDelete(filename)

	case "ls":
		fmt.Printf("Listing VM addresses storing %s\n", filename)

		// handleLs(filename)

	case "store":
		fmt.Println("Listing all files being stored on this VM")

		// handleStore()

	case "list_mem_ids":
		fmt.Println("Displaying current membership list along with Node ID on ring")

		// handleListMemIds()

	default:
		utility.LogMessage("Unknown command: " + cmd)
	}
}

func GetSuccesorIPsForFilename(filename string) (uint32, []string) {
	ringID := hydfs.Hashmurmur(filename)
	utility.LogMessage("Ring ID generated for filename " + filename + ": " + strconv.FormatInt(int64(ringID), 10))

	// ips = hydfs.GetSuccessorNodeIps(ringID)
	ips := []string{"1.1.1.1", "2.2.2.2", "3.3.3.3"}

	return ringID, ips
}

func SendRequestToNodes(ips []string, request ClientData) []Response {
	responses := make(chan Response, len(ips))
	var wg sync.WaitGroup

	for _, ip := range ips {
		wg.Add(1)
		go func(ip string) {
			defer wg.Done()
			sendRequest(ip, request, responses)
		}(ip)
	}

	go func() {
		wg.Wait()
		close(responses)
	}()

	return collectResponses(responses, request, len(ips))
}

func sendRequest(ip string, request ClientData, responses chan<- Response) {
	// Establish TCP connection
	conn, err := net.DialTimeout("tcp", ip+":"+port, 10*time.Second)
	if err != nil {
		responses <- Response{IP: ip, Err: fmt.Errorf("connection error: %v", err)}
		return
	}
	defer conn.Close()

	// Marshal the request to JSON
	jsonData, err := json.Marshal(request)
	if err != nil {
		responses <- Response{IP: ip, Err: fmt.Errorf("JSON marshaling error: %v", err)}
		return
	}

	// Send the JSON data
	_, err = conn.Write(jsonData)
	if err != nil {
		responses <- Response{IP: ip, Err: fmt.Errorf("send error: %v", err)}
		return
	}

	// Read the response
	var buffer bytes.Buffer
	_, err = io.Copy(&buffer, conn)
	if err != nil {
		responses <- Response{IP: ip, Err: fmt.Errorf("receive error: %v", err)}
		return
	}

	responses <- Response{IP: ip, Data: buffer.Bytes()}
}

func collectResponses(responses <-chan Response, request ClientData, limit int) []Response {
	var result []Response
	for resp := range responses {
		if resp.Err == nil && request.LocalFilePath != "" {
			err := writeResponseToFile(resp.Data, request.LocalFilePath)
			if err != nil {
				resp.Err = fmt.Errorf("failed to write response to file: %v", err)
			}
		}
		result = append(result, resp)
		if len(result) == limit {
			break
		}
	}
	return result
}

func writeResponseToFile(data []byte, filePath string) error {
	return os.WriteFile(filePath, data, 0644)
}
