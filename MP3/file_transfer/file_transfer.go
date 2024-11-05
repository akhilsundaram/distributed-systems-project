package file_transfer

import (
	"bufio"
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

var (
	LOGGER_FILE = "/home/log/hydfs.log"
	HYDFS_DIR   = "/home/hydfs/files"
	HYDFS_CACHE = "/home/hydfs/cache"
	HYDFS_TMP   = "/home/hydfs/tmp"
)

type ClientData struct {
	Operation     string    `json:"operation"`
	Filename      string    `json:"filename,omitempty"`
	LocalFilePath string    `json:"local_path,omitempty"`
	NodeAddr      string    `json:"node_addr,omitempty"`
	Data          []byte    `json:"data,omitempty"`
	RingID        uint32    `json:"ringId,omitempty"`
	TimeStamp     time.Time `json:"timestamp,omitempty"`
}

/*
type Response struct {
	IP        string    `json:"ip"`
	Data      []byte    `json:"data"`
	TimeStamp time.Time `json:"timestamp,omitempty"`
	Hash      string    `json:"hash,omitempty"`
	RingId    uint32    `json:"ring_id,omitempty"`
	Err       string    `json:"error,omitempty"`
}*/

type Response struct {
	IP        string
	Data      []byte
	TimeStamp time.Time
	Hash      string
	RingId    uint32
	Err       error
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

// Client and Server code - Client puts in request for file fetching , file append

func HyDFSServer() {

	utility.LogMessage("Starting HyDFS Server to listen for incoming connections")

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

	buffer := make([]byte, 4096) // 4KB buffer
	var data []byte

	for {
		n, err := conn.Read(buffer)
		if err != nil {
			if err == io.EOF {
				break // End of data
			}
			utility.LogMessage("Error reading data: " + err.Error())
			return
		}
		data = append(data, buffer[:n]...)
	}

	var parsedData ClientData
	jsonErr := json.Unmarshal(data, &parsedData)
	if jsonErr != nil {
		utility.LogMessage("Error parsing JSON: " + jsonErr.Error())
	}

	cmd := parsedData.Operation
	utility.LogMessage("Incoming connection received, cmd = " + cmd)
	// Process the request and prepare the response
	// resp := Response{
	//    Data:      []byte(""),
	//}
	switch cmd {
	case "get", "get_from_replica":
		hydfsPath := HYDFS_DIR + "/" + parsedData.Filename

		// ipAddr := parsedData.NodeAddr
		utility.LogMessage("Fetching file " + hydfsPath + " from this HyDFS node")

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

		/*
			// Marshal the response to JSON
			jsonResp, err := json.Marshal(resp)
			if err != nil {
				// Handle error (e.g., log it)
				errorMsg := []byte("Error creating JSON response")
				conn.Write(errorMsg)
				return
			}
		*/
		//sending file data back to the client
		utility.LogMessage("filedata : " + string(fileData))
		_, err = conn.Write(fileData) // _, err = conn.Write(jsonResp)
		if err != nil {
			utility.LogMessage("Error sending file data: " + err.Error())
			return
		}

		utility.LogMessage("File " + hydfsPath + " sent successfully")

	case "create":
		hydfsPath := HYDFS_DIR + "/" + parsedData.Filename

		fmt.Printf("Creating file %s in HyDFS \n", hydfsPath)
		if utility.FileExists(hydfsPath) {
			utility.LogMessage("File with name exists")
		}

		// file write
		err := os.WriteFile(hydfsPath, parsedData.Data, 0644)
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
		bufW := bufio.NewWriter(conn)
		_, err = bufW.WriteString(response)
		if err != nil {
			utility.LogMessage("error writing resp to write buffer - " + err.Error())
		}

		err = bufW.Flush()
		if err != nil {
			utility.LogMessage("error on buffer writer flush - " + err.Error())
		}

		//_, err = conn.Write([]byte(response))
		//if err != nil {
		//	utility.LogMessage("Error sending response: " + err.Error())
		//}

		// won't be sending create req for the same file
	case "append":
		hydfsPath := HYDFS_DIR + "/" + parsedData.Filename
		if utility.FileExists(hydfsPath) {
			fmt.Println("File exists")
		} else {
			fmt.Println("File does not exist")
		}

		// handleAppend(filename, localPath)

	case "merge":
		hydfsPath := HYDFS_DIR + "/" + parsedData.Filename
		fmt.Printf("Merging all replicas of %s in HyDFS\n", hydfsPath)

		// Check with hash value being sent, if hash is diff then
		// ask for file and make changes to hydfs file

	case "delete":
		hydfsPath := HYDFS_DIR + "/" + parsedData.Filename
		fmt.Printf("Deleting %s from HyDFS\n", hydfsPath)

		// handleDelete(filename)

	case "ls":
		hydfsPath := HYDFS_DIR + "/" + parsedData.Filename
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

		// will change this condition to include caching, but right now we overwrite files
		if utility.FileExists(localPath) {
			utility.LogMessage("File exists, will be overwritten")
		} else {
			utility.LogMessage("File does not exist, will be created")
		}
		// add a check for checking if the file is in cache
		// if yes, then ensure we pull from there and update its LRU counter in the cache
		// can implement LRU in memory, as a variable just like file hash and timestamp that we have right now. See implementations for this , using heap ?
		// need to push out the entry which has not been used / used the least

		// filename = filename of file in hydfs
		fileID, senderIPs := GetSuccesorIPsForFilename(filename)
		request.RingID = fileID
		utility.LogMessage("Successor IPs assigned - " + senderIPs[0] + ", " + senderIPs[1] + ", " + senderIPs[2])

		// remove lines when ring ID integrated
		clear(senderIPs)
		senderIPs = []string{"172.22.94.195"}

		responses := SendRequestToNodes(senderIPs, request)
		for _, response := range responses {
			if response.Err != nil {
				utility.LogMessage("Error from " + response.IP + ": " + response.Err.Error())
			} else {
				utility.LogMessage("File recevied successfully from " + response.IP)
			}
		}
		// handleGet(filename, localPath)

	case "get_from_replica":
		localPath := request.LocalFilePath
		nodeAddr := request.NodeAddr
		fmt.Printf("Fetching file %s from HyDFS node %s to local path %s\n", filename, nodeAddr, localPath)
		if utility.FileExists(localPath) {
			utility.LogMessage("File exists, will be overwritten")
		} else {
			utility.LogMessage("File does not exist, will be created")
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
			utility.LogMessage("Response : " + string(response.Data))
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
			utility.LogMessage("File at " + localPath + " exists")
		} else {
			fmt.Println("File does not exist")
			return
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

		// remove lines when ring ID integrated
		clear(senderIPs)
		senderIPs = []string{"172.22.94.195"}

		responses := SendRequestToNodes(senderIPs, request)
		for _, response := range responses {
			if response.Err == nil {
				fmt.Println("File created successfully on " + response.IP) // can print file Ring ID if needed
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
	utility.LogMessage("")
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
	utility.LogMessage("Sending TCP request to " + ip)
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
	readDeadline := time.Now().Add(5 * time.Second)
	conn.SetReadDeadline(readDeadline)

	for {
		// Read in small chunks
		chunk := make([]byte, 4096)
		n, err := conn.Read(chunk)
		utility.LogMessage("reading in chunks : " + string(chunk[:n]))
		if err != nil {
			if err == io.EOF {
				utility.LogMessage("EOF")
				break // End of response
			}
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				utility.LogMessage("Timeout")
				break // Timeout, assume end of response
			}
			responses <- Response{IP: ip, Err: fmt.Errorf("receive error: %v", err)}
			return
		}
		data := chunk[:n]
		utility.LogMessage("reading in chunks : " + string(data))
		buffer.Write(chunk[:n])

		// Check if we've reached the deadline
		if time.Now().After(readDeadline) {
			break
		}
	}

	utility.LogMessage("Received response for file write from : " + ip)
	utility.LogMessage("Buffer value : " + buffer.String())
	responses <- Response{IP: ip, Data: buffer.Bytes()}
}

func collectResponses(responses <-chan Response, request ClientData, limit int) []Response {
	// After getting response, what should be done to the response before sending control
	// back to the client terminal
	var result []Response
	cmd := request.Operation
	counter := 0
	utility.LogMessage("Recevied response from server")
	for resp := range responses {

		if resp.Err != nil {
			// recieve error , parsed from conn buffer
			utility.LogMessage("Error from ip " + resp.IP + " - " + resp.Err.Error())
		} else {
			switch cmd {
			case "get":
				utility.LogMessage("File Get Ack from IP : " + resp.IP + ", file timestamp = " + resp.TimeStamp.String())
				err := writeResponseToFile(resp.Data, request.LocalFilePath)
				if err != nil {
					resp.Err = fmt.Errorf("failed to write response to file: %v", err)
				}
				fmt.Println("File written at path : " + request.LocalFilePath)
			case "merge":
				utility.LogMessage("File Merge request Ack from IP : " + resp.IP + ", file timestamp = " + resp.TimeStamp.String())
			case "create":
				// handle write responses
				utility.LogMessage("File Write Ack from IP (written successfully): " + resp.IP + ", ack msg : " + string(resp.Data))
				counter += 1
			default:
				utility.LogMessage("Unknown command in CollectResponses: " + cmd)
			}
			result = append(result, resp)
		}
		if len(result) == limit {
			// at how many responses do we stop parsing , eg if we quorum == 2 , then set limit to 2
			break
		}
	}
	return result
}

func writeResponseToFile(data []byte, filePath string) error {
	return os.WriteFile(filePath, data, 0644)
}

// not used
func WriteToConnBuffer(conn net.Conn, filePath string) error {
	// Open the file
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("error opening file: %v", err)
	}
	defer file.Close()

	// scanner to read the file
	fileScanner := bufio.NewScanner(file)

	// Write Buffer
	bufW := bufio.NewWriter(conn)

	// Read and write file line by line
	for fileScanner.Scan() {
		line := fileScanner.Text()
		if line != "" {
			_, err := bufW.WriteString(line)
			if err != nil {
				return fmt.Errorf("error writing file line to write buffer: %v", err)
			}

			// Write newline character at the end of each line
			err = bufW.WriteByte('\n')
			if err != nil {
				return fmt.Errorf("error writing newline to buffer: %v", err)
			}
		}
	}

	// Check for errors during file scanning
	if err := fileScanner.Err(); err != nil {
		return fmt.Errorf("error reading file: %v", err)
	}

	// Flush
	err = bufW.Flush()
	if err != nil {
		return fmt.Errorf("error on buffer writer flush: %v", err)
	}

	return nil
}

// not used
func ReadFromConnBuffer(conn net.Conn, filePath string) error {
	// Create or open the file for writing
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("error creating file: %v", err)
	}
	defer file.Close()

	// Create a buffered reader for the connection
	bufR := bufio.NewReader(conn)

	// Create a buffered writer for the file
	bufW := bufio.NewWriter(file)
	defer bufW.Flush()

	// Read from connection and write to file
	buffer := make([]byte, 4096) // 4KB buffer

	for {
		n, err := bufR.Read(buffer)
		if err != nil {
			if err == io.EOF {
				break // End of file, exit loop
			}
			return fmt.Errorf("error reading from connection: %v", err)
		}

		// Write the read bytes to the file
		_, err = bufW.Write(buffer[:n])
		if err != nil {
			return fmt.Errorf("error writing to file: %v", err)
		}
	}

	return nil
}
