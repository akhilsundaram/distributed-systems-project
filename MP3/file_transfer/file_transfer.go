package file_transfer

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"hydfs/ring"
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

type ClientData struct {
	Operation     string    `json:"operation"`
	Filename      string    `json:"filename,omitempty"`
	LocalFilePath string    `json:"local_path,omitempty"`
	NodeAddr      string    `json:"node_addr,omitempty"`
	Data          []byte    `json:"data,omitempty"`
	RingID        uint32    `json:"ringId,omitempty"`
	TimeStamp     time.Time `json:"timestamp,omitempty"`
}

type ResponseJson struct {
	IP        string    `json:"ip"`
	Data      []byte    `json:"data"`
	TimeStamp time.Time `json:"timestamp,omitempty"`
	Hash      string    `json:"hash,omitempty"`
	RingId    uint32    `json:"ring_id,omitempty"`
	Err       string    `json:"error,omitempty"`
}

type Response struct {
	IP        string
	Data      []byte
	TimeStamp time.Time
	Hash      string
	RingId    uint32
	Err       error
}

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
	serverAddr := conn.RemoteAddr().String()
	resp := ResponseJson{
		IP: serverAddr,
	}
	switch cmd {
	case "get", "get_from_replica":
		hydfsPath := utility.HYDFS_DIR + "/" + parsedData.Filename

		// ipAddr := parsedData.NodeAddr
		utility.LogMessage("Fetching file " + hydfsPath + " from this HyDFS node")

		if !utility.FileExists(hydfsPath) {
			resp.Err = "File does not exist on this replica"
			utility.LogMessage(resp.Err)
		} else {
			fileData, err := os.ReadFile(hydfsPath)
			if err != nil {
				resp.Err = "Error reading file: " + err.Error()
				utility.LogMessage(resp.Err)
			} else {
				resp.Data = fileData
				utility.LogMessage("File " + hydfsPath + " read successfully")
			}
		}

		//sending file data back to the client

		utility.LogMessage("File " + hydfsPath + " sent successfully")

	case "create":
		hydfsPath := utility.HYDFS_DIR + "/" + parsedData.Filename

		fmt.Printf("Creating file %s in HyDFS \n", hydfsPath)
		if utility.FileExists(hydfsPath) {
			utility.LogMessage("File with name exists")
		}

		// file write
		err := os.WriteFile(hydfsPath, parsedData.Data, 0644)
		if err != nil {
			resp.Err = "Error writing file: " + err.Error()
			utility.LogMessage(resp.Err)
		} else {
			// write to virtual representation
			filehash, _ := utility.GetMD5(hydfsPath)
			utility.HydfsFileStore[hydfsPath] = utility.FileMetaData{
				Hash:      filehash,
				Timestamp: parsedData.TimeStamp,
				RingId:    parsedData.RingID,
			}
			resp.Data = []byte("File created successfully: " + hydfsPath)
			utility.LogMessage(string(resp.Data))
		}
		// won't be sending create req for the same file
	case "append":
		hydfsPath := utility.HYDFS_DIR + "/" + parsedData.Filename
		if utility.FileExists(hydfsPath) {
			fmt.Println("File exists")
		} else {
			fmt.Println("File does not exist")
		}

		// handleAppend(filename, localPath)

	case "merge":
		hydfsPath := utility.HYDFS_DIR + "/" + parsedData.Filename
		fmt.Printf("Merging all replicas of %s in HyDFS\n", hydfsPath)

		// Check with hash value being sent, if hash is diff then
		// ask for file and make changes to hydfs file

	case "delete":
		hydfsPath := utility.HYDFS_DIR + "/" + parsedData.Filename
		fmt.Printf("Deleting %s from HyDFS\n", hydfsPath)

		// handleDelete(filename)

	case "ls":
		hydfsPath := utility.HYDFS_DIR + "/" + parsedData.Filename
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

	// Marshal the response to JSON
	jsonResp, err := json.Marshal(resp)
	if err != nil {
		errorMsg := "Error creating JSON response: " + err.Error()
		utility.LogMessage(errorMsg)
		conn.Write([]byte(errorMsg))
		return
	}

	// Send JSON response back to the client
	_, err = conn.Write(jsonResp)
	if err != nil {
		utility.LogMessage("Error sending JSON response: " + err.Error())
		return
	}

	utility.LogMessage("Response sent successfully")
}

func HyDFSClient(request ClientData) {

	cmd := request.Operation
	filename := request.Filename
	var wg sync.WaitGroup
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

		wg.Add(1)
		go func() {
			defer wg.Done()
			response, err := SendRequest(nodeAddr, request)
			if err != nil {
				utility.LogMessage(fmt.Sprintf("Error in get_from_replica: %v", err))
				return
			}
			if response.Err != "" {
				utility.LogMessage(fmt.Sprintf("Error from server: %s", response.Err))
				return
			}
			// Handle the response data (e.g., write to file)
			err = os.WriteFile(localPath, response.Data, 0644)
			if err != nil {
				utility.LogMessage(fmt.Sprintf("Error writing file: %v", err))
			}
		}()

		// Wait for the goroutine to finish
		wg.Wait()
		utility.LogMessage("File fetch operation completed")

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
		request.TimeStamp = time.Now()
		utility.LogMessage("Successor IPs assigned - " + senderIPs[0] + ", " + senderIPs[1] + ", " + senderIPs[2])

		// remove lines when ring ID integrated
		// clear(senderIPs)
		// senderIPs = []string{"172.22.94.195"}

		for i := 0; i < len(senderIPs); i++ {
			wg.Add(1)
			go func(ip_addr string) {
				defer wg.Done()
				response, err := SendRequest(ip_addr, request)
				if err != nil {
					utility.LogMessage(fmt.Sprintf("Error in create: %v", err))
					return
				}
				if response.Err != "" {
					utility.LogMessage(fmt.Sprintf("Error from server: %s", response.Err))
					return
				}
				utility.LogMessage(fmt.Sprintf("File created successfully: %s", string(response.Data)))
			}(senderIPs[i])
		}

		// Wait for the goroutine to finish
		wg.Wait()
		utility.LogMessage("File create operation completed")

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
	ringID := utility.Hashmurmur(filename)
	servers_list := ring.GetFileNodes(filename)

	var ips []string
	for _, server := range servers_list {
		ips = append(ips, utility.GetIPAddr(server).String())
	}
	utility.LogMessage("Ring ID generated for filename " + filename + ": " + strconv.FormatInt(int64(ringID), 10))
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
			SendRequest(ip, request)
		}(ip)
	}

	go func() {
		wg.Wait()
		close(responses)
	}()

	return collectResponses(responses, request, len(ips))
}

func SendRequest(ip string, request ClientData) (*ResponseJson, error) {
	// Establish TCP connection
	utility.LogMessage("Sending TCP request to " + ip)

	// Marshal the request to JSON
	jsonData, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("error marshaling request: %v", err)
	}

	conn, err := net.DialTimeout("tcp", ip+":"+port, 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("error connecting to %s: %v", ip, err)
	}
	defer conn.Close()

	tcpConn, ok := conn.(*net.TCPConn)
	if ok {
		tcpConn.SetNoDelay(true)
	}

	// Send the JSON data
	_, err = conn.Write(jsonData)
	if err != nil {
		return nil, fmt.Errorf("error sending data to %s: %v", ip, err)
	}

	if ok {
		tcpConn.CloseWrite()
	}
	// Read the response
	var buffer bytes.Buffer

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
			return nil, fmt.Errorf("error reading response from %s: %v", ip, err)
		}
		data := chunk[:n]
		utility.LogMessage("reading in chunks : " + string(data))
		buffer.Write(chunk[:n])
	}

	utility.LogMessage("Received response for file write from : " + ip)
	// utility.LogMessage("Buffer value : " + buffer.String())

	// Parse the JSON response
	var response ResponseJson
	err = json.Unmarshal(buffer.Bytes(), &response)
	if err != nil {
		return nil, fmt.Errorf("error unmarshaling response from %s: %v", ip, err)
	}

	// Log the response details
	utility.LogMessage(fmt.Sprintf("Response from %s: IP=%s, Timestamp=%v, Error=%s",
		ip, response.IP, response.TimeStamp, response.Err))

	if len(response.Data) > 0 {
		utility.LogMessage(fmt.Sprintf("Received data length: %d bytes", len(response.Data)))
	}

	return &response, nil
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
