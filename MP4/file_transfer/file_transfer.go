package file_transfer

import (
	"bytes"
	context "context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"rainstorm/cache"
	"rainstorm/ring"
	"rainstorm/utility"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	grpc "google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	port      = "6060"
	mergeport = "6061"
	timeout   = 10 * time.Millisecond
)

type ClientData struct {
	Operation     string    `json:"operation"`
	Filename      string    `json:"filename,omitempty"`
	LocalFilePath string    `json:"local_path,omitempty"`
	NodeAddr      string    `json:"node_addr,omitempty"`
	Data          []byte    `json:"data,omitempty"`
	RingID        uint32    `json:"ringId,omitempty"`
	TimeStamp     time.Time `json:"timestamp,omitempty"`
	MultiAppend   bool      `json:"multiappend,omitempty"`
}

type ResponseJson struct {
	IP        string    `json:"ip"`
	Data      []byte    `json:"data"`
	TimeStamp time.Time `json:"timestamp,omitempty"`
	Hash      string    `json:"hash,omitempty"`
	RingId    uint32    `json:"ring_id,omitempty"`
	HasAppend bool      `json:"has_append,omitempty"`
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

type MergeFilesServer struct {
	UnimplementedMergeServiceServer
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

	// Start file rpc server for ring
	merge_listener, listener_error := net.Listen("tcp", ":"+mergeport)
	if listener_error != nil {
		utility.LogMessage("Failed to listen on port - " + mergeport + " : " + listener_error.Error())
	}

	server := grpc.NewServer()
	RegisterMergeServiceServer(server, &MergeFilesServer{})
	go func() {
		utility.LogMessage("RPC merge server goroutine entered")
		if err := server.Serve(merge_listener); err != nil {
			utility.LogMessage("Init Merge replication server error : " + err.Error())
		}
	}()

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
	serverAddr := conn.LocalAddr().String()
	clientAddr := conn.RemoteAddr().String()
	clientIp, _, err := net.SplitHostPort(clientAddr)
	if err != nil {
		// If there's an error (e.g., if the address doesn't have a port),
		// just return the original string
		utility.LogMessage("Error in getting ip from conn addr " + err.Error())
	}
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
				// check if file has appends
				// if yes, then concatenate and send the file , but dont change the original file entry
				metadata, exists := utility.GetHyDFSMetadata(parsedData.Filename)
				if exists && metadata.Appends > 0 {
					appendEntries := utility.GetEntries(parsedData.Filename)
					sort.Slice(appendEntries, func(i, j int) bool {
						return appendEntries[i].Timestamp.Before(appendEntries[j].Timestamp)
					})

					for _, entry := range appendEntries {
						sourceFile, err := os.Open(entry.FilePath)
						if err != nil {
							// resp.Err = "Failed to open append file " + entry.FilePath + " : " + err.Error()
							utility.LogMessage("Failed to open append file " + entry.FilePath + " : " + err.Error())
							continue // Skip this file and move to the next one
						}
						defer sourceFile.Close()
						// add data to fileData parameter

						appendData, err := io.ReadAll(sourceFile)
						if err != nil {
							// resp.Err = "Failed to read append file " + entry.FilePath + " : " + err.Error()
							utility.LogMessage("Failed to read append file " + entry.FilePath + " : " + err.Error())
							continue // Skip this file and move to the next one
						}

						// Append the data to fileData
						fileData = append(fileData, appendData...)
						utility.LogMessage("Get operation : Appended content from " + entry.FilePath)
					}

					hash := md5.Sum(fileData)
					// Convert the hash to a hexadecimal string
					md5String := hex.EncodeToString(hash[:])
					// Now md5String contains the MD5 hash of fileData
					utility.LogMessage("MD5 hash of the appended file: " + md5String)

					resp.Hash = md5String
					resp.HasAppend = true
					resp.Data = fileData
					resp.TimeStamp = metadata.Timestamp
				} else {
					// else if file has no appends, just send the original fileData
					resp.Data = fileData
					resp.TimeStamp = metadata.Timestamp
					resp.Hash = metadata.Hash
				}
				utility.LogMessage("File " + hydfsPath + " read successfully")
			}
		}

		//sending file data back to the client

		utility.LogMessage("File " + hydfsPath + " sent successfully")
	case "cache":
		hydfsPath := utility.HYDFS_DIR + "/" + parsedData.Filename
		value, exists := utility.GetHyDFSMetadata(parsedData.Filename)
		if exists {
			utility.LogMessage("Returning File metadata for " + hydfsPath + " in hydfs file store")
			resp.Hash = value.Hash
			resp.TimeStamp = value.Timestamp
			resp.RingId = value.RingId
			if value.Appends > 0 {
				resp.HasAppend = true
			}
		} else {
			utility.LogMessage("File metadata for " + hydfsPath + "not found in hydfs file store")
			// for filename, v := utility.GetAllHyDFSMetadata() {
			//fmt.Printf("Filename: %s, Ring ID: %d, md5 hash: %s, timestamp: %s", filename, v.RingId, v.Hash, v.Timestamp)
			//}
			resp.Err = "File does not exist on this replica's hydfs file store"
		}

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
			utility.LogMessage("MD5 hash of file " + hydfsPath + " : " + filehash)
			// utility.GetHyDFSMetadata(parsedData.Filename)
			fileMetaData := utility.FileMetaData{
				Hash:      filehash,
				Timestamp: parsedData.TimeStamp,
				RingId:    parsedData.RingID,
				Appends:   0,
			}
			utility.SetHyDFSMetadata(parsedData.Filename, fileMetaData)
			resp.Data = []byte("File created successfully: " + hydfsPath)
			utility.LogMessage(string(resp.Data))
		}
		// won't be sending create req for the same file
	case "append":
		hydfsPath := utility.HYDFS_DIR + "/" + parsedData.Filename
		if !utility.FileExists(hydfsPath) {
			resp.Err = "File does not exist on this replica"
			utility.LogMessage(resp.Err)
		} else {
			metadata, exists := utility.GetHyDFSMetadata(parsedData.Filename)
			if !exists {
				resp.Err = "File exists on local FS but not a part of HydfsFileStore"
				utility.LogMessage(resp.Err)
			}
			if parsedData.MultiAppend {
				fmt.Printf("Starting multiappend op from VM : %s, appending file : %s to filename : %s\n", clientAddr, parsedData.LocalFilePath, parsedData.Filename)
			}
			metadata.Appends += 1
			// update append file path
			appendPath := utility.HYDFS_APPEND + "/" + parsedData.Filename + "_" + strconv.Itoa(metadata.Appends)

			//append file write
			err := os.WriteFile(appendPath, parsedData.Data, 0644)
			if err != nil {
				resp.Err = "Error writing append file: " + err.Error()
				utility.LogMessage(resp.Err)
			} else {
				utility.LogMessage("File write for append success : " + appendPath)
				utility.AddAppendsEntry(parsedData.Filename, appendPath, parsedData.TimeStamp, clientAddr)
				utility.SetHyDFSMetadata(parsedData.Filename, metadata)
				// utility.HydfsFileStore[parsedData.Filename] = metadata
				//appends_list := utility.GetEntries(parsedData.Filename)
				utility.LogMessage("appends List for file : " + parsedData.Filename)
				if parsedData.MultiAppend {
					fmt.Printf("Completed multiappend op from VM : %s, append stored at: %s\n", clientAddr, appendPath)
				}
				//for _, entry := range appends_list {
				//	utility.LogMessage("FilePath: " + entry.FilePath + "Timestamp: " + entry.Timestamp.String() + "Client IP: " + entry.IP)
				//}
			}
		}
		// handleAppend(filename, localPath)

	case "merge":
		hydfsPath := utility.HYDFS_DIR + "/" + parsedData.Filename
		// fmt.Printf("Merging all replicas of %s in HyDFS\n", hydfsPath)

		// Check if file exists
		if !utility.FileExists(hydfsPath) {
			resp.Err = "File does not exist on this replica"
			utility.LogMessage(resp.Err)
		} else {
			metadata, exists := utility.GetHyDFSMetadata(parsedData.Filename)
			if !exists {
				resp.Err = "File exists on local FS but not a part of HydfsFileStore"
				utility.LogMessage(resp.Err)
			}
			// Check if file has appends
			if metadata.Appends > 0 {
				// If yes, concatenate
				appendEntries := utility.GetEntries(parsedData.Filename)
				// Sort entries according to time stamp, ensuring that we have same order of appends for the same client
				sort.Slice(appendEntries, func(i, j int) bool {
					return appendEntries[i].Timestamp.Before(appendEntries[j].Timestamp)
				})

				file, err := os.OpenFile(hydfsPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
				if err != nil {
					resp.Err = "Failed to open file for merge: " + err.Error()
					utility.LogMessage(resp.Err)
				}
				defer file.Close()

				for _, entry := range appendEntries {
					sourceFile, err := os.Open(entry.FilePath)
					if err != nil {
						resp.Err = "Failed to open append file " + entry.FilePath + " : " + err.Error()
						utility.LogMessage(resp.Err)
					}
					defer sourceFile.Close()

					_, err = io.Copy(file, sourceFile)
					if err != nil {
						resp.Err = "Failed to append file content " + entry.FilePath + " : " + err.Error()
						utility.LogMessage(resp.Err)
					}

					utility.LogMessage("Appended content from " + entry.FilePath + " to " + hydfsPath)

					// remove file from directory TODO
					err = os.Remove(entry.FilePath)
					if err != nil {
						errMsg := fmt.Sprintf("Failed to remove file %s: %v", entry.FilePath, err)
						utility.LogMessage(errMsg)
					} else {
						utility.LogMessage("Removed file: " + entry.FilePath)
					}
				}

				// once done concatenating, update md5 hash and timestamp
				newMD5Hash, err := utility.GetMD5(hydfsPath)
				if err != nil {
					utility.LogMessage("Error in generating MD5 has for the merged file")
				}

				metadata, _ := utility.GetHyDFSMetadata(parsedData.Filename)
				metadata.Hash = newMD5Hash
				metadata.Timestamp = time.Now()
				metadata.Appends = 0

				utility.SetHyDFSMetadata(parsedData.Filename, metadata)
				utility.LogMessage("Updated Hash and Timestamp of the merged file")
				// send signal to other replicas to pick file from this node / send a merge write request to override the file and meta data
				var wg sync.WaitGroup
				_, senderIPs, _ := GetSuccesorIPsForFilename(parsedData.Filename)
				utility.LogMessage("Sending merge request to replicas for file ID ")
				data, err := os.ReadFile(hydfsPath)
				if err != nil {
					errMsg := fmt.Sprintf("error reading merged and concatenated file:  %s: %v", hydfsPath, err)
					utility.LogMessage(errMsg)
				}

				for i := 0; i < len(senderIPs); i++ {
					if senderIPs[i] != clientIp {
						wg.Add(1)
						go func(ip_addr string) {
							defer wg.Done()
							SendMergedFile(ip_addr, parsedData.Filename, data, newMD5Hash, metadata.Timestamp)
						}(senderIPs[i])
					}
				}

				wg.Wait()
				// send response to client on success after your completion / after all nodes complete
				resp.Data = []byte("All entries appended successfully to " + hydfsPath)
				utility.LogMessage(string(resp.Data))
			} else {
				// no need to do any merge
				// return
				resp.Data = []byte("No appends found, no need to merge: " + hydfsPath)
				utility.LogMessage(string(resp.Data))
			}
		}

	case "delete":
		hydfsPath := utility.HYDFS_DIR + "/" + parsedData.Filename
		fmt.Printf("Deleting %s from HyDFS\n", hydfsPath)

		// handleDelete(filename)

	case "multiappend":
		// forward requests to each successor ip for append operation
		reqData := parsedData
		filename := parsedData.Filename

		reqData.MultiAppend = true
		reqData.Operation = "append"
		SendAppends(reqData, filename)
		resp.Data = []byte("Append request from VM " + clientIp + " sent")
		utility.LogMessage(string(resp.Data))

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

func HyDFSClient(request ClientData, options ...[]string) {

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
		}

		// add a check for checking if the file is in cache
		entry, exists := cache.GetCacheEntry(filename)
		fileID, senderIPs, _ := GetSuccesorIPsForFilename(filename)
		request.RingID = fileID
		var vm_ip string

		// if found in cache, do pre-flight request asking for data from replicas
		// check if replica time stamps are same or older than the one in cache
		// if cache has most recent entry , then update cache counter

		// for now, test without cache

		if exists {
			utility.LogMessage(filename + " found in cache, entry : " + entry.Filename + ", " + entry.Hash + ", " + entry.Timestamp.String())
			responses := GetFilenameReplicasMetadata(filename, senderIPs)
			vm_ip = ParseCacheResponses(responses, entry.Timestamp, entry.Hash)
			if vm_ip == "" {
				utility.LogMessage("Cache entry TS same as most recent update, using cache to serve request")
				// if cache has most recent entry , then update cache counter / put it at start of the q
				// then, use the contents of the cache file and store it a the location provided
				cache_filename := utility.HYDFS_CACHE + "/" + filename
				utility.LogMessage("Copying " + cache_filename + " to " + localPath)

				err := utility.CopyFile(cache_filename, localPath)
				if err != nil {
					utility.LogMessage("Error copying file from cache: " + err.Error())
				}
				cache.IncrementCacheEntryCounter(filename)
			} else {
				// if not , then use the ip returned by cache to request for file data
				// can be handled by the else case below
				utility.LogMessage("Cache entry stale, requesting vms to send latest version")
			}
		}
		if !exists || vm_ip != "" {
			// if not found in cache, get the file by quorum and update cache
			utility.LogMessage("No cache entry found for filename : " + filename + " continuing with get method call")
			// filename = filename of file in hydfs

			utility.LogMessage("Successor IPs for file - " + senderIPs[0] + ", " + senderIPs[1] + ", " + senderIPs[2])

			// remove lines when ring ID integrated
			// clear(senderIPs)
			// senderIPs = []string{"172.22.94.195"}

			responses := GetFileFromReplicas(senderIPs, request)
			latestResponse := ParseGetRespFromReplicas(responses, senderIPs[0], localPath)
			// Add response to cache
			utility.LogMessage("Filename : " + filename + " to be stored in cache , timestamp saved as : " + latestResponse.TimeStamp.String())
			cache.AddOrUpdateCacheEntry(filename, latestResponse.Hash, latestResponse.RingId, latestResponse.TimeStamp, localPath)

		}

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
		fileID, senderIPs, senderIDs := GetSuccesorIPsForFilename(filename)
		request.RingID = fileID
		request.TimeStamp = time.Now()
		utility.LogMessage("Successor IPs assigned - " + senderIPs[0] + ", " + senderIPs[1] + ", " + senderIPs[2])

		// remove lines when ring ID integrated
		// clear(senderIPs)
		// senderIPs = []string{"172.22.94.195"}

		for i := 0; i < len(senderIPs); i++ {
			wg.Add(1)
			go func(ip_addr string, sender_id string) {
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
				utility.LogMessage(fmt.Sprintf("File created successfully: %s on %s", string(response.Data), response.IP))
				fmt.Printf("Created file %s (ID - %d) in HyDFS Node (ID - %s) \n", filename, fileID, sender_id)
			}(senderIPs[i], senderIDs[i])
		}

		// Wait for the goroutine to finish
		wg.Wait()
		utility.LogMessage("File create operation completed")

		// handleCreate(filename, localPath)
		// we won't be sending create req for the same file
	case "append":
		SendAppends(request, filename)
		// handleAppend(filename, localPath)

	case "merge":
		// handleMerge(filename)
		fileID, senderIPs, _ := GetSuccesorIPsForFilename(filename)
		request.RingID = fileID
		request.TimeStamp = time.Now()
		fmt.Printf("Merging file %s (ID = %d) , request sent to HyDFS node %s \n", filename, fileID, senderIPs[0])

		wg.Add(1)
		go func() {
			defer wg.Done()
			response, err := SendRequest(senderIPs[0], request)
			if err != nil {
				utility.LogMessage(fmt.Sprintf("Error in merge: %v", err))
				return
			}
			if response.Err != "" {
				utility.LogMessage(fmt.Sprintf("Error from server: %s", response.Err))
				return
			}
		}()

		// Wait for the goroutine to finish
		wg.Wait()
		utility.LogMessage("File merge operation completed")
		fmt.Printf("File merge operation completed\n")

	case "multiappend":
		fileID, _, _ := GetSuccesorIPsForFilename(filename)
		request.RingID = fileID
		var vmList, localFileList []string
		if len(options) > 0 {
			vmList = options[0]
			if len(options) > 1 {
				localFileList = options[1]
			}
		}

		fmt.Printf("VM IP len : %d\n", len(vmList))
		fmt.Printf("File list len : %d\n", len(localFileList))

		// no wait groups needed
		for i := 0; i < len(vmList); i++ {
			go func(ip_addr string, localfilepath string, mrequest ClientData) {
				// fmt.Printf("signal being sent to VM %s for appending %s ", ip_addr, localfilepath)
				mrequest.LocalFilePath = localfilepath
				response, err := SendRequest(ip_addr, mrequest)
				if err != nil {
					utility.LogMessage(fmt.Sprintf("Error in multiappend: %v", err))
					return
				}
				if response.Err != "" {
					utility.LogMessage(fmt.Sprintf("Error from server: %s", response.Err))
					return
				}
				fmt.Printf("signal sent to VM %s for appending %s \n", ip_addr, localfilepath)
			}(vmList[i], localFileList[i], request)
		}
		// :multiappend; 172.22.158.195, 172.22.94.195 ; /home/hydfs/1.txt, /home/hydfs/2.txt,

	case "delete":
		fmt.Printf("Deleting %s from HyDFS\n", filename)

		// handleDelete(filename)

	case "ls":
		fileID, _, _ := GetSuccesorIPsForFilename(filename)
		fmt.Printf("Listing VM addresses storing %s (ring id - %d)\n", filename, fileID)
		request.RingID = fileID
		VMs := ring.GetFileNodes(filename)
		for i := 0; i < len(VMs); i++ {
			fmt.Printf("%d. %s , ring id - %d\n", i, VMs[i], utility.Hashmurmur(VMs[i]))
		}

	default:
		utility.LogMessage("Unknown command: " + cmd)
	}
}

func GetSuccesorIPsForFilename(filename string) (uint32, []string, []string) {
	ringID := utility.Hashmurmur(filename)
	servers_list := ring.GetFileNodes(filename)

	var ips []string
	for _, server := range servers_list {
		ips = append(ips, utility.GetIPAddr(server).String())
	}
	utility.LogMessage("Ring ID generated for filename " + filename + ": " + strconv.FormatInt(int64(ringID), 10))
	return ringID, ips, servers_list
}

func SendRequest(ip string, request ClientData) (*ResponseJson, error) {
	// Establish TCP connection
	ip = strings.Trim(ip, " ")

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
		// utility.LogMessage("reading in chunks : " + string(chunk[:n]))
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
		// data := chunk[:n]
		// utility.LogMessage("reading in chunks : " + string(data))
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

func GetFileFromReplicas(senderIPs []string, request ClientData) []ResponseJson {
	var wg sync.WaitGroup
	responseChannel := make(chan ResponseJson, len(senderIPs))

	for _, ip := range senderIPs {
		wg.Add(1)
		go func(ip string) {
			defer wg.Done()
			response, err := SendRequest(ip, request)
			if err != nil {
				utility.LogMessage(fmt.Sprintf("Error in request to %s: %v", ip, err))
				responseChannel <- ResponseJson{IP: ip, Err: err.Error()}
				return
			}

			if response.Err != "" {
				utility.LogMessage(fmt.Sprintf("Error from server %s: %s", ip, response.Err))
			}

			responseChannel <- *response
		}(ip)
	}

	// Close the channel when all goroutines are done
	go func() {
		wg.Wait()
		close(responseChannel)
	}()

	// Collect all responses
	var responses []ResponseJson
	for response := range responseChannel {
		responses = append(responses, response)
	}

	return responses
}

// Quorum of 2 nodes should be met to print out a file / save it
// else , just use primary node result
func ParseGetRespFromReplicas(responses []ResponseJson, primaryNodeIP string, localPath string) ResponseJson {
	var validResponses []ResponseJson
	var primaryResponse ResponseJson
	var matchCount int
	var latestTimestamp time.Time
	var latestResponse ResponseJson

	utility.LogMessage("IN CACHE Latest timestamp before response parsing : " + latestTimestamp.String())

	for _, response := range responses {
		// Skip responses with errors
		if response.Err != "" {
			utility.LogMessage(fmt.Sprintf("Skipping response from %s due to error: %s", response.IP, response.Err))
			continue
		}

		validResponses = append(validResponses, response)

		// Store the primary node response
		if response.IP == primaryNodeIP {
			primaryResponse = response
		}

		// Keep track of the latest timestamp
		if response.TimeStamp.After(latestTimestamp) {
			utility.LogMessage("IN CACHE Latest timestamp : " + latestTimestamp.String() + " , response timestamp : " + response.TimeStamp.String())
			latestTimestamp = response.TimeStamp
			latestResponse = response
		}
	}

	utility.LogMessage("IN CACHE Latest timestamp after response parsing : " + latestTimestamp.String())
	// can rework quorum for appends. need to check hash and timestamp both
	// Check for quorum (at least 2 matching responses)
	for i := 0; i < len(validResponses); i++ {
		matchCount = 1
		for j := i + 1; j < len(validResponses); j++ {
			if validResponses[i].TimeStamp == validResponses[j].TimeStamp &&
				validResponses[i].Hash == validResponses[j].Hash {
				matchCount++
				if matchCount >= 2 {
					// Quorum met, write this data to file
					writeResponseToFile(validResponses[i].Data, localPath)
					utility.LogMessage(fmt.Sprintf("Quorum met. Writing data from %s to %s", validResponses[i].IP, localPath))
					return validResponses[i]
				}
			}
		}
	}

	// If no quorum, use the primary node response
	if primaryResponse.Data != nil {
		writeResponseToFile(primaryResponse.Data, localPath)
		utility.LogMessage(fmt.Sprintf("No quorum met. Using primary node (%s) data for %s", primaryNodeIP, localPath))
		return primaryResponse
	} else if latestResponse.Data != nil {
		// If primary node doesn't have data, use the latest response
		writeResponseToFile(latestResponse.Data, localPath)
		utility.LogMessage(fmt.Sprintf("No quorum met and no primary data. Using latest data from %s for %s", latestResponse.IP, localPath))
		return latestResponse
	} else {
		utility.LogMessage("No valid data found to write to file")
	}
	return primaryResponse
}

func writeResponseToFile(data []byte, filePath string) error {
	return os.WriteFile(filePath, data, 0644)
}

// Cache - pre get check to see whether cache should be used or not, method # 1
func GetFilenameReplicasMetadata(filename string, senderIPs []string) []ResponseJson {
	var wg sync.WaitGroup
	responseChannel := make(chan ResponseJson, len(senderIPs))

	for _, ip := range senderIPs {
		wg.Add(1)
		go func(ip string) {
			defer wg.Done()

			requestData := ClientData{
				Operation: "cache",
				Filename:  filename,
			}

			response, err := SendRequest(ip, requestData)
			if err != nil {
				utility.LogMessage(fmt.Sprintf("Error in request to %s: %v", ip, err))
				responseChannel <- ResponseJson{IP: ip, Err: err.Error()}
				return
			}

			if response.Err != "" {
				utility.LogMessage(fmt.Sprintf("Error from server %s: %s", ip, response.Err))
			}

			responseChannel <- *response
		}(ip)
	}

	// Close the channel when all goroutines are done
	go func() {
		wg.Wait()
		close(responseChannel)
	}()

	// Collect all responses
	var responses []ResponseJson
	for response := range responseChannel {
		responses = append(responses, response)
	}

	return responses
}

// Read cache responses, returns IP to query
func ParseCacheResponses(responses []ResponseJson, cachedFileTS time.Time, cachedFileHash string) string {
	for _, response := range responses {
		// Skip responses with errors
		if response.Err != "" {
			utility.LogMessage(fmt.Sprintf("Skipping response from %s due to error: %s", response.IP, response.Err))
			continue
		}

		// Compare response timestamp with cached file timestamp
		if response.TimeStamp.After(cachedFileTS) {
			utility.LogMessage(fmt.Sprintf("Found newer version on %s. Local: %v, Remote: %v",
				response.IP, cachedFileTS, response.TimeStamp))
			return response.IP
		}

		// If timestamps are equal, compare hashes
		if response.TimeStamp.Equal(cachedFileTS) && response.Hash != cachedFileHash {
			utility.LogMessage(fmt.Sprintf("Found different version with same timestamp on %s. Local hash: %s, Remote hash: %s",
				response.IP, cachedFileHash, response.Hash))
			return response.IP
		}

		// pre flight cache check resp tells us that the file name has some appends, in this case always refresh cache
		if response.HasAppend {
			utility.LogMessage("HYDFS node " + response.IP + " has appends for the file which we are checking")
			utility.LogMessage("Because of this, we will be sending request to all VMs and refreshing cache entry")
			return response.IP
		}
	}

	utility.LogMessage("No newer versions found in cache responses")
	return ""
}

func (s *MergeFilesServer) MergeFiles(req *MergeRequest, stream MergeService_MergeFilesServer) error {
	utility.LogMessage("RPC server entered Invoked for filename : " + req.Filename)
	hydfsPath := utility.HYDFS_DIR + "/" + req.Filename

	// Open the file with O_WRONLY (write-only), O_CREATE (create if not exist), and O_TRUNC (truncate if exist)
	file, err := os.OpenFile(hydfsPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		errMsg := fmt.Sprintf("Failed to open file %s: %v", hydfsPath, err)
		utility.LogMessage(errMsg)
		return fmt.Errorf("failed to open file %s: %v", hydfsPath, err)
	}
	defer file.Close()

	// Write the data from the request to the file
	_, err = io.Copy(file, bytes.NewReader(req.Data))
	if err != nil {
		errMsg := fmt.Sprintf("Failed to write data to file %s: %v", hydfsPath, err)
		utility.LogMessage(errMsg)
		return fmt.Errorf("failed to write data to file %s: %v", hydfsPath, err)
	}

	utility.LogMessage("File " + req.Filename + " has been successfully overwritten")

	// Send file content and metadata to the client
	err = stream.Send(&MergeResponse{Ack: "File " + req.Filename + " has been successfully merged"})
	if err != nil {
		errMsg := fmt.Sprintf("Failed to send response: %v", err)
		utility.LogMessage(errMsg)
		return fmt.Errorf("failed to send response: %v", err)
	}

	// change the hydfs file store entry
	FileMetaData, _ := utility.GetHyDFSMetadata(req.Filename)
	FileMetaData.Hash = req.Hash
	FileMetaData.Timestamp = req.Timestamp.AsTime()
	FileMetaData.Appends = 0
	utility.SetHyDFSMetadata(req.Filename, FileMetaData)

	// change the append file store entry
	utility.DeleteEntries(req.Filename)

	// clear the append directory with those filenames appends
	append_path := utility.HYDFS_APPEND + "/" + req.Filename
	utility.LogMessage("Clearing all files in append directory with " + append_path + " as prefix")
	utility.ClearAppendFiles(utility.HYDFS_APPEND+"/", req.Filename)
	return nil
}

func SendMergedFile(serverIP string, filename string, data []byte, hash string, newTimeStamp time.Time) {
	conn, err := grpc.Dial(serverIP+":"+mergeport, grpc.WithInsecure())
	if err != nil {
		utility.LogMessage("Unable to connect to server - merge rpc server - " + err.Error())
	}
	defer conn.Close()
	utility.LogMessage("created connection with merge server ip : " + serverIP)

	client := NewMergeServiceClient(conn)

	fileRequest := &MergeRequest{
		Filename:  filename,
		Data:      data,
		Hash:      hash,
		Timestamp: timestamppb.New(newTimeStamp),
	}
	// utility.LogMessage("here - 1")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	// utility.LogMessage("here - 2")
	stream, err := client.MergeFiles(ctx, fileRequest)
	if err != nil {
		utility.LogMessage("error sending file - " + err.Error())
		return
		// log.Fatalf("Error calling GetFiles: %v", err)
	}

	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			utility.LogMessage("EOF received " + err.Error())
			break
		}
		if err != nil {
			utility.LogMessage("Error receiving stream -  " + err.Error())
		}

		utility.LogMessage("File received at server - " + resp.Ack)
	}

}

func SendAppends(request ClientData, filename string) {
	var wg sync.WaitGroup
	localPath := request.LocalFilePath
	if request.MultiAppend {
		utility.LogMessage("entered append for multiappend - for -> " + filename + " localfilepath -> " + localPath)
	}
	if !utility.FileExists(localPath) {
		fmt.Println("File does not exist : " + localPath)
		return
	}
	utility.LogMessage("here - 1")
	fileData, err := os.ReadFile(localPath)
	if err != nil {
		utility.LogMessage("Error reading local file: " + err.Error())
		return
	}
	utility.LogMessage("here - 2")
	request.Data = fileData
	fileID, senderIPs, _ := GetSuccesorIPsForFilename(filename)
	request.RingID = fileID
	request.TimeStamp = time.Now()
	utility.LogMessage("here - 3")
	utility.LogMessage("Successor IPs for append - " + senderIPs[0] + ", " + senderIPs[1] + ", " + senderIPs[2])

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
			utility.LogMessage(fmt.Sprintf("File appended successfully: %s on %s", string(response.Data), response.IP))
		}(senderIPs[i])
	}

	// Wait for the goroutine to finish
	wg.Wait()
	utility.LogMessage("File append operation completed")

	// handleAppend(filename, localPath)

}
