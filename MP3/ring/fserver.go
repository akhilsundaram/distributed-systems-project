package ring

import (
	context "context"
	"fmt"
	"hydfs/membership"
	"hydfs/utility"
	"io"
	"os"
	"path"
	"strconv"
	"time"

	grpc "google.golang.org/grpc"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
)

type FileServer struct {
	UnimplementedFileServiceServer
}

func (s *FileServer) GetFiles(req *FileRequest, stream FileService_GetFilesServer) error {
	utility.LogMessage("RPC server entered - " + req.Command)
	switch req.Command {
	case "getfiles":
		return s.handleGetFiles(req, stream)
	case "getfilenames":
		return s.handleGetFileNames(req, stream)
	case "getappendfiles":
		return s.handleGetAppendFiles(req, stream)
	default:
		return fmt.Errorf("not a correct req - wrong command passed to grpc ring fserver")
	}
}

func (s *FileServer) handleGetFiles(req *FileRequest, stream FileService_GetFilesServer) error {
	for _, filename := range req.Filenames {
		// Read file content
		content, err := os.ReadFile(path.Join(utility.HYDFS_DIR, filename))
		if err != nil {
			utility.LogMessage("fserver(grpc) in ring: Failed to read file " + filename + " becuase of the the err => " + err.Error())
			continue
		}

		// get hash
		//file_hash := utility.HydfsFileStore[filename].Hash
		//timestamp_file := utility.HydfsFileStore[filename].Timestamp

		FileMetaData, _ := utility.GetHyDFSMetadata(filename)
		file_hash := FileMetaData.Hash
		timestamp_file := FileMetaData.Timestamp
		append_ctr := FileMetaData.Appends

		// Send file content and metadata to the client
		response := &FileResponse{
			Filename:  filename,
			Content:   content,
			Hash:      file_hash,
			Timestamp: timestamppb.New(timestamp_file),
			Append:    int64(append_ctr),
		}
		if err := stream.Send(response); err != nil {
			utility.LogMessage("fserver(grpc) in ring: Failed to Send file " + filename + " becuase of the the err => " + err.Error())
			continue
		}
	}
	return nil
}

func (s *FileServer) handleGetFileNames(req *FileRequest, stream FileService_GetFilesServer) error {
	output_files := getFileList(req.Ranges[0], req.Ranges[1])
	utility.LogMessage("files requested at ranges - " + strconv.FormatUint(uint64(req.Ranges[0]), 10) + "," + strconv.FormatUint(uint64(req.Ranges[1]), 10))
	var output_appends []int64
	for _, file_name := range output_files {
		utility.LogMessage("file found in range - " + file_name)
		val, _ := utility.GetHyDFSMetadata(file_name)
		output_appends = append(output_appends, int64(val.Appends))
	}

	// Send file content and metadata to the client
	response := &FileResponse{
		Filenames: output_files,
		Appends:   output_appends,
	}

	if err := stream.Send(response); err != nil {
		utility.LogMessage("fserver(grpc) in ring: Failed to Send filelist becuase of the the err => " + err.Error())
	}

	return nil
}

func (s *FileServer) handleGetAppendFiles(req *FileRequest, stream FileService_GetFilesServer) error {
	append_list := utility.GetEntries(req.Filename)
	for _, appendFile := range append_list {
		content, err := os.ReadFile(appendFile.FilePath)
		if err != nil {
			utility.LogMessage("fserver(grpc) in ring: Failed to read append file at," + appendFile.FilePath + " of " + req.Filename + " becuase of the the err => " + err.Error())
			continue
		}

		// Send file content and metadata to the client
		response := &FileResponse{
			Filename:  req.Filename,
			Filepath:  appendFile.FilePath,
			Content:   content,
			Timestamp: timestamppb.New(appendFile.Timestamp),
			Ip:        appendFile.IP,
		}
		if err := stream.Send(response); err != nil {
			utility.LogMessage("fserver(grpc) in ring: Failed to read append file at," + appendFile.FilePath + " of " + req.Filename + " becuase of the the err => " + err.Error())
			continue
		}
	}
	return nil
}

func callFileServerFiles(server string, files []string) {
	serverIP := utility.GetIPAddr(server)
	var files_with_appends []string
	conn, err := grpc.Dial(serverIP.String()+":"+port, grpc.WithInsecure(), grpc.WithDefaultCallOptions(
		grpc.MaxCallRecvMsgSize(128*1024*1024), // Increase to 16 MB
		grpc.MaxCallSendMsgSize(128*1024*1024), // Increase to 16 MB
	))
	if err != nil {
		utility.LogMessage("Unable to connect to server - ring rpc fserver - " + err.Error())
	}
	defer conn.Close()
	utility.LogMessage("created conn with server: " + server)

	client := NewFileServiceClient(conn)

	// List of files to request
	fileRequest := &FileRequest{
		Command:   "getfiles",
		Filenames: files,
	}
	// utility.LogMessage("here - 1")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	// utility.LogMessage("here - 2")
	start_time := time.Now()
	var datatransferred int64 = 0
	stream, err := client.GetFiles(ctx, fileRequest)
	if err != nil {
		utility.LogMessage("error getiing file - " + err.Error())
		return
		// log.Fatalf("Error calling GetFiles: %v", err)
	}

	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			utility.LogMessage(" EOF received " + err.Error())
			break
		}
		if err != nil {
			utility.LogMessage("Error receiving stream -  " + err.Error())
		}

		utility.LogMessage("File received at client - " + resp.Filename)

		// Write the content to the destination file
		err = os.WriteFile(path.Join(utility.HYDFS_DIR, resp.Filename), resp.Content, 0644)
		if err != nil {
			utility.LogMessage("Error writing to destination file: " + err.Error())
		}
		utility.LogMessage("file written")

		//utility.HydfsFileStore[resp.Filename] = utility.FileMetaData{
		//	Hash:      resp.Hash,
		//	Timestamp: resp.Timestamp.AsTime(),
		//	RingId:    utility.Hashmurmur(resp.Filename),
		//}
		datatransferred += int64(len(resp.Content))
		NewFileMetaData := utility.FileMetaData{
			Hash:      resp.Hash,
			Timestamp: resp.Timestamp.AsTime(),
			RingId:    utility.Hashmurmur(resp.Filename),
			Appends:   int(resp.Append),
		}
		utility.SetHyDFSMetadata(resp.Filename, NewFileMetaData)
		if resp.Append > 0 {
			files_with_appends = append(files_with_appends, resp.Filename)
		}
	}
	time_taken := time.Since(start_time).Milliseconds()
	if del_check {
		utility.LogTest("Time used for getting data: " + strconv.FormatInt(time_taken, 10))
		utility.LogTest("Data received: " + strconv.FormatInt(datatransferred, 10))
		utility.LogTest("Bandwidth: " + strconv.FormatInt(datatransferred/time_taken, 10))
	}

	for _, file := range files_with_appends {
		callFileServerAppendFiles(server, file)
	}

}

func callFileServerNames(server string, low uint32, high uint32) ([]string, []int64) {
	serverIP := utility.GetIPAddr(server)
	conn, err := grpc.Dial(serverIP.String()+":"+port, grpc.WithInsecure(), grpc.WithDefaultCallOptions(
		grpc.MaxCallRecvMsgSize(128*1024*1024), // Increase to 16 MB
		grpc.MaxCallSendMsgSize(128*1024*1024), // Increase to 16 MB
	))
	if err != nil {
		utility.LogMessage("Unable to connect to server - ring rpc fserver - " + err.Error())
	}
	defer conn.Close()

	client := NewFileServiceClient(conn)

	// List of files to request
	fileRequest := &FileRequest{
		Command: "getfilenames",
		Ranges:  []uint32{low, high},
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	stream, err := client.GetFiles(ctx, fileRequest)
	if err != nil {
		// log.Fatalf("Error calling GetFiles: %v", err)
		utility.LogMessage("Error calling GetFiles: " + err.Error())
		return []string{}, []int64{}
	}

	resp, err := stream.Recv()
	if err != nil {
		utility.LogMessage("Error receiving stream -  " + err.Error())
	}
	utility.LogMessage("files requested")
	utility.LogMessage("received: ----------")
	for _, v := range resp.Filenames {
		utility.LogMessage("filename sent - " + v)
	}

	return resp.Filenames, resp.Appends
}

func callFileServerAppendFiles(server string, filename string) {
	serverIP := utility.GetIPAddr(server)
	conn, err := grpc.Dial(serverIP.String()+":"+port, grpc.WithInsecure(), grpc.WithDefaultCallOptions(
		grpc.MaxCallRecvMsgSize(128*1024*1024), // Increase to 16 MB
		grpc.MaxCallSendMsgSize(128*1024*1024), // Increase to 16 MB
	))
	if err != nil {
		utility.LogMessage("Unable to connect to server - ring rpc fserver - " + err.Error())
	}
	defer conn.Close()
	utility.LogMessage("created conn with server: " + server)

	client := NewFileServiceClient(conn)

	fileRequest := &FileRequest{
		Command:  "getappendfiles",
		Filename: filename,
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	stream, err := client.GetFiles(ctx, fileRequest)
	if err != nil {
		utility.LogMessage("error getting append file - " + err.Error())
		return
	}

	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			utility.LogMessage(" EOF received, stopping file recv " + err.Error())
			break
		}
		if err != nil {
			utility.LogMessage("Error receiving stream -  " + err.Error())
		}

		utility.LogMessage("Append file received and to be written at client - " + resp.Filepath)

		// Write the content to the destination file
		err = os.WriteFile(resp.Filepath, resp.Content, 0644)
		if err != nil {
			utility.LogMessage("Error writing to destination file: " + err.Error())
		}
		utility.LogMessage("append file written")
		utility.AddAppendsEntry(resp.Filename, resp.Filepath, resp.Timestamp.AsTime(), resp.Ip)
	}

}

func pullFiles(low uint32, high uint32, server string) bool {
	if server == membership.My_hostname {
		utility.LogMessage("No pulls from self please")
		return false
	}
	// IF we have files within this range already -----> add a data struct for this if not there.
	//Don't do anything
	//Else
	// Pull from the correct replicas (for now, 1 call, later to all replicas) -----> pull from server if the var was passed, else usual hashcheck
	self_list := getFileList(low, high)

	// Get file list for the ranges in the other servers.
	remote_list, remote_appends := callFileServerNames(server, low, high)
	if len(remote_list) == 0 {
		utility.LogMessage("No files received in the range")
		return false
	}
	utility.LogMessage("files to pull")

	map_self_list := make(map[string]struct{}, len(self_list))
	diff := []string{}
	diff_appends := []string{}
	for _, file := range self_list {
		map_self_list[file] = struct{}{}
	}
	for i, v := range remote_list {
		if _, found := map_self_list[v]; !found {
			diff = append(diff, v)
			utility.LogMessage("file to get final! => " + v)
		} else {
			file_info, _ := utility.GetHyDFSMetadata(v)
			append_num := file_info.Appends

			if int64(append_num) < remote_appends[i] {
				diff_appends = append(diff_appends, v)
			}
		}
	}

	if len(diff) > 0 {
		callFileServerFiles(server, diff)
	}

	for _, file_name := range diff_appends {
		utility.LogMessage("inconsistent appends - pulling from  - " + file_name)
		callFileServerAppendFiles(server, file_name)
	}

	//Testing only
	if len(diff) > 0 || len(diff_appends) > 0 {
		return true
	}
	return false

}
