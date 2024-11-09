package ring

import (
	context "context"
	"fmt"
	"hydfs/membership"
	"hydfs/utility"
	"io"
	"os"
	"path"
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
		file_hash := utility.HydfsFileStore[filename].Hash
		timestamp_file := utility.HydfsFileStore[filename].Timestamp

		// Send file content and metadata to the client
		response := &FileResponse{
			Filename:  filename,
			Content:   content,
			Hash:      file_hash,
			Timestamp: timestamppb.New(timestamp_file),
		}
		if err := stream.Send(response); err != nil {
			utility.LogMessage("fserver(grpc) in ring: Failed to Send file " + filename + " becuase of the the err => " + err.Error())
			continue
		}
	}
	return nil
}

func (s *FileServer) handleGetFileNames(req *FileRequest, stream FileService_GetFilesServer) error {
	output := getFileList(req.Ranges[0], req.Ranges[1])

	// Send file content and metadata to the client
	response := &FileResponse{
		Filenames: output,
	}

	if err := stream.Send(response); err != nil {
		utility.LogMessage("fserver(grpc) in ring: Failed to Send filelist becuase of the the err => " + err.Error())
	}

	return nil
}

func callFileServerFiles(server string, files []string) {
	serverIP := utility.GetIPAddr(server)
	conn, err := grpc.Dial(serverIP.String()+":"+port, grpc.WithInsecure())
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
		utility.HydfsFileStore[resp.Filename] = utility.FileMetaData{
			Hash:      resp.Hash,
			Timestamp: resp.Timestamp.AsTime(),
			RingId:    utility.Hashmurmur(resp.Filename),
		}
	}

}

func callFileServerNames(server string, low uint32, high uint32) []string {
	serverIP := utility.GetIPAddr(server)
	conn, err := grpc.Dial(serverIP.String()+":"+port, grpc.WithInsecure())
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
		return
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

	return resp.Filenames
}

func pullFiles(low uint32, high uint32, server string) {
	if server == membership.My_hostname {
		utility.LogMessage("No pulls from self please")
		return
	}
	// IF we have files within this range already -----> add a data struct for this if not there.
	//Don't do anything
	//Else
	// Pull from the correct replicas (for now, 1 call, later to all replicas) -----> pull from server if the var was passed, else usual hashcheck
	self_list := getFileList(low, high)

	// Get file list for the ranges in the other servers.
	remote_list := callFileServerNames(server, low, high)
	if len(remote_list) == 0 {
		utility.LogMessage("No files received in the range")
		return
	}
	utility.LogMessage("files to pull")

	map_self_list := make(map[string]struct{}, len(self_list))
	diff := []string{}
	for _, file := range self_list {
		map_self_list[file] = struct{}{}
	}
	for _, v := range remote_list {
		if _, found := map_self_list[v]; !found {
			diff = append(diff, v)
			utility.LogMessage("file to get final! => " + v)
		}
	}

	callFileServerFiles(server, diff)

}
