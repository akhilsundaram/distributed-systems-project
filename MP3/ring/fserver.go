package ring

import (
	context "context"
	"fmt"
	"hydfs/utility"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	grpc "google.golang.org/grpc"
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
		content, err := os.ReadFile(filepath.Join(utility.HYDFS_DIR, filename))
		if err != nil {
			utility.LogMessage("fserver(grpc) in ring: Failed to read file " + filename + " becuase of the the err => " + err.Error())
			continue
		}

		// get hash
		file_hash := utility.HydfsFileStore[filename].Hash

		// Send file content and metadata to the client
		response := &FileResponse{
			Filename: filename,
			Content:  content,
			Hash:     file_hash,
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

	client := NewFileServiceClient(conn)

	// List of files to request
	fileRequest := &FileRequest{
		Command:   "getfiles",
		Filenames: files,
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	stream, err := client.GetFiles(ctx, fileRequest)
	if err != nil {
		log.Fatalf("Error calling GetFiles: %v", err)
	}

	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			utility.LogMessage("Error receiving stream -  " + err.Error())
		}

		utility.LogMessage("File received at server - " + resp.Filename)

		// Write the content to the destination file
		err = os.WriteFile(filepath.Join(utility.HYDFS_DIR, resp.Filename), resp.Content, 0644)
		if err != nil {
			utility.LogMessage("Error writing to destination file: " + err.Error())
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
		log.Fatalf("Error calling GetFiles: %v", err)
	}

	resp, err := stream.Recv()
	if err != nil {
		utility.LogMessage("Error receiving stream -  " + err.Error())
	}

	return resp.Filenames
}
