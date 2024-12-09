package utility

import (
	"bufio"
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"rainstorm/stormgrpc"
	"strings"
	"sync"
	"time"

	"github.com/spaolacci/murmur3"
	"google.golang.org/grpc"
)

// -----------------------------------VARS----------------------------------------------------//
var (
	logFile *os.File
	logger  *log.Logger
	once    sync.Once
	mu      sync.Mutex
)

type FileMetaData struct {
	Hash      string
	Timestamp time.Time
	RingId    uint32
	Appends   int
}

type FileAppend struct {
	FilePath  string
	Timestamp time.Time
	IP        string
}

var (
	LOGGER_FILE  = "/home/log/rainstorm.log"
	HYDFS_DIR    = "/home/hydfs/files"
	HYDFS_CACHE  = "/home/hydfs/cache"
	HYDFS_TMP    = "/home/hydfs/tmp"
	HYDFS_APPEND = "/home/hydfs/append"
)

/* LOGGER STUFF */
func initLogger() {
	once.Do(func() {
		var err error
		logFile, err = os.OpenFile(LOGGER_FILE, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			log.Fatal(err)
		}
		logger = log.New(logFile, "", log.LstdFlags)
	})
}

func init() {
	AppendsFileStore = &FileAppendsTracker{
		files: make(map[string][]FileAppend),
	}
}

func LogMessage(message string) {
	initLogger()
	mu.Lock()
	defer mu.Unlock()
	logger.Println(message)
}

/* IP ADDRESS  && NET */

func GetIPAddr(host string) net.IP {
	ips, err := net.LookupIP(host) // Can give us a string of IPs.

	if err != nil {
		LogMessage("Error on IP lookup for : " + host)
	}

	for _, ip := range ips { //iterate through and get first IP.
		if ipv4 := ip.To4(); ipv4 != nil {
			return ipv4
		}
	}
	return net.IPv4(127, 0, 0, 1) // return loopback as default
}

// --------------------------------------------------------------------------------------------//
/*FILES AND HASHES*/

var HydfsFileStore = map[string]FileMetaData{} //key is filename
var hydfsFileStoreMutex sync.RWMutex

type FileAppendsTracker struct {
	mu    sync.RWMutex
	files map[string][]FileAppend
}

var AppendsFileStore *FileAppendsTracker

func SetupDirectories(directories ...string) error {

	for _, dir := range directories {
		// check if directory exists
		_, err := os.Stat(dir)

		if os.IsNotExist(err) {
			// directory doesn't exist, create it
			err = os.MkdirAll(dir, 0755)
			if err != nil {
				LogMessage("failed to create directory " + dir + ": " + err.Error())
				return err
			}
			LogMessage("Created directory: " + dir)
		} else if err != nil {
			LogMessage("error with directory " + dir + ": " + err.Error())
			// any other err
			return err
		} else {
			// directory exists, clear its contents
			clearDirectory(dir)
			LogMessage("Cleared contents of directory: " + dir)
		}
	}
	return nil
}

func AddAppendsEntry(filename, filepath string, timestamp time.Time, ip string) {
	AppendsFileStore.mu.Lock()
	defer AppendsFileStore.mu.Unlock()

	entry := FileAppend{
		FilePath:  filepath,
		Timestamp: timestamp,
		IP:        ip,
	}

	AppendsFileStore.files[filename] = append(AppendsFileStore.files[filename], entry)
}

func GetEntries(filename string) []FileAppend {
	AppendsFileStore.mu.RLock()
	defer AppendsFileStore.mu.RUnlock()

	return AppendsFileStore.files[filename]
}

func DeleteEntries(filename string) {
	AppendsFileStore.mu.RLock()
	defer AppendsFileStore.mu.RUnlock()

	delete(AppendsFileStore.files, filename)
}

// GetMetadata retrieves metadata for a file
func GetHyDFSMetadata(filename string) (FileMetaData, bool) {
	hydfsFileStoreMutex.RLock()
	defer hydfsFileStoreMutex.RUnlock()
	metadata, exists := HydfsFileStore[filename]
	return metadata, exists
}

// SetMetadata sets or updates metadata for a file
func SetHyDFSMetadata(filename string, metadata FileMetaData) {
	hydfsFileStoreMutex.Lock()
	defer hydfsFileStoreMutex.Unlock()
	HydfsFileStore[filename] = metadata
}

func GetAllHyDFSMetadata() map[string]FileMetaData {
	hydfsFileStoreMutex.RLock()
	defer hydfsFileStoreMutex.RUnlock()
	copyMap := make(map[string]FileMetaData, len(HydfsFileStore))
	for k, v := range HydfsFileStore {
		copyMap[k] = v
	}
	return copyMap
}

// DeleteMetadata removes metadata for a file
func DeleteMetadata(filename string) {
	hydfsFileStoreMutex.Lock()
	defer hydfsFileStoreMutex.Unlock()
	delete(HydfsFileStore, filename)
}

func clearDirectory(dir string) {
	d, err := os.Open(dir)
	if err != nil {
		LogMessage("failed to open directory " + dir + " : " + err.Error())
		return
	}
	defer d.Close()

	names, err := d.Readdirnames(-1)
	if err != nil {
		LogMessage("failed to read directory " + dir + " : " + err.Error())
		return
	}

	for _, name := range names {
		err = os.RemoveAll(filepath.Join(dir, name))
		if err != nil {
			LogMessage("failed to remove file " + name + " from directory " + dir + " : " + err.Error())
			return
		}
	}
}

func FileExists(filePath string) bool {
	_, err := os.Stat(filePath)
	return !os.IsNotExist(err)
}

// file comparision , checking md5 hash

func GetMD5(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hash := md5.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}

	return fmt.Sprintf("%x", hash.Sum(nil)), nil
}

func CompareFiles(file1, file2 string) (bool, error) {
	hash1, err := GetMD5(file1)
	if err != nil {
		return false, err
	}

	hash2, err := GetMD5(file2)
	if err != nil {
		return false, err
	}

	return hash1 == hash2, nil
}

func CopyFile(src, dst string) error {
	// Open the source file
	sourceFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	// Create the destination file
	destinationFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer destinationFile.Close()

	// Copy the content from source to destination
	_, err = io.Copy(destinationFile, sourceFile)
	if err != nil {
		return err
	}

	// Flush the contents to the disk (in case of buffered writes)
	err = destinationFile.Sync()
	if err != nil {
		return err
	}

	LogMessage("File copied from " + src + " to " + dst)
	return nil
}

// Hashing function
func Hashmurmur(name string) uint32 {
	return murmur3.Sum32([]byte(name)) % 1024
}

func ClearAppendFiles(directoryPath, prefix string) error {

	return filepath.Walk(directoryPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err // If there's an error accessing the path, return it
		}

		// Skip directories
		if info.IsDir() {
			return nil
		}

		// Check if the file name starts with the prefix
		if strings.HasPrefix(info.Name(), prefix) {
			err := os.Remove(path)
			if err != nil {
				errMsg := fmt.Sprintf("Failed to remove file %s: %v", path, err)
				LogMessage(errMsg)
				return err // Or continue to next file if you prefer: return nil
			}
			LogMessage("Removed file: " + path)
		}

		return nil
	})
}

// -------------------------------------
/* SCHEDULER Variables, Methods */

func FileLineCount(filePath string) (int, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	count := 0
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		count++
	}
	LogMessage(fmt.Sprintf("File %s has %d lines", filePath, count))
	return count, scanner.Err()
}

// in case we have to use word count as Aggregate by Key function
// this will return the ASCII value of the first letter of the word
// and we will use a range of such ascii values to assign keys to Nodes
// or we can just use utility.KeyMurmurHash(word)
func GetFirstLetterASCII(word string) int {
	if len(word) > 0 {
		return int(word[0])
	}
	return -1
}

func KeyMurmurHash(word string, numTasks int) uint32 {
	// return 0 , 1 , 2 , 3
	return murmur3.Sum32([]byte(word)) % uint32(numTasks)
}

/*Storm stop server */
func StopServerStormVer(node string) {
	serverIP := GetIPAddr(node)
	conn, err := grpc.Dial(serverIP.String()+":"+"4001", grpc.WithInsecure())
	if err != nil {
		LogMessage("Unable to connect to server - ring rpc fserver - " + err.Error())
	}
	defer conn.Close()
	client := stormgrpc.NewStormWorkerClient(conn)

	// Prepare the request
	req := &stormgrpc.StopStormRequest{
		Node: node,
	}
	LogMessage(fmt.Sprintf("Sending Stop signal to node: %v", node))
	// Call the PerformOperation RPC
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = client.StopServer(ctx, req)
	if err != nil {
		log.Fatalf("Failed to perform operation: %v", err)
	}
}
