package utility

import (
	"crypto/md5"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
)

var (
	logFile *os.File
	logger  *log.Logger
	once    sync.Once
	mu      sync.Mutex
)

var LOGGER_FILE = "/home/log/hydfs.log"

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

func LogMessage(message string) {
	initLogger()
	mu.Lock()
	defer mu.Unlock()
	logger.Println(message)
}

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
