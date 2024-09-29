package grepper

import (
	"bufio"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
)

var (
	fileMutex         sync.Mutex
	countMutex        sync.Mutex
	LISTENER_PORT_NO  = 8080
	serverCounts      = make(map[string]int)
	OUTPUT_FILE       = "/home/log/finaloutput.log"
	OUTPUT_COUNT_FILE = "/home/log/finalcount.log"
)

//go:embed configs/machines.txt
var MACHINE_LIST embed.FS

type InputData struct {
	Type string `json:"type"` //if data is cmd
	Data string `json:"data"`
}

func CallCommand(cmd string, options string, pattern string, ut ...string) {
	// Get server list
	startTime := time.Now()

	servers := getServerLists()
	// fmt.Print(servers)

	//Initialize counts for servers
	for i := 0; i < len(servers); i++ {
		serverCounts[servers[i]] = 0
	}

	//Go routine wait group
	var wg sync.WaitGroup

	// request string
	var req InputData

	//Set Type
	req.Type = "cmd"
	if len(ut) > 0 {
		req.Type = "test"
	}

	//Output if Options are empty.
	if options != "" {
		req.Data = cmd + " " + "-" + options + " " + pattern // add filename if needed
	} else {
		req.Data = cmd + " " + pattern // add filename if needed
	}
	request, jsonErr := json.Marshal(req)
	if jsonErr != nil {
		fmt.Printf("json marshall err for req data - %v", jsonErr)
	}

	//Clear output file
	if err := os.Truncate(OUTPUT_FILE, 0); err != nil {
		fmt.Printf("failed to truncate: %v", err)
	}

	// Call each server, go routines
	for i := 0; i < len(servers); i += 1 {
		// add check for same machine
		wg.Add(1)

		go func(server string) {
			defer wg.Done()

			//Clear server's output file file
			file, err := os.OpenFile(server, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
			if err != nil {
				fmt.Printf("error on creating empty file / emptying existing file - %v", err)
			}
			file.Close()

			//Create a connection to listener
			ip_addr := getIPAddr(server)
			conn, err := net.DialTimeout("tcp", ip_addr.String()+":"+strconv.Itoa(LISTENER_PORT_NO), time.Millisecond*1200)
			if err != nil {
				//fmt.Printf("err in connection - %v - %s\n", err, server)
				countMutex.Lock()
				serverCounts[server] = -1
				countMutex.Unlock()
				return
			}
			defer conn.Close()

			//Send req
			_, err = conn.Write([]byte(request))
			if err != nil {
				//fmt.Printf("err in sending req to listener - %v", err)
				countMutex.Lock()
				serverCounts[server] = -1
				countMutex.Unlock()
				return
			}

			// Create reader and read buffer
			reader := bufio.NewReader(conn)
			buffer := make([]byte, 1024)
			// fmt.Println("Buffer created")
			append_or_trunc := false

			// Read in chunks
			for {
				sizeResponse, err := reader.Read(buffer)
				if err != nil {
					if err.Error() == "EOF" {
						break
					}
					fmt.Printf("err reading bytes into buffer from listener - %v", err)
				}
				// fmt.Println("Data chunk read")
				data := buffer[:sizeResponse]
				// fmt.Printf("%s", string(data))
				// Write to file temp
				if append_or_trunc {
					WriteToFile(server, data, true) //append every other time.
				} else {
					WriteToFile(server, data, false) //Truncate the first time, the set bool.
					append_or_trunc = true
				}
				// fmt.Println("Write to file placeholder")

			}

			//temp file count total lines
			count, cErr := countLines(server)
			if cErr != nil {
				fmt.Printf("count line err - %v", err)
			}

			// Updated counter
			countMutex.Lock()
			serverCounts[server] += count
			countMutex.Unlock()
			//Write whole thing to Output file
			lockedWrite(server)

		}(servers[i])
	}

	// Wait for threads to return
	wg.Wait()
	endTime := time.Now()
	duration := endTime.Sub(startTime)
	fmt.Printf("grep took %s to complete.\n", duration)

	outputgrep()
	// fmt.Printf("End func!\n") // Change this to actually output/print file, ++ line counts.
	//parse output file and print

}

// Get server list
func getServerLists() []string {
	var out []string
	file_io, err := MACHINE_LIST.Open("configs/machines.txt")
	if err != nil {
		fmt.Printf("read error - %v", err)
	}

	defer file_io.Close()

	scan_file := bufio.NewScanner(file_io)

	for scan_file.Scan() {
		out = append(out, scan_file.Text())
	}
	if err := scan_file.Err(); err != nil {
		fmt.Println("Error reading embedded file:", err)
	}

	return out
}

// Locked writes
func lockedWrite(filename string) { // Reads from OUTPUT_FILE var which is a file embed
	fileMutex.Lock()
	defer fileMutex.Unlock()

	//Open inout file
	file, err := os.Open(filename)
	if err != nil {
		fmt.Printf("file open error - %s\n", err)
	}
	defer file.Close()

	// make a reader
	input_reader := bufio.NewReader(file)
	buf := make([]byte, 1024)
	myString := "********** Log from " + filename + "***********\n"
	m := len(myString)
	copy(buf, []byte(myString))
	WriteToFile(OUTPUT_FILE, buf[:m], true)

	//Call write function
	for {
		n, err := input_reader.Read(buf)

		if err != nil && err != io.EOF {
			fmt.Printf("error reading from input file into buffer in lockedwrite - %v\n", err)
		}
		//EOF, no more data
		if n == 0 {
			break
		}
		WriteToFile(OUTPUT_FILE, buf[:n], true)
	}

	// fmt.Printf("write called - locked - %s\n", filename)

}

func getIPAddr(host string) net.IP {
	ips, err := net.LookupIP(host) // Can give us a string of IPs.

	if err != nil {
		fmt.Printf("Error on IP lookup for %s", host)
	}

	for _, ip := range ips { //iterate through and get first IP.
		if ipv4 := ip.To4(); ipv4 != nil {
			return ipv4
		}
	}
	return net.IPv4(127, 0, 0, 1) // return loopback as default
}

func WriteToFile(filename string, data []byte, appendMode bool) error {

	var fileFlags int
	if appendMode {
		// append if it exists
		fileFlags = os.O_CREATE | os.O_WRONLY | os.O_APPEND
	} else {
		// truncate the file
		fileFlags = os.O_CREATE | os.O_WRONLY | os.O_TRUNC
	}

	// open file with flasg
	file, err := os.OpenFile(filename, fileFlags, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Write(data) // write data
	if err != nil {
		return err
	}

	// fmt.Println("data written successfully!")
	return nil
}

// Source - https://gist.github.com/djale1k/a328bbb96e26ec304d320f60e8c5e87a
func countLines(filename string) (int, error) {
	file, err := os.Open(filename)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lineCount := 0

	// Iterate through each line in the file
	for scanner.Scan() {
		lineCount++
	}

	// Check for any errors encountered during scanning
	if err := scanner.Err(); err != nil {
		return 0, err
	}

	return lineCount, nil

}

// Read line by line and print output of grep from OUTPUT_FILE
// cite - https://golangdocs.com/golang-read-file-line-by-line
func outputgrep() {

	//open output file
	/*
		file, err := os.Open(OUTPUT_FILE)
		if err != nil {
			fmt.Println(err)
		}

		defer file.Close()

		reader := bufio.NewScanner(file)
		reader.Split(bufio.ScanLines)

		for reader.Scan() {
			fmt.Println(reader.Text())
		}
	*/

	total_count := 0
	// Print counts
	for key, value := range serverCounts {
		machine_name := "machine." + key[13:15] + ".log"
		if value != -1 {
			total_count += value
			fmt.Printf("Matching lines in server %s: %d\n", machine_name, value)
		} else {
			fmt.Printf("Matching lines in server %s: NA\n", machine_name)
		}
	}
	fmt.Printf("Total number of matching lines: %d\n", total_count)

	// write final count ot file for unit test checking
	file, err := os.OpenFile(OUTPUT_COUNT_FILE, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		fmt.Println(err)
	}

	defer file.Close()
	data := strconv.Itoa(total_count)
	_, err = file.Write([]byte(data))
	if err != nil {
		fmt.Println("Error writing to file:", err)
		return
	}
}
