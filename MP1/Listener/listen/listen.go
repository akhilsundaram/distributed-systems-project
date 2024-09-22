package listen

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// var (
// 	ip   string
// 	port string
// )

var SOURCE_DIR = "/home/log/"
var SOURCE_FILE = "/home/log/ut.log"
var OUTPUT_FILE = "/home/log/sample.log"
var LOG_PATH = "/usr/bin/log/"

// var logfile = "test.log"

type InputData struct {
	Type string `json:"type"` //if data is cmd / test (UT)
	Data string `json:"data"`
}

func HostListener(portNo string) {
	// ip_addr := getIPAddr(host)
	// ip_addr := net.IPv4(127, 0, 0, 1)

	listener, err := net.Listen("tcp", ":8080")
	println("Listener created")

	if err != nil {
		fmt.Print(err)
	}

	defer func() { _ = listener.Close() }() // will get called at the end of HostListener.

	// Listener func
	for {
		println("inside listener")
		conn, err := listener.Accept() // Accept blocks so the listener, so we need a go routine

		if err != nil {
			fmt.Print(err)
		}

		go func(c net.Conn) {
			defer c.Close() // closes when the go routine ends
			buf := make([]byte, 1024)
			serverAddr := c.RemoteAddr().String()
			var parsedData InputData // struct to hold parsed data
			var out string
			out = ""

			for {
				//Check data
				n, err := c.Read(buf)
				if err != nil {
					fmt.Printf("Closing connection with server %s\n", serverAddr)
					return
				}

				//Parse data
				data := buf[:n]
				jsonErr := json.Unmarshal(data, &parsedData)
				if jsonErr != nil {
					println("Error Parsing data")
				}

				//Get machine number and machine_log name

				machine_log := "machine.log"

				if parsedData.Type == "cmd" {
					//Call Grep command
					println("GREP called")

					// exec.Command(parsedData.Data + LOG_PATH + logfile) // probably wrong. need cmd + args.!!!!!!!!

					out, err = executeGrepCommand(parsedData.Data + " /home/log/" + machine_log) // + strings.Split(host_name, ".")[0] + ".log")
					if err != nil {
						fmt.Printf("grep error says - %s\n", err)
					}
					//"1:IF THIS WAS A GREP\n2:I would be an output\n"
					writeErr := WriteToSocket(c, out, machine_log)
					if writeErr != nil {
						fmt.Printf("Write to socket errored out - %v\n", writeErr)
					}
					return
				} else if parsedData.Type == "test" {

					// call log_generator
					LogGenerator()
					// then go ahead with executing and write it to socket
					println("TEST called")

					machine_log = "sample.log" // ut file
					out, err = executeGrepCommand(parsedData.Data + " /home/log/" + machine_log)
					if err != nil {
						fmt.Printf("grep error says - %s\n", err)
					}

					writeErr := WriteToSocket(c, out, machine_log)
					if writeErr != nil {
						fmt.Printf("Write to socket errored out - %v\n", writeErr)
					}
					return

				} else {
					fmt.Println("Only cmd/test input supported for log listener")
					return
				}

			}

		}(conn)
	}

}

// TO CHANGE - insert machine.1.log before each line.
func WriteToSocket(c net.Conn, input string, machine_name string) error {
	// Write Buffer
	bufW := bufio.NewWriter(c)

	lines := strings.Split(input, "\n")

	for _, line := range lines {
		//append log file name before every line
		if line != "" {
			newline := machine_name + " :: " + line

			_, err := bufW.WriteString(newline)
			if err != nil {
				return fmt.Errorf("error writing modified log line to write buffer - %v", err)
			}

			// newline char at end of each log line again
			err = bufW.WriteByte('\n')
			if err != nil {
				return fmt.Errorf("error writing newline to buffer - %v", err)
			}
		}
	}

	//Flush
	err := bufW.Flush()
	if err != nil {
		return fmt.Errorf("error on buffer writer flush - %v", err)
	}

	//No errors
	return nil
}

func executeGrepCommand(command string) (string, error) {
	// Create the shell to execute  command
	c := strings.Split(command, " ")
	e := fmt.Errorf("")
	output := ""
	if len(c) == 3 {
		cmd := exec.Command(c[0], c[1], c[2])
		output, err := cmd.CombinedOutput()
		if err != nil {
			return "", err
		}
		return string(output), nil
	} else if len(c) == 4 {
		cmd := exec.Command(c[0], c[1], c[2], c[3])
		output, err := cmd.CombinedOutput()
		if err != nil {
			return "", err
		}
		return string(output), nil
	} else {
		e = fmt.Errorf("exec grep has wrong number of arguments - has %d. Expected 3 or 4 ", len(c))
	}

	// Return the output as a string
	return string(output), e
}

func randomString(r *rand.Rand, minLength, maxLength int) string {
	charset := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

	// random length between minLength and maxLength
	length := r.Intn(maxLength-minLength+1) + minLength

	result := make([]byte, length)
	for i := range result {
		result[i] = charset[r.Intn(len(charset))]
	}

	return string(result)
}

func LogGenerator() {

	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	// open directory for fetching file names
	files, err := os.ReadDir(SOURCE_DIR)
	if err != nil {
		fmt.Printf("Failed to read directory: %v", err)
	}

	substring := "machine"
	var machine_no string
	var machine_name string

	for _, file := range files {
		if !file.IsDir() && strings.Contains(file.Name(), substring) {
			machine_name = strings.Split(file.Name(), ".log")[0]
			machine_no = machine_name[len(machine_name)-2:]
		}
	}

	num, err := strconv.Atoi(machine_no)
	if err != nil {
		fmt.Printf("Error converting string to int: %v\n", err)
		return
	}

	// read file
	data, err := os.ReadFile(SOURCE_FILE)
	if err != nil {
		fmt.Printf("Error reading file: %v\n", err)
		return
	}

	// calculate section size
	totalSize := len(data)
	sectionSize := totalSize / 10

	if num < 1 || num > 10 {
		fmt.Println("section number :", num)
		return
	}

	// calculate the start and end position
	start := (num - 1) * sectionSize
	end := start + sectionSize
	if num == 10 {
		end = totalSize // last sectin goes to EOF
	}

	section := data[start:end]

	// Write the section to the output file
	err = os.WriteFile(OUTPUT_FILE, section, 0644)
	if err != nil {
		fmt.Printf("Error writing file: %v\n", err)
		return
	}

	file, err := os.OpenFile(OUTPUT_FILE, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()

	// not checking error here because Go is stupid
	file.WriteString("\n")

	for i := 0; i < 100; i++ {

		randomWord := randomString(r, 20, 35)

		_, err := file.WriteString(fmt.Sprintf("%s\n", randomWord))
		if err != nil {
			fmt.Println("Error writing to file:", err)
			return
		}
	}

	fmt.Println("Generated log file for Unit test for", machine_no, "at location", OUTPUT_FILE)
}
