package log_generator

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"
)

var SOURCE_DIR = "/home/log/"
var SOURCE_FILE = "/home/log/ut.log"
var OUTPUT_FILE = "/home/log/sample.log"

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

	// create a new
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	// open directory for fetching file names
	files, err := os.ReadDir(SOURCE_DIR)
	if err != nil {
		fmt.Printf("Failed to read directory: %v", err)
	}

	substring := "fa24-cs425-59"
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

	fmt.Println("Generated log file for Unit test for ", machine_no, " at location ", OUTPUT_FILE)
}
