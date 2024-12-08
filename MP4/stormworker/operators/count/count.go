package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
)

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		input := scanner.Text()

		// Define the input structure
		var inputData struct {
			State  map[string]int    `json:"state"`
			Params string            `json:"params"`
			Data   map[string]string `json:"data"`
		}

		err := json.Unmarshal([]byte(input), &inputData)
		if err != nil {
			continue
		}

		category := ""
		for key, _ := range inputData.Data {
			category = key
			break
		}

		inputData.State[category] += 1

		jsonOutput, err := json.Marshal(inputData.State)
		if err != nil {
			continue
		}
		fmt.Println(string(jsonOutput))
		return
	}
}
