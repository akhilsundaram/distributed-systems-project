package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		input := scanner.Text()

		// Define the input structure
		var inputData struct {
			Params string            `json:"params"`
			Data   map[string]string `json:"data"`
		}

		err := json.Unmarshal([]byte(input), &inputData)
		if err != nil {
			continue
		}

		lineData := ""
		for _, value := range inputData.Data {
			lineData = value
		}

		needed := strings.Join(strings.Split(lineData, ",")[2:4], ",")

		output := map[string]interface{}{
			needed: "",
		}
		jsonOutput, err := json.Marshal(output)
		if err != nil {
			continue
		}
		fmt.Println(string(jsonOutput))
		return
	}
}
