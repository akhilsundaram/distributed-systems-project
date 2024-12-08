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
		pattern := strings.Split(inputData.Params, ",")[0]

		lineData := ""
		linekey := ""
		for key, value := range inputData.Data {
			lineData = value
			linekey = key
		}
		if strings.Contains(lineData, pattern) {
			output := map[string]interface{}{
				linekey: lineData,
			}
			jsonOutput, err := json.Marshal(output)
			if err != nil {
				continue
			}
			fmt.Println(string(jsonOutput))
			return
		}
	}
}
