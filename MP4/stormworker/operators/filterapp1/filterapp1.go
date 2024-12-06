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
			Content string `json:"Content"`
			LineNo  int    `json:"LineNo"`
			Pattern string `json:"Pattern"`
		}

		err := json.Unmarshal([]byte(input), &inputData)
		if err != nil {
			continue
		}
		var contentData struct {
			Meta struct {
				Columns       string `json:"columns"`
				LineProcessed int    `json:"lineProcessed"`
			} `json:"meta"`
			Data map[string]string `json:"data"`
		}

		err = json.Unmarshal([]byte(inputData.Content), &contentData)
		if err != nil {
			continue
		}
		lineData := ""
		for _, value := range contentData.Data {
			lineData = value
			break
		}
		if strings.Contains(lineData, inputData.Pattern) {
			output := map[string]interface{}{
				"meta": map[string]interface{}{
					"columns":       contentData.Meta.Columns,
					"lineProcessed": inputData.LineNo,
				},
				"data": contentData.Data,
			}

			jsonOutput, err := json.Marshal(output)
			if err != nil {
				continue
			}
			fmt.Println(string(jsonOutput))
		}
	}
}
