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
			Col     string `json:"Col"` //hardcode
			Pattern string `json:"Pattern"`
			OutCol  string `json:"OutCol"` //hardcode
		}

		err := json.Unmarshal([]byte(input), &inputData)
		if err != nil {
			continue
		}

		// Parse Content as JSON
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

		// Extract columns_from_input_string and split it
		columnsFromInput := strings.Split(contentData.Meta.Columns, ",")
		lineData := ""
		for _, value := range contentData.Data {
			lineData = value // Assuming only one key-value pair in Data
			break
		}

		// Get indexes of Col and OutCol
		colIndex := -1
		outColIndex := -1
		for i, col := range columnsFromInput {
			if col == inputData.Col {
				colIndex = i
			}
			if col == inputData.OutCol {
				outColIndex = i
			}
		}

		// If indexes are invalid, skip processing
		if colIndex == -1 || outColIndex == -1 {
			continue
		}

		// Split line_data and get line_value and out_value
		lineList := strings.Split(lineData, ",")
		if colIndex >= len(lineList) || outColIndex >= len(lineList) {
			continue
		}
		lineValue := lineList[colIndex]
		outValue := lineList[outColIndex]

		// Check if Pattern matches line_value
		if inputData.Pattern == lineValue {
			// Create the output structure
			output := map[string]interface{}{
				"meta": map[string]interface{}{
					"columns":       contentData.Meta.Columns,
					"lineProcessed": inputData.LineNo,
				},
				"data": map[string]string{
					outValue: lineData,
				},
			}

			// Marshal the output to JSON and print
			jsonOutput, err := json.Marshal(output)
			if err != nil {
				continue
			}
			fmt.Println(string(jsonOutput))
		}
	}
}
