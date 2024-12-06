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
		var inputData struct {
			Content string `json:"Content"`
			LineNo  int    `json:"LineNo"`
			Count   string `json:"Count"`
		}

		err := json.Unmarshal([]byte(input), &inputData)
		if err != nil {
			continue
		}
		countMap := make(map[string]int)
		err = json.Unmarshal([]byte(inputData.Count), &countMap)
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

		countKey := ""
		for key := range contentData.Data {
			countKey = key
			break
		}

		if _, exists := countMap[countKey]; exists {
			countMap[countKey]++
		} else {
			countMap[countKey] = 1
		}

		output := map[string]interface{}{
			"meta": map[string]interface{}{
				"columns":       contentData.Meta.Columns,
				"lineProcessed": inputData.LineNo,
				"count":         countMap,
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
