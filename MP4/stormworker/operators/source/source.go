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
			Filename string `json:"filename"`
			LineNo   int    `json:"linenumber"`
			Content  string `json:"content"`
		}

		err := json.Unmarshal([]byte(input), &inputData)
		if err != nil {
			continue
		}
		key := fmt.Sprintf("%s:%d", inputData.Filename, inputData.LineNo)

		output := map[string]interface{}{
			key: inputData.Content,
		}

		jsonOutput, err := json.Marshal(output)
		if err != nil {
			continue
		}
		fmt.Println(string(jsonOutput))
		return
	}
}
