package main

import (
	"bufio"
	"encoding/csv"
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
		linemeta := ""
		for key, value := range inputData.Data {
			lineData = value
			linemeta = key

		}

		// values := strings.Split(lineData, ",")
		csv_reader := csv.NewReader(strings.NewReader(lineData))
		values, err := csv_reader.Read()
		if err != nil {
			continue
		}
		sign_post := values[6]

		if sign_post == pattern {

			output := map[string]interface{}{
				values[8]: linemeta,
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
