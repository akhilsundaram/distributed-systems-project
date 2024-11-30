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
		var inputData map[string]string
		err := json.Unmarshal([]byte(input), &inputData)
		if err != nil {
			continue
		}
		var output []string
		for _, value := range inputData {
			words := strings.Fields(value)
			for _, word := range words {
				output = append(output, fmt.Sprintf("{'%s': 0}", word))
			}
		}
		fmt.Println(strings.Join(output, ", "))
	}
}
