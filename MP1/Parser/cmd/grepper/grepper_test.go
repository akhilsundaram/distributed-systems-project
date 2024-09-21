package grepper

import (
	"bufio"
	"fmt"
	"os"
	"testing"
)

var FINAL_OUTPUT = "/home/log/finalcount.log"
var RESULTS_DIR = "/home/log/verified_ut_logs/"

func TestCallCommand(t *testing.T) {

	// UT 1 - Frequent Pattern
	fmt.Println("Unit test 1")
	t.Run("FrequentPattern", func(t *testing.T) {
		CallCommand("grep", "", "PUT", "test")
		differingCounts, err := CompareTestResults(RESULTS_DIR+"correct_op1.log", FINAL_OUTPUT)
		if err != nil {
			fmt.Println("Error:", err)
			return
		}

		if differingCounts == 0 {
			fmt.Println("The files are identical.")
		} else {
			t.Errorf("The files differ at lines: %v\n", differingCounts)
		}

	})

	fmt.Println("Unit test 2")
	// UT 2 - Rare Pattern
	t.Run("RarePattern", func(t *testing.T) {
		CallCommand("grep", "", "huffman", "test")

		differingCounts, err := CompareTestResults(RESULTS_DIR+"correct_op2.log", FINAL_OUTPUT)
		if err != nil {
			fmt.Println("Error:", err)
			return
		}

		if differingCounts == 0 {
			fmt.Println("The files are identical.")
		} else {
			t.Errorf("The files differ at lines: %v\n", differingCounts)
		}

	})

	fmt.Println("Unit test 3")
	// UT 3 - Somewhat Frequent Pattern
	t.Run("SomewhatFrequentPattern", func(t *testing.T) {
		CallCommand("grep", "", "homepage", "test")

		differingCounts, err := CompareTestResults(RESULTS_DIR+"correct_op3.log", FINAL_OUTPUT)
		if err != nil {
			fmt.Println("Error:", err)
			return
		}

		if differingCounts == 0 {
			fmt.Println("The files are identical.")
		} else {
			t.Errorf("The files differ at lines: %v\n", differingCounts)
		}

	})

	// UT 4 - Regex
	fmt.Println("Unit test 4")
	t.Run("RegexSearch", func(t *testing.T) {
		CallCommand("grep", "E", "pad*", "test")

		differingCounts, err := CompareTestResults(RESULTS_DIR+"correct_op4.log", FINAL_OUTPUT)
		if err != nil {
			fmt.Println("Error:", err)
			return
		}

		if differingCounts == 0 {
			fmt.Println("The files are identical.")
		} else {
			t.Errorf("The files differ at lines: %v\n", differingCounts)
		}

	})

	// UT 5
	t.Run("InvertMatch", func(t *testing.T) {
		CallCommand("grep", "v", "hostname", "test")

		differingCounts, err := CompareTestResults(RESULTS_DIR+"correct_op5.log", FINAL_OUTPUT)
		if err != nil {
			fmt.Println("Error:", err)
			return
		}

		if differingCounts == 0 {
			fmt.Println("The files are identical.")
		} else {
			t.Errorf("The files differ at lines: %v\n", differingCounts)
		}

	})
}

func CompareTestResults(file1, file2 string) (int, error) {

	f1, err := os.Open(file1)
	if err != nil {
		return -1, fmt.Errorf("could not open file1: %v", err)
	}
	defer f1.Close()

	f2, err := os.Open(file2)
	if err != nil {
		return -1, fmt.Errorf("could not open file2: %v", err)
	}
	defer f2.Close()

	scanner1 := bufio.NewScanner(f1)
	scanner2 := bufio.NewScanner(f2)

	var differingCounts int

	// check eahc file line by line

	line1 := scanner1.Text()
	line2 := scanner2.Text()

	if line1 != line2 {
		differingCounts = 1
	} else {
		differingCounts = 0
	}

	if err := scanner1.Err(); err != nil {
		return -1, fmt.Errorf("error reading file1: %v", err)
	}
	if err := scanner2.Err(); err != nil {
		return -1, fmt.Errorf("error reading file2: %v", err)
	}

	return differingCounts, nil
}
