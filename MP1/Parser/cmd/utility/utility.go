package utility

import (
	"Parser/cmd/grepper"
	"flag"
	"fmt"
	"os"
)

func CLIParser() {
	cmd := flag.String("cmd", "grep", "Command to be passed to distributed file logger")
	options := flag.String("o", "", "Options to be passed to command - GREP")
	pattern := flag.String("s", "", "Pattern to be searched")
	test := flag.String("t", "", "Unit Test execution")

	flag.Parse()

	if *cmd != "grep" {
		fmt.Println("Only `grep` supported")
		os.Exit(1)
	}

	if *pattern == "" {
		fmt.Println("Please specify pattern - cannot be empty")
		os.Exit(1)
	}

	// fmt.Printf("cmd - %s -%s %s", *cmd, *options, *pattern)

	// CallToGrepper(*cmd, *options, *pattern)
	if *test != "" {
		grepper.CallCommand(*cmd, *options, *pattern, *test)
	} else {
		grepper.CallCommand(*cmd, *options, *pattern)
	}
}
