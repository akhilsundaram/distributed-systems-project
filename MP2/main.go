package main

import (
	"failure_detection/pingpong"
	"os"
)

func main() {
	args := os.Args
	pingpong.LogMessage(args[0])
	// typeArg is either "machine" or "caller", otherwise invalid

}
