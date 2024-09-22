package utility

import (
	"log"
	"os"
	"sync"
)

var (
	logFile *os.File
	logger  *log.Logger
	once    sync.Once
	mu      sync.Mutex
)

func initLogger() {
	once.Do(func() {
		var err error
		logFile, err = os.OpenFile("app.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			log.Fatal(err)
		}
		logger = log.New(logFile, "", log.LstdFlags)
	})
}

func LogMessage(message string) {
	initLogger()
	mu.Lock()
	defer mu.Unlock()
	logger.Println(message)
}
