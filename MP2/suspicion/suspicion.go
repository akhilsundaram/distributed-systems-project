package suspicion

import (
	"time"
)

type state int8

const (
	Suspicious state = iota
	Alive
	Faulty
)

var (
	suspicionTimeout = time.Microsecond * 10
	faultyTimeout    = time.Microsecond * 10
)

// Handles all incoming suspicion messages
func SuspicionHandler(message string) {
	// This should handle all incoming suspicion messages from listener
	// SUS -alive
	// SUS -faulty
	// SUS -normal sus
	// Handle messages of type sus here.

}

// Declare a host as suspicious //
func DeclareSuspicion(host string) error {
	// Only time our ping/ server ever reqs sus data.
	// Declares aftertimer to handle states internally. Maybe even callable from the Handler
	return nil
}
