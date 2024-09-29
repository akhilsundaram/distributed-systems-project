package suspicion

import (
	"failure_detection/buffer"
	"failure_detection/membership"
	"failure_detection/utility"
	"fmt"
	"time"
)

var (
	suspicionTimeout = time.Millisecond * 200
	Enabled          = false
)

// Handles all incoming suspicion messages
func SuspicionHandler(Message, Node_id, hostname string) {
	switch Message {
	case "f":
		membership.UpdateSuspicion(hostname, membership.Faulty)
		membership.DeleteMember(Node_id, hostname)
		buffer.WriteToBuffer("f", Node_id, hostname)
		return
	case "s":
		membership.UpdateSuspicion(hostname, membership.Suspicious)
		time.AfterFunc(suspicionTimeout, func() { stateTransitionOnTimeout(hostname) })
		return
	case "a":
		membership.UpdateSuspicion(hostname, membership.Alive)
		// time.AfterFunc(suspicionTimeout, func() { stateTransitionOnTimeout(hostname) })
		return
	default:
		return

	}
	// SUS -alive
	// SUS -faulty
	// SUS -normal sus
	// Handle messages of type sus here.

}

// Declare a host as suspicious //
func DeclareSuspicion(hostname string) error {
	// Only time our ping/ server ever reqs sus data.
	// Declares aftertimer to handle states internally. Maybe even callable from the Handler
	state, err := membership.GetSuspicion(hostname)
	if err != nil {
		if state == -1 {
			utility.LogMessage("DeclareSuspicion error - " + err.Error())
			return fmt.Errorf("DeclareSuspicion error - no member %s in membership list error", hostname)
		}
	}
	if state == -2 || state == membership.Alive { //No suspicion exists, but host does
		membership.UpdateSuspicion(hostname, membership.Suspicious)
		time.AfterFunc(suspicionTimeout, func() { stateTransitionOnTimeout(hostname) })
		membership.WriteToBuffer("s", hostname) //Need to decide format for string/data output. Or handle it in membership ?
		return nil
	}
	return nil
}

func stateTransitionOnTimeout(hostname string) {
	state, err := membership.GetSuspicion(hostname)
	if err != nil {
		utility.LogMessage("StateTransitionOnTimeout error - " + err.Error())
	}
	if state == membership.Suspicious {
		membership.UpdateSuspicion(hostname, membership.Faulty)
		membership.WriteToBuffer("f", hostname)
	}
}
