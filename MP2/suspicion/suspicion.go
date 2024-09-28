package suspicion

import (
	"encoding/json"
	"failure_detection/membership"
	"failure_detection/utility"
	"fmt"
	"time"
)

var (
	suspicionTimeout = time.Microsecond * 10
	faultyTimeout    = time.Microsecond * 10
	Enabled          = false
)

// Handles all incoming suspicion messages
func SuspicionHandler(key string, value string) {
	switch key {
	case "f":
		membership.UpdateSuspicion(value, membership.Faulty)
		time.AfterFunc(faultyTimeout, func() { stateTransitionOnTimeout(value) })
		return
	case "s":
		membership.UpdateSuspicion(value, membership.Suspicious)
		time.AfterFunc(suspicionTimeout, func() { stateTransitionOnTimeout(value) })
		return
	case "a":
		membership.UpdateSuspicion(value, membership.Alive)
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
		if state == -2 {
			utility.LogMessage("DeclareSuspicion error - " + err.Error())
			return fmt.Errorf("DeclareSuspicion error - no member %s in membership list error", hostname)
		}
	}
	if state == -1 || state == membership.Alive { //No suspicion exists, but host does
		membership.UpdateSuspicion(hostname, membership.Suspicious)
		time.AfterFunc(suspicionTimeout, func() { stateTransitionOnTimeout(hostname) })

		data := make(map[string]interface{})
		data["s"] = hostname // can I put ip address  and it will only be 4 bytes ?

		jsonData, err := json.Marshal(data)
		if err != nil {
			return fmt.Errorf("DeclareSuspicion error - unable to marshall buffer json value")
		}
		membership.WriteToBuffer(jsonData) //Need to decide format for string/data output. Or handle it in membership ?
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
		data := make(map[string]interface{})
		data["f"] = hostname

		jsonData, err := json.Marshal(data)
		if err != nil {
			utility.LogMessage("StateTransitionOnTimeout error - unable to marshall buffer json value")
		}
		membership.WriteToBuffer(jsonData)
	}
}
