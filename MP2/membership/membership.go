package membership

import (
	"encoding/json"
	"failure_detection/utility"
	"fmt"
	"maps"
	"net"
	"strings"
	"sync"
	"time"
)

// States
type SuspicionState int8

const (
	Suspicious SuspicionState = iota
	Alive
	Faulty
)

// Shared Buffer table element
type BufferValue struct {
	TimesSent int64
	Data      []byte
	CreatedAt time.Time
}

// Member type for memberhsip list
type Member struct {
	Node_id           string
	IncarnationNumber string
}

var (
	// Shared membership data
	membership_list = map[string]Member{}
	memLock         sync.RWMutex

	//Shared Suspicion table lists
	suspicion_table = map[string]SuspicionState{}
	susLock         sync.RWMutex

	//Shared Buffer table
	shared_buffer []BufferValue
	buffLock      sync.RWMutex
	BufferMap     = map[string]string{}
	maxTimesSent  = 4
)

/////// MEMBERSHIP TABLE FUNCTIONS ////////

func PrintMembershipList() {
	memLock.Lock()
	defer memLock.Unlock()
	for key, value := range membership_list {
		utility.LogMessage("Hostname: " + key + ", member_id: " + value.Node_id)
	}
}

func PrintMembershipListStdOut() {
	memLock.Lock()
	defer memLock.Unlock()
	for key, value := range membership_list {
		fmt.Println("Hostname: " + key + ", member_id: " + value.Node_id)
	}
}

func IsMember(hostname string) bool {
	memLock.Lock()
	defer memLock.Unlock()

	if _, ok := membership_list[hostname]; ok {
		return true
	} else {
		return false
	}

}

func GetMemberID(hostname string) string {
	if _, ok := membership_list[hostname]; ok {
		return membership_list[hostname].Node_id
	} else {
		return "-1"
	}
}

func GetMemberHostname(member_id string) string {
	ip := strings.Split(member_id, "_")[0]
	Hostname, err := net.LookupAddr(ip)
	if err != nil {
		utility.LogMessage("NewMemb error - getting hostname from " + ip + " due to - " + err.Error())
		return ""
	}

	return Hostname[0]
}

func AddMember(node_id string, hostname string) error {
	memLock.Lock()
	defer memLock.Unlock()

	//Add member to membership_list
	if _, ok := membership_list[hostname]; ok {
		// TODO timestamp check between node_ids- new and existing
		return nil
	} else {
		//initialise new member
		var new_member Member
		new_member.IncarnationNumber = "0"
		new_member.Node_id = node_id

		//Add to map
		membership_list[hostname] = new_member
		utility.LogMessage("New member added: " + hostname + " with member id: " + node_id)
	}

	return nil
}

func DeleteMember(node_id string, hostname string) error {
	memLock.Lock()
	defer memLock.Unlock()

	if _, ok := membership_list[hostname]; ok {
		delete(membership_list, hostname)
		utility.LogMessage("Member Deleted: " + hostname)
	} else {
		return fmt.Errorf("error mem: member does not exist")
	}

	return nil
}

func GetMembershipList() map[string]Member {
	return maps.Clone(membership_list)
}

/////// SUSPICION TABLE FUNCTIONS //////

func UpdateSuspicion(member string, state SuspicionState) {
	susLock.Lock()
	defer susLock.Unlock()

	suspicion_table[member] = state
}

func GetSuspicion(member string) (SuspicionState, error) {
	susLock.RLock()
	defer susLock.RUnlock()

	if _, ok := suspicion_table[member]; ok {
		return suspicion_table[member], nil
	} else if !IsMember(member) {
		return -1, fmt.Errorf("error sus: member does not exist")
	} else {
		return -2, fmt.Errorf("error sus: member does not have suspicion")
	}
}

// ///// BUFFER TABLE FUNCTIONS //////
func WriteToBuffer(msg_type string, node_id string) {
	buffLock.Lock()
	defer buffLock.Unlock()

	// Create byte buffer data block
	bufferData := make(map[string]interface{})
	bufferData[msg_type] = node_id
	jsonData, err := json.Marshal(bufferData)
	if err != nil {
		utility.LogMessage("write to buffer err " + err.Error())
	}
	BufferMap[string(jsonData)] = ""

	// Append to buffer
	var new_buffer_element BufferValue
	new_buffer_element.CreatedAt = time.Now()
	new_buffer_element.Data = []byte(jsonData)
	new_buffer_element.TimesSent = 0

	shared_buffer = append(shared_buffer, new_buffer_element)
}

func UpdateBufferGossipCounts() {
	buffLock.Lock()
	defer buffLock.Unlock()
	var toDelete []int

	for i := 0; i < len(shared_buffer); i++ {
		shared_buffer[i].TimesSent += 1
		if shared_buffer[i].TimesSent > int64(maxTimesSent) {
			toDelete = append(toDelete, i)
		}
	}

	for i := 0; i < len(toDelete); i++ {
		//Key
		delete(BufferMap, string(shared_buffer[toDelete[i]].Data))
	}

	for i := len(toDelete) - 1; i >= 0; i-- {
		shared_buffer = append(shared_buffer[:toDelete[i]], shared_buffer[toDelete[i]+1:]...)
	}

}

func GetBufferElements() []BufferValue {
	buffLock.RLock()
	defer buffLock.RUnlock()
	// Should I call UpdateBufferGossipCount here ? or should the pinger take this ?
	return shared_buffer //careful of modifying data - race conditions

}

func CheckBuffer(data []byte) bool {
	buffLock.Lock()
	defer buffLock.Unlock()
	if _, ok := BufferMap[string(data)]; ok {
		return true
	}
	return false
}
