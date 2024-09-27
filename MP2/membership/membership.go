package membership

import (
	"fmt"
	"maps"
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
	node_id           string
	incarnationNumber string
}

var (
	// Shared membership data
	membership_list map[string]Member
	memLock         sync.RWMutex

	//Shared Suspicion table lists
	suspicion_table map[string]SuspicionState
	susLock         sync.RWMutex

	//Shared Buffer table
	shared_buffer []BufferValue
	buffLock      sync.RWMutex
)

/////// MEMBERSHIP TABLE FUNCTIONS ////////

func IsMember(hostname string) bool {
	memLock.Lock()
	defer memLock.Unlock()

	if _, ok := membership_list[hostname]; ok {
		return true
	} else {
		return false
	}

}

// func GetMemberHostname(member_id string) (string, error) {
// 	memLock.Lock()
// 	defer memLock.Unlock()

// 	if _, ok := membership_list[member_id]; ok {
// 		return membership_list[member_id].hostname, nil
// 	} else {
// 		return "", fmt.Errorf("get hostname of member: No member exists of name %s", member_id)
// 	}

// }

func AddMember(node_id string, hostname string) error {
	memLock.Lock()
	defer memLock.Unlock()

	//Add member to membership_list
	if _, ok := membership_list[hostname]; ok {
		return fmt.Errorf("error mem: member already exists.\n")
	} else {
		//initialise new member
		var new_member Member
		new_member.incarnationNumber = "0"
		new_member.node_id = node_id

		//Add to map
		membership_list[hostname] = new_member
	}

	return nil
}

func DeleteMember(hostname string) error {
	memLock.Lock()
	defer memLock.Unlock()

	if _, ok := membership_list[hostname]; ok {
		delete(membership_list, hostname)
	} else {
		return fmt.Errorf("error mem: member does not exist.\n")
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
		return -1, fmt.Errorf("error sus: member does not exist\n")
	} else {
		return -2, fmt.Errorf("error sus: member does not have suspicion.\n")
	}
}

// ///// BUFFER TABLE FUNCTIONS //////
func WriteToBuffer(data []byte) {
	buffLock.Lock()
	defer buffLock.Unlock()

	var new_buffer_element BufferValue
	new_buffer_element.CreatedAt = time.Now()
	new_buffer_element.Data = []byte(data)
	new_buffer_element.TimesSent = 0

	shared_buffer = append(shared_buffer, new_buffer_element)
	return
}

func UpdateBufferGossipCounts(count int) {
	buffLock.Lock()
	defer buffLock.Unlock()

	for i := 0; i < count; i++ {
		shared_buffer[i].TimesSent += 1
	}
}

func GetBufferElements(count int) []BufferValue {
	buffLock.RLock()
	defer buffLock.RUnlock()
	// Should I call UpdateBufferGossipCount here ? or should the pinger take this ?
	return shared_buffer[count:] //careful of modifying data - race conditions

}
