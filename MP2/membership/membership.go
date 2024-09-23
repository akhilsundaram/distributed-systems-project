package membership

import (
	"fmt"
	"net"
	"sync"
	"time"
)

// Shared Buffer table element
type BufferValue struct {
	TimesSent int64
	Data      []byte
	CreatedAt time.Time
}

// Member type for memberhsip list
type Member struct {
	id                string
	host              string
	ip                net.IP
	incarnationNumber string
	timeStamp         time.Time
}

var (
	// Shared membership data
	membership_list map[string]Member
	memLock         sync.RWMutex

	//Shared Suspicion table lists
	suspicion_table map[string]int
	susLock         sync.RWMutex

	//Shared Buffer table
	shared_buffer []BufferValue
)

/////// MEMBERSHIP TABLE FUNCTIONS ////////

func GetMembership(member string) bool {
	memLock.Lock()
	defer memLock.Unlock()

	if _, ok := membership_list[member]; ok {
		return true
	} else {
		return false
	}

}

func AddMembership(member string) error {
	memLock.Lock()
	defer memLock.Unlock()

	//Add member to membership_list
	if _, ok := membership_list[member]; ok {
		return fmt.Errorf("error mem: member already exists.\n")
	} else {
		membership_list[member] = false
	}

	return nil
}

func DeleteMembership(member string) error {
	memLock.Lock()
	defer memLock.Unlock()

	if _, ok := membership_list[member]; ok {
		delete(membership_list, member)
	} else {
		return fmt.Errorf("error mem: member does not exist.\n")
	}

	return nil
}

/////// SUSPICION TABLE FUNCTIONS //////

func UpdateSuspicion(member string, state int) {
	susLock.Lock()
	defer susLock.Unlock()

	suspicion_table[member] = state
}

func GetSuspicion(member string) (int, error) {
	susLock.RLock()
	defer susLock.RUnlock()

	if _, ok := suspicion_table[member]; ok {
		return suspicion_table[member], nil
	} else {
		return -1, fmt.Errorf("error sus: member does not have suspicion.\n")
	}
}
