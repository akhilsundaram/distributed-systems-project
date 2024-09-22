package membership

import (
	"fmt"
	"sync"
)

var (
	membership_list map[string]bool
	suspicion_table map[string]int
	memLock         sync.RWMutex
	susLock         sync.RWMutex
)

/////// MEMBERSHIP TABLE FUNCTIONS //////

func CheckMember(member string) bool {
	memLock.Lock()
	defer memLock.Unlock()

	if _, ok := membership_list[member]; ok {
		return true
	} else {
		return false
	}

}

func AddMember(member string) error {
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

func DeleteMember(member string) error {
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
		suspicion_table[member] = state
	} else {
		return -1, fmt.Errorf("error sus: member does not have suspicion.\n")
	}
}
