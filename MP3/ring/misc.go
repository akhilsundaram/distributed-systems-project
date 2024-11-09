package ring

import (
	"fmt"
	"hydfs/membership"
	"hydfs/utility"
	"sort"
)

func AddRingMember(newMember ringMember) {
	ringLock.Lock()
	defer ringLock.Unlock()
	utility.LogMessage("Adding new member - " + newMember.serverName)
	ring = append(ring, newMember)
	sort.Slice(ring, func(i, j int) bool {
		return ring[i].hashID < ring[j].hashID
	})
	utility.LogMessage("member list added, sorted")
	updateSuccessors()
	utility.LogMessage("successor list updated")
	ringNodes[newMember.hashID] = 1
	utility.LogMessage("Hash check for node updated.")
}

func DeleteRingMember(serverName string) (int, error) {
	ringLock.Lock()
	defer ringLock.Unlock()
	var index int
	found := false
	for i, member := range ring {
		if member.serverName == serverName {
			index = i
			found = true
			break
		}
	}
	if !found {
		return -1, fmt.Errorf("server %s not found in ring", serverName)
	}
	ring = append(ring[:index], ring[index+1:]...)
	updateSuccessors()
	delete(ringNodes, utility.Hashmurmur(serverName))
	return index, nil
}

func updateSuccessors() {
	n := len(ring)
	for i := 0; i < n; i++ {
		ring[i].successor = make([]uint32, 0, 2)
		for j := 1; j <= 2; j++ {
			ring[i].successor = append(ring[i].successor, ring[(i+j)%n].hashID)
		}
	}
}

func findFileRanges(serverName string) (uint32, uint32, error) {
	n := len(ring)
	var index int
	found := false
	for i, member := range ring {
		if member.serverName == serverName {
			index = i
			found = true
			break
		}
	}
	if !found {
		return 0, 0, fmt.Errorf("server %s not found in ring", serverName)
	}

	if n <= 3 {
		return 0, 1024, nil
	}

	lowerIndex := (index - replicas + n) % n
	low := ring[lowerIndex].hashID
	high := ring[index].hashID

	return low, high, nil
}

// Ask node to drop the file list - called when it gets { a files req from a newly added node } OR {sees newly added node and asks the replica + 1th node to drop files which won't be part of added node's hash }
func dropFiles(low uint32, high uint32) {
	delete_list := getFileList(low, high)
	for _, filename := range delete_list {
		//change to file_transfer function
		handleDelete(filename)
	}
}

// Ask node to drop the file list - called when it gets { a files req from a newly added node } OR {sees newly added node and asks the replica + 1th node to drop files which won't be part of added node's hash }
func dropFilesNotInRange(low uint32, high uint32) {
	utility.LogMessage("Files tried to be dropped: ")
	needed_files := getFileList(high, low)
	needed := make(map[string]struct{})
	for _, item := range needed_files {
		utility.LogMessage("Deletion marked for - " + item)
		needed[item] = struct{}{} // Using an empty struct to save memory
	}

	for filename, _ := range utility.HydfsFileStore {
		if _, exists := needed[filename]; exists {
			handleDelete(filename)
		}
	}
}

func PrintRing() {
	for _, ringMember := range ring {
		low, high, err := findFileRanges(ringMember.serverName)
		if err != nil {
			utility.LogMessage("error getting file ranges while printing")
		}
		println(ringMember.serverName)
		if ringMember.serverName == membership.My_hostname {
			fmt.Printf("host: %s, ringID: %d, successors: %d & %d, fileranges: [%d - %d]   <----------- Current Server\n", ringMember.serverName, ringMember.hashID, ringMember.successor[0], ringMember.successor[1], low, high)
		} else {
			fmt.Printf("host: %s, ringID: %d, successors: %d & %d, fileranges: [%d - %d] \n", ringMember.serverName, ringMember.hashID, ringMember.successor[0], ringMember.successor[1], low, high)
		}
	}
}

func GetNodeIndex(serverName string) (int, error) {
	for i, member := range ring {
		if member.serverName == serverName {
			return i, nil
		}
	}
	return -1, fmt.Errorf("server %s not found in ring", serverName)
}
