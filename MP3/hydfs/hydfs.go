package hydfs

import (
	"fmt"
	"hydfs/file_transfer"
	"hydfs/membership"
	"hydfs/utility"
	"os"
	"sort"
	"sync"

	"github.com/spaolacci/murmur3"
)

type ringMember struct {
	serverName string
	hashID     uint32
	successor  []uint32 //store the location of replicas. Can use it later on deletes etc
}

var (
	ring      []ringMember
	ringNodes map[uint32]int
	replicas  = 3
	ringLock  sync.Mutex
)

/*Initialize a new server for hydfs ring.*/
func StartHyDFS() {
	// Get current server name
	// Get current member list
	// Construct Ring
	initRing()

	for {
		membership_change := <-membership.RingMemberchan
		UpdateRingMemeber(membership_change.NodeName, membership_change.Event)
	}
}

// AddMember to ring
func initRing() {
	members_list := membership.GetMembershipList()
	current_node_index := 0

	ringLock.Lock()
	for key := range members_list {
		var ring_member ringMember
		ring_member.hashID = Hashmurmur(key)
		ring_member.serverName = key
		ring = append(ring, ring_member)

		ringNodes[ring_member.hashID] = 0
	}
	// go sort based on hashID
	sort.Slice(ring, func(i, j int) bool {
		return ring[i].hashID < ring[j].hashID
	})
	for i := 0; i < len(ring); i++ {
		for j := 0; i < replicas-1; j++ {
			ring[i].successor = append(ring[i].successor, ring[i+j%len(ring)].hashID)
		}
		if ring[i].serverName == membership.My_hostname {
			current_node_index = i
		}
	}
	ringLock.Unlock()

	//Pull data from previous node.
	//Make a call to file server that reqs files from numbers [x,y] inclusive.
	pullFiles(ring[((current_node_index-1)%len(ring)+len(ring))%len(ring)].hashID, ring[current_node_index+1%len(ring)].hashID, ring[current_node_index+1%len(ring)].serverName)
	// Pull data/files on node from predecessor at node init. // //Make a call to file server that reqs files from numbers [x,y] inclusive. //

	//Pull replica files into your system
	num := ((current_node_index-replicas)%len(ring) + len(ring)) % len(ring)
	for i := 0; i < replicas-1; i++ {
		pullFiles(ring[num+i].hashID, ring[(num+i+1)%len(ring)].hashID)
	}

	//Add logic to event to pull replica data ? or a push based on add ??
	// Always called when you're the new node in the system
	// no nodes/ first node in the system
}

func UpdateRingMemeber(node string, action membership.MemberState) error {
	hash_value_of_node := Hashmurmur(node)
	switch action {
	case membership.Add: // add to ring
		if nodeInRing(node) {
			return fmt.Errorf("error - Node %v already in Ring! cannot add", node)
		}
		insertion := 0
		ringLock.Lock()
		for i := 0; i < len(ring); i++ { // len == 0 cannot happen because 0 members mean we're dead too.
			if ring[i].hashID < hash_value_of_node && i != len(ring)-1 { // Unless it's the last element, then do the same insertion at the end.
				continue
			}
			// Create ring element
			var ring_member ringMember
			ring_member.hashID = hash_value_of_node
			ring_member.serverName = node
			ring = append(ring[:i], append([]ringMember{ring_member}, ring[i:]...)...)
			insertion = i + 1
			break
		}

		//two nodes behind -  need to change successor
		num := ((insertion-replicas)%len(ring) + len(ring)) % len(ring)
		for c := 0; c < replicas; c++ {
			ring[(num+c)%len(ring)].successor = []uint32{ring[(num+c+1)%len(ring)].hashID, ring[(num+c+2)%len(ring)].hashID}
		}
		ringLock.Unlock()

		//if we're part of two (num_replicas - 1) nodes after, drop data replica after a while.
		for i := 1; i < replicas; i++ {
			if ring[(insertion+i)%len(ring)].serverName == membership.My_hostname {
				num := ((insertion+i-replicas-1)%len(ring) + len(ring)) % len(ring)
				dropFiles(ring[num%len(ring)].hashID, ring[(num+1)%len(ring)].hashID)
			}
		}
		//if we're part of the two nodes before, we need to replicate data to new node, but this should be pulled from init not here. //NOT done in init
		return nil

	case membership.Delete: // remove from ring
		if !nodeInRing(node) {
			return fmt.Errorf("error - Node %v not in Ring! cannot delete", node)
		}
		deletion := 0
		ringLock.Lock()
		for i := 0; i < len(ring); i++ {
			if ring[i].hashID != hash_value_of_node {
				continue
			}
			// Delete ring element
			ring = append(ring[:i], ring[i+1:]...)
			deletion = i
			break
		}
		num := ((deletion-replicas+1)%len(ring) + len(ring)) % len(ring)
		for c := 0; c < replicas-1; c++ {
			ring[(num+c)%len(ring)].successor = []uint32{ring[(num+c+1)%len(ring)].hashID, ring[(num+c+2)%len(ring)].hashID}
		}
		ringLock.Unlock()

		// The two successors of a deleted element will replicate one node further in.
		// 1st successor of deleted node will have more files assigned to it. The replicas of this node will also need to get these files
		for i := 0; i < replicas-1; i++ {
			// I'm the next node
			if ring[(deletion+i)%len(ring)].serverName == membership.My_hostname {
				num := ((deletion+i-replicas)%len(ring) + len(ring)) % len(ring)
				pullFiles(ring[num].hashID, ring[(num+1)%len(ring)].hashID)
			}
		}

		// deletion index has more nodes, so one extra replica needs to pull this.
		if ring[(deletion+replicas-1)%len(ring)].serverName == membership.My_hostname {
			num := ((deletion-1)%len(ring) + len(ring)) % len(ring)
			pullFiles(ring[num].hashID, hash_value_of_node)
		}
		return nil

	}
	return nil
}

func nodeInRing(node string) bool {
	if _, exists := ringNodes[Hashmurmur(node)]; exists {
		return true
	} else {
		return false
	}

}

// Get Successor node for a file
func GetFileNodes(filename string) []string {
	hash_of_file := Hashmurmur(filename)
	idx := 0
	var output []string

	for i := 0; i < len(ring); i++ {
		if hash_of_file > ring[i].hashID {
			continue
		} else {
			idx = i
		}
	}

	for i := idx; i < idx+replicas; i++ {
		output = append(output, ring[i].serverName)
	}

	return output
}

// Hashing function
func Hashmurmur(name string) uint32 {
	return murmur3.Sum32([]byte(name)) % 1024
}

// Ask node to drop the file list - called when it gets { a files req from a newly added node } OR {sees newly added node and asks the replica + 1th node to drop files which won't be part of added node's hash }
func dropFiles(low uint32, high uint32) {
	delete_list := getFileList(low, high)
	for _, filename := range delete_list {
		//change to file_transfer function
		handleDelete(filename)
	}
}

// To move to file_transfer
func handleDelete(filename string) {
	//Path to file may need to change : HDFS_URL + filename

	// Delete the file
	err := os.Remove(file_transfer.HYDFS_DIR + "/" + filename)
	if err != nil {
		utility.LogMessage("File does not exist")
		return
	}

}

func pullFiles(low uint32, high uint32, server ...string) {
	// IF we have files within this range already -----> add a data struct for this if not there.
	//Don't do anything
	//Else
	// Pull from the correct replicas (for now, 1 call, later to all replicas) -----> pull from server if the var was passed, else usual hashcheck
	self_list := getFileList(low, high)

	// Get file list for the ranges in the other servers.
	var remote_list []string
	if len(server) == 0 {
		// Get File from server func - wait for anurag to rewrite this.
		remote_list = append(remote_list, "") // get list from server[0]
	} else {
		// Get list of servers with the hash ids of that range
		idx := 0
		for i := 0; i < len(ring); i++ {
			if low > ring[i].hashID {
				continue
			} else {
				idx = i
			}
		}
		remote_list = append(remote_list, "") // get list from servers
	}

	//Find the correct server to send data to. difference in lists.
	map_self_list := make(map[string]struct{}, len(self_list))
	diff := []string{}
	for _, file := range self_list {
		map_self_list[file] = struct{}{}
	}
	for _, v := range remote_list {
		if _, found := map_self_list[v]; !found {
			diff = append(diff, v)
		}
	}

	for _, file_to_get := range diff {
		//Call get file for "file_to_get"
		fmt.Printf("file_to_get: %v\n", file_to_get)
	}

}

func getFileList(low uint32, high uint32) []string {
	var file_list []string
	for filename, metadata := range file_transfer.HydfsFileStore {
		if low < metadata.RingId && metadata.RingId <= high {
			file_list = append(file_list, filename)
		}
	}
	return file_list
}
