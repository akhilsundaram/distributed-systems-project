package ring

import (
	"fmt"
	"hydfs/membership"
	"hydfs/utility"
	"log"
	"net"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	grpc "google.golang.org/grpc"
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
	port      = "5050"
)

/*Initialize a new server for hydfs ring.*/
func StartRing() {
	// Get current server name
	// Get current member list
	// Construct Ring
	ringNodes = make(map[uint32]int)

	// Start file rpc server for ring
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("Failed to listen on port: %v", err)
	}
	server := grpc.NewServer()
	RegisterFileServiceServer(server, &FileServer{})
	go func() {
		utility.LogMessage("RPC server goroutine entered")
		if err := server.Serve(listener); err != nil {
			utility.LogMessage("Init Ring -  start fserver failure")
		}
	}()

	//End file server start
	time.Sleep(2 * time.Second)
	initRing()

	for {
		membership_change := <-membership.RingMemberchan
		UpdateRingMemeber(membership_change.NodeName, membership_change.Event)
	}
}

// AddMember to ring
func initRing() {
	utility.LogMessage("Init ring started")
	members_list := membership.GetMembershipList()
	// current_node_index := 0

	ringLock.Lock()
	for key := range members_list {
		var ring_member ringMember
		ring_member.hashID = utility.Hashmurmur(key)
		ring_member.serverName = key
		ring = append(ring, ring_member)

		ringNodes[ring_member.hashID] = 0
	}
	// go sort based on hashID
	sort.Slice(ring, func(i, j int) bool {
		return ring[i].hashID < ring[j].hashID
	})
	utility.LogMessage("after sorted")
	n := len(ring)
	for i := 0; i < n; i++ {
		ring[i].successor = make([]uint32, 0, 2)
		for j := 1; j <= 2; j++ {
			ring[i].successor = append(ring[i].successor, ring[(i+j)%n].hashID)
		}
	}
	utility.LogMessage("Ring init done!")
	ringLock.Unlock()

	//Pull data from previous node.
	//Make a call to file server that reqs files from numbers [x,y] inclusive.

	low_self, high_self, _ := findFileRanges(membership.My_hostname)
	node_idx, _ := GetNodeIndex(membership.My_hostname)
	num := ((node_idx-replicas)%len(ring) + len(ring)) % len(ring)

	utility.LogMessage("pull files in init start")
	for i := 0; i < replicas*2; i++ {
		n := (num + i) % len(ring)
		if ring[n].serverName != membership.My_hostname {
			utility.LogMessage("Trying to pull from - " + ring[n].serverName)
			utility.LogMessage("with ranges - [" + strconv.FormatUint(uint64(low_self), 10) + "," + strconv.FormatUint(uint64(high_self), 10))
			pullFiles(low_self, high_self, ring[n].serverName)
		}
	}

	// pullFiles(ring[((current_node_index-1)%len(ring)+len(ring))%len(ring)].hashID, ring[(current_node_index+1)%len(ring)].hashID, ring[(current_node_index+1)%len(ring)].serverName)
	// // Pull data/files on node from predecessor at node init. // //Make a call to file server that reqs files from numbers [x,y] inclusive. //
	// utility.LogMessage("pull files in init end")
	// //Pull replica files into your system
	// num := ((current_node_index-replicas)%len(ring) + len(ring)) % len(ring)
	// for i := 0; i < replicas-1; i++ {
	// 	utility.LogMessage("more pull files in init start - loop")
	// 	utility.LogMessage("Trying to pull from - " + ring[(num+i+1)%len(ring)].serverName)
	// 	pullFiles(ring[(num+i)%len(ring)].hashID, ring[(num+i+1)%len(ring)].hashID, ring[(num+i+1)%len(ring)].serverName)
	// } // can add later - failure to find node/ we can retry to get the files from successor of this node.

	// //Add logic to event to pull replica data ? or a push based on add ??
	// Always called when you're the new node in the system
	// no nodes/ first node in the system
}

func UpdateRingMemeber(node string, action membership.MemberState) error {
	hash_value_of_node := utility.Hashmurmur(node)
	switch action {
	case membership.Add: // add to ring
		if nodeInRing(node) {
			return fmt.Errorf("error - Node %v already in Ring! cannot add", node)
		}
		//Add to hashmap

		utility.LogMessage("Signal to add New node -" + node)
		var ring_member ringMember
		ring_member.hashID = hash_value_of_node
		ring_member.serverName = node
		AddRingMember(ring_member)

		utility.LogMessage("Node added to ring")
		//Update successor for 3 nodes, newly inserted node and two before it ^^ done in addring member.

		low, high, err := findFileRanges(membership.My_hostname)
		if err != nil {
			utility.LogMessage("error deleting files not in range - " + err.Error())
		}
		dropFilesNotInRange(low, high)
		utility.LogMessage("Dropping files out of range - [" + strconv.FormatUint(uint64(low), 10) + "," + strconv.FormatUint(uint64(high), 10) + "]")

		//if we're part of two (num_replicas - 1) nodes after, drop data replica after a while.
		// CHANGE THIS AND KEEP TRACK OF THE FILE RANGES WE NEED TO STORE! USE THIS TO DROP FILES OUTSIDE RANGE. BETTER
		// for i := 1; i < replicas; i++ {
		// 	if ring[(insertion+i)%len(ring)].serverName == membership.My_hostname {
		// 		num := ((insertion+i-replicas-1)%len(ring) + len(ring)) % len(ring)
		// 		dropFiles(ring[num%len(ring)].hashID, ring[(num+1)%len(ring)].hashID)
		// 	}
		// }
		//if we're part of the two nodes before, we need to replicate data to new node, but this should be pulled from init not here. //NOT done in init
		return nil

	case membership.Delete: // remove from ring
		if !nodeInRing(node) {
			return fmt.Errorf("error - Node %v not in Ring! cannot delete", node)
		}
		//Delete map entry of node
		deletion, err := DeleteRingMember(node)
		if err != nil {
			utility.LogMessage("node deletion in ring errored - " + err.Error())
		}

		deletion = deletion % len(ring) //ensure stable next index of deleted node.

		// The two successors of a deleted element will replicate one node further in.
		// Node right after deleted node, (at idx deletion%len(ring)th position and two nodes after that will have files added in their replication.
		for i := 0; i < replicas; i++ {
			num := (deletion + i) % len(ring)
			if ring[num].serverName == membership.My_hostname {
				low, high, _ := findFileRanges(membership.My_hostname)
				for j := 1; j < replicas; j++ {
					pull_from := (num - j + len(ring)) % len(ring)
					utility.LogMessage("PULL ON DELETE -from: " + ring[(num-j+len(ring))%len(ring)].serverName + "to: " + membership.My_hostname)
					utility.LogMessage("with ranges - [" + strconv.FormatUint(uint64(low), 10) + "," + strconv.FormatUint(uint64(high), 10))
					pullFiles(low, high, ring[pull_from].serverName)
				}
			}
		}

		// partial optimization - do later. In the 3rd node after deleted node, we only need to pull a partial subset of nodes instead of everything,
		// But we can handle it in pullfiles, NO NEED HERE
		// if ring[(deletion+replicas-1)%len(ring)].serverName == membership.My_hostname {
		// 	num := ((deletion-1)%len(ring) + len(ring)) % len(ring)
		// 	pullFiles(ring[num].hashID, hash_value_of_node, ring[deletion%len(ring)].serverName)
		// }
		return nil

	}
	return nil
}

func nodeInRing(node string) bool {
	if _, exists := ringNodes[utility.Hashmurmur(node)]; exists {
		return true
	} else {
		return false
	}

}

// Get Successor node for a file
func GetFileNodes(filename string) []string {
	hash_of_file := utility.Hashmurmur(filename)
	idx := -1 // negative to indicate init
	var output []string

	for i := 0; i < len(ring); i++ {
		if hash_of_file > ring[i].hashID {
			continue
		} else {
			idx = i
			break
		}
	}
	if idx == -1 && ring[len(ring)-1].hashID < hash_of_file { // still init value, and the last node in ring is still < hash_of_node, wrap around.
		idx = 0
	}

	for i := 0; i < replicas; i++ {
		utility.LogMessage(ring[(idx+i)%len(ring)].serverName)
		output = append(output, ring[(idx+i)%len(ring)].serverName)
	}

	return output
}

// To move to file_transfer
func handleDelete(filename string) {
	//Path to file may need to change : HDFS_URL + filename

	// Delete the file
	err := os.Remove(utility.HYDFS_DIR + "/" + filename)
	if err != nil {
		utility.LogMessage("File does not exist")
		return
	}
	utility.DeleteMetadata(filename)
	// delete(utility.HydfsFileStore, filename)

}

// func getFilesFromServer() {

// }

func getFileList(low uint32, high uint32) []string {
	var file_list []string
	hydfsFS := utility.GetAllHyDFSMetadata()
	if low > high {
		for filename, metadata := range hydfsFS {
			if low < metadata.RingId && metadata.RingId <= 1023 || (0 <= metadata.RingId && metadata.RingId <= high) {
				file_list = append(file_list, filename)
			}
		}
	} else {
		for filename, metadata := range hydfsFS {
			if low < metadata.RingId && metadata.RingId <= high {
				file_list = append(file_list, filename)
			}
		}
	}

	return file_list
}

func hashwithinRange(value uint32, low uint32, high uint32) bool {
	if low > high {
		if low < value && value <= 1023 {
			return true
		}
		if low <= value && value <= high {
			return true
		}
	} else {
		if low < value && value <= high {
			return true
		}
	}
	return false
}

// case "get_files_in_range":
// 	low_range := parsedData.RangeRingID[0]
// 	high_range := parsedData.RangeRingID[1]

// 	output_list := GetFileList(low_range, high_range)

// 	jsonBytesOutput, err := json.Marshal(output_list)
// 	if err != nil {
// 		utility.LogMessage("json marshal error - hydfs file server - get_files_in_range")
// 	}

// 	resp.Data = jsonBytesOutput

// // Get file
// func GetFileList(low uint32, high uint32) []string {
// 	var file_list []string
// 	if low > high {
// 		for filename, metadata := range HydfsFileStore {
// 			if low < metadata.RingId && metadata.RingId <= 1023 {
// 				file_list = append(file_list, filename)
// 			}
// 			if low <= metadata.RingId && metadata.RingId <= high {
// 				file_list = append(file_list, filename)
// 			}
// 		}
// 	} else {
// 		for filename, metadata := range HydfsFileStore {
// 			if low < metadata.RingId && metadata.RingId <= high {
// 				file_list = append(file_list, filename)
// 			}
// 		}
// 	}

// 	return file_list
// }
