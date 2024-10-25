package hydfs

import (
	"fmt"
	"hydfs/membership"
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
	}
	ringLock.Unlock()
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
		//if we're part of the two nodes before, we need to replicate data to new node, but this should be pulled from init not here.
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

// Hashing function
func Hashmurmur(name string) uint32 {
	return murmur3.Sum32([]byte(name)) % 1024
}
