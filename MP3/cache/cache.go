package cache

import (
	"fmt"
	"hydfs/utility"
	"os"
	"sync"
	"time"
)

const MaxCacheSize = 5
const MaxCacheSizeBytes = 100 * 1024 * 1024 // 100 MB

type CachedFileEntry struct {
	Filename    string
	Hash        string
	Timestamp   time.Time
	RingId      uint32
	Filesize    int64
	AccessCount int
}

type Cache struct {
	entries     []*CachedFileEntry
	mutex       sync.RWMutex
	currentSize int64
}

var CachedFileStore *Cache

func init() {
	utility.LogMessage("Initializing Cache with size limit of " + fmt.Sprintf("%d bytes", MaxCacheSizeBytes))
	CachedFileStore = &Cache{
		entries:     make([]*CachedFileEntry, 0),
		currentSize: 0,
	}
}

func AddOrUpdateCacheEntry(filename, hash string, ringId uint32, timeStamp time.Time, filePath string) {
	CachedFileStore.mutex.Lock()
	defer CachedFileStore.mutex.Unlock()

	// Estimate filesize (you may need to implement a way to get the actual filesize)
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		// Handle the error, e.g., file doesn't exist
		return
	}
	filesize := fileInfo.Size()

	// Check if the entry already exists
	for i, entry := range CachedFileStore.entries {
		if entry.Filename == filename {
			// Update existing entry
			CachedFileStore.currentSize -= entry.Filesize
			CachedFileStore.currentSize += filesize
			CachedFileStore.entries[i] = &CachedFileEntry{
				Filename:    filename,
				Hash:        hash,
				Timestamp:   timeStamp,
				RingId:      ringId,
				Filesize:    filesize,
				AccessCount: entry.AccessCount + 1,
			}
			utility.LogMessage("Updated cache entry: " + filename)
			return
		}
	}

	// If the entry doesn't exist, add it
	newEntry := &CachedFileEntry{
		Filename:    filename,
		Hash:        hash,
		Timestamp:   timeStamp,
		RingId:      ringId,
		Filesize:    filesize,
		AccessCount: 1,
	}

	// Remove least used entries until there's enough space
	for CachedFileStore.currentSize+filesize > MaxCacheSizeBytes && len(CachedFileStore.entries) > 0 {
		leastUsedIndex := 0
		for i, entry := range CachedFileStore.entries {
			if entry.AccessCount < CachedFileStore.entries[leastUsedIndex].AccessCount {
				leastUsedIndex = i
			}
		}
		removedEntry := CachedFileStore.entries[leastUsedIndex]
		CachedFileStore.entries = append(CachedFileStore.entries[:leastUsedIndex], CachedFileStore.entries[leastUsedIndex+1:]...)
		CachedFileStore.currentSize -= removedEntry.Filesize
		utility.LogMessage("Removed least used cache entry: " + removedEntry.Filename)
	}

	// Add the new entry
	CachedFileStore.entries = append(CachedFileStore.entries, newEntry)
	CachedFileStore.currentSize += filesize
	utility.LogMessage("Added new cache entry: " + filename)
}

// New method to increment the counter for a specific cache entry
func IncrementCacheEntryCounter(filename string) {
	CachedFileStore.mutex.Lock()
	defer CachedFileStore.mutex.Unlock()

	for i, entry := range CachedFileStore.entries {
		if entry.Filename == filename {
			CachedFileStore.entries[i].AccessCount++
			utility.LogMessage("Incremented access count for: " + filename)
			return
		}
	}
	utility.LogMessage("Cache entry not found for incrementing: " + filename)
}

// GetCacheEntry retrieves a cache entry
func GetCacheEntry(filename string) (*CachedFileEntry, bool) {
	CachedFileStore.mutex.Lock()
	defer CachedFileStore.mutex.Unlock()

	for i, entry := range CachedFileStore.entries {
		if entry.Filename == filename {
			// Increment AccessCount
			CachedFileStore.entries[i].AccessCount++
			utility.LogMessage("Retrieved cache entry for: " + filename)
			return entry, true
		}
	}

	utility.LogMessage("Cache miss for: " + filename)
	return nil, false
}

// RemoveCacheEntry removes a cache entry
func RemoveCacheEntry(filename string) {
	CachedFileStore.mutex.Lock()
	defer CachedFileStore.mutex.Unlock()

	for i, entry := range CachedFileStore.entries {
		if entry.Filename == filename {
			// Remove the entry by slicing
			CachedFileStore.entries = append(CachedFileStore.entries[:i], CachedFileStore.entries[i+1:]...)
			CachedFileStore.currentSize -= entry.Filesize
			utility.LogMessage("Removed cache entry: " + filename)
			return
		}
	}
}

// GetAllCacheEntries returns all cache entries
func GetCacheSize() int64 {
	CachedFileStore.mutex.RLock()
	defer CachedFileStore.mutex.RUnlock()

	utility.LogMessage("Current cache size: " + fmt.Sprintf("%d bytes", CachedFileStore.currentSize))
	return CachedFileStore.currentSize
}
