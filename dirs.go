package main

import (
	"github.com/boourns/iq"
	"github.com/bouk/goamz/s3"
	"log"
	"sync/atomic"
)

func DirManager() {
	iq.SliceIQ(DirCollector, DirQueue)
}

func pushDirectory(key string) {
	atomic.AddInt64(&Stats.directories, 1)
	statsUpdated()
	dirworkerGroup.Add(1)
	log.Printf("Pushed directory %s", key)
	DirCollector <- key
}

func (s *S3Connection) pushFile(key s3.Key) {
	atomic.AddInt64(&Stats.files, 1)
	atomic.AddInt64(&Stats.bytes, key.Size)
	statsUpdated()
	fileGroup.Add(1)
	log.Printf("Starting transfer for %s", key.Key)
	FileQueue <- key.Key
}

func (s *S3Connection) dirWorker(number int) {
	for dir := range DirQueue {
		log.Printf("Dirworker %d started working on %s", number, dir)
		s.workDir(dir)
		dirworkerGroup.Done()
	}
}

func (s *S3Connection) workDir(dir string) {
	sourceList, err := s.Source.List(dir, "/", "", 1000)
	if err != nil {
		addError(err)
		return
	}

	destList, err := s.Destination.List(dir, "/", "", 1000)
	if err != nil {
		addError(err)
		return
	}

	// push changed files onto file queue
	for i := 0; i < len(sourceList.Contents); i++ {
		key := sourceList.Contents[i]
		existing, found := findKey(key.Key, destList)
		if !found || keyChanged(key, existing) {
			s.pushFile(key)
		}
	}

	// push subdirectories onto directory queue
	for i := 0; i < len(sourceList.CommonPrefixes); i++ {
		pushDirectory(sourceList.CommonPrefixes[i])
	}

	// push subdirectories that no longer exist onto queue
	for i := 0; i < len(destList.CommonPrefixes); i++ {
		if !inList(destList.CommonPrefixes[i], sourceList.CommonPrefixes) {
			pushDirectory(destList.CommonPrefixes[i])
		}
	}
}

func keyChanged(src s3.Key, dest s3.Key) bool {
	return src.Size != dest.Size || src.ETag != dest.ETag || src.StorageClass != dest.StorageClass
}

func inList(input string, list []string) bool {
	for i := 0; i < len(list); i++ {
		if input == list[i] {
			return true
		}
	}
	return false
}

func findKey(name string, list *s3.ListResp) (s3.Key, bool) {
	for i := 0; i < len(list.Contents); i++ {
		if list.Contents[i].Key == name {
			return list.Contents[i], true
		}
	}
	return s3.Key{}, false
}
