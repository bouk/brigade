package main

import (
	"github.com/boourns/iq"
	"github.com/bouk/goamz/s3"
	"sync/atomic"
)

func DirManager() {
	iq.SliceIQ(DirCollector, NextDir)
}

func pushDirectory(key string) {
	atomic.AddInt64(&Stats.directories, 1)
	statsUpdated()
	dirworkerGroup.Add(1)
	DirCollector <- key
}

func (s *S3Connection) pushFile(key string) {
	atomic.AddInt64(&Stats.files, 1)
	statsUpdated()
	fileGroup.Add(1)
	go s.copyFileInWaitGroup(key)
}

func (s *S3Connection) dirWorker(quitChannel chan int) {
	for dir := range NextDir {
		s.workDir(dir)
	}
	quitChannel <- 1
}

func (s *S3Connection) workDir(dir string) {
	defer dirworkerGroup.Done()

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
			s.pushFile(key.Key)
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
