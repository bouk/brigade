package main

import (
	"github.com/boourns/goamz/s3"
	"github.com/boourns/iq"
	"log"
)

var DirCollector chan string
var NextDir chan string

func dirManager() {
	iq.SliceIQ(DirCollector, NextDir)
}

func (s *S3Connection) CopyDirectory(dir string) error {
	Stats.directories++

	sourceList, err := s.SourceBucket.List(dir, "/", "", 1000)
	if err != nil {
		return err
	}

	destList, err := s.DestBucket.List(dir, "/", "", 1000)
	if err != nil {
		return err
	}

	// push subdirectories onto directory queue
	for i := 0; i < len(sourceList.CommonPrefixes); i++ {
		log.Printf("Pushing dir")
		DirCollector <- sourceList.CommonPrefixes[i]
	}

	// push subdirectories that no longer exist onto queue
	for i := 0; i < len(destList.CommonPrefixes); i++ {
		if !inList(destList.CommonPrefixes[i], sourceList.CommonPrefixes) {
			DirCollector <- destList.CommonPrefixes[i]
		}
	}

	// push changed files onto file queue
	for i := 0; i < len(sourceList.Contents); i++ {
		key := sourceList.Contents[i]
		existing, found := findKey(key.Key, destList)
		if !found || keyChanged(key, existing) {
			CopyFiles <- key.Key
		}
	}

	// push removed files onto delete list
	for i := 0; i < len(destList.Contents); i++ {
		key := destList.Contents[i]
		_, found := findKey(key.Key, sourceList)
		if !found {
			DeleteFiles <- key.Key
		}
	}

	return nil
}

func (s *S3Connection) dirCopier(finished chan int) {
	for dir := range NextDir {
		err := s.CopyDirectory(dir)
		if err != nil {
			ErrorMutex.Lock()
			Errors.PushBack(err)
			ErrorMutex.Unlock()
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

var nilKey s3.Key

func findKey(name string, list *s3.ListResp) (s3.Key, bool) {
	for i := 0; i < len(list.Contents); i++ {
		if list.Contents[i].Key == name {
			return list.Contents[i], true
		}
	}
	return nilKey, false
}
