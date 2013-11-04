package main

import (
	"github.com/bouk/goamz/s3"
	"github.com/bouk/priority_iq"
	"log"
	"strings"
	"sync/atomic"
)

func DirManager() {
	priority_iq.HeapIQ(DirCollector, DirQueue)
}

func pushDirectory(key string) {
	atomic.AddInt64(&Stats.directories, 1)
	statsUpdated()
	dirworkerGroup.Add(1)
	if Config.Verbose {
		log.Printf("Pushed directory %s", key)
	}

	// Put the directories in the priority queue by key so deeper directories get worked on first
	DirCollector <- priority_iq.Object{strings.Count(key, "/"), key}
}

func pushFile(key s3.Key) {
	atomic.AddInt64(&Stats.files, 1)
	atomic.AddInt64(&Stats.bytes, key.Size)
	statsUpdated()
	fileGroup.Add(1)
	if Config.Verbose {
		log.Printf("Starting transfer for %s", key.Key)
	}
	FileQueue <- key.Key
}

func (s *S3Connection) dirWorker(number int) {
	for dir := range DirQueue {
		if Config.Verbose {
			log.Printf("Dirworker %d started working on %s", number, dir)
		}
		s.workDir(dir)
		dirworkerGroup.Done()
	}
}

func (s *S3Connection) workDir(dir string) {
	sourceFiles, sourceDirectories, err := listAllFiles(dir, s.Source)
	if err != nil {
		addError(err)
		return
	}

	destinationFiles, destinationDirectories, err := listAllFiles(dir, s.Destination)
	if err != nil {
		addError(err)
		return
	}

	destinationFileMap := make(map[string]*s3.Key)
	for _, key := range destinationFiles {
		destinationFileMap[key.Key] = &key
	}

	sourceDirectoryMap := make(map[string]bool)
	for _, directory := range sourceDirectories {
		sourceDirectoryMap[directory] = true
	}

	for _, sourceKey := range sourceFiles {
		destinationKey, ok := destinationFileMap[sourceKey.Key]
		if !ok || keyChanged(&sourceKey, destinationKey) {
			pushFile(sourceKey)
		}
	}

	// push subdirectories onto directory queue
	for _, directory := range sourceDirectories {
		pushDirectory(directory)
	}

	// push subdirectories that no longer exist onto queue (so the files in them can be deleted)
	for _, directory := range destinationDirectories {
		if !sourceDirectoryMap[directory] {
			pushDirectory(directory)
		}
	}
}

// List all the files in a bucket, taking in mind that a file list might be truncated
func listAllFiles(dir string, bucket *s3.Bucket) (files []s3.Key, directories []string, err error) {
	list, err := bucket.List(dir, "/", "", 1000)
	if err != nil {
		return
	}
	files = list.Contents
	directories = list.CommonPrefixes
	for list.IsTruncated {
		list, err = bucket.List(dir, "/", list.Marker, 1000)
		if err != nil {
			return
		}
		files = append(files, list.Contents...)
		directories = append(directories, list.CommonPrefixes...)
	}
	return
}

func keyChanged(src *s3.Key, dest *s3.Key) bool {
	return src.ETag != dest.ETag || src.LastModified != dest.LastModified || src.StorageClass != dest.StorageClass
}
