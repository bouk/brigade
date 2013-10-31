package main

import (
	"github.com/bouk/goamz/s3"
	"log"
	"sync/atomic"
)

func (s *S3Connection) fileCopier(finished chan int) {
	for key := range CopyFiles {
		s.copyFile(key)
	}
	log.Printf("Worker finished")
	finished <- 1
}

func (s *S3Connection) copyFile(key string) {
	atomic.AddInt64(&Stats.files, 1)
	atomic.AddInt64(&Stats.working, 1)

	defer func() {
		atomic.AddInt64(&Stats.working, -1)
	}()

	err := s.Destination.PutHeader(key, []byte{}, map[string][]string{
		"x-amz-copy-source": {Config.Source + "/" + key},
	}, s3.PublicRead)

	if err != nil {
		addError(err)
	}
}
