package main

import (
	"github.com/bouk/goamz/s3"
	"log"
	"strconv"
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

	source, err := s.SourceBucket.GetResponse(key)
	if err != nil {
		addError(err)
		return
	}
	defer source.Body.Close()

	if source.Header["Content-Length"] == nil || len(source.Header["Content-Length"]) != 1 {
		log.Printf("Missing Content-Length for key %s\n", key)
		return
	}

	if source.Header["Content-Type"] == nil || len(source.Header["Content-Type"]) != 1 {
		log.Printf("Missing Content-Type for key %s\n", key)
		return
	}

	length, err := strconv.ParseInt(source.Header["Content-Length"][0], 10, 64)
	if err != nil {
		addError(err)
		return
	}

	mime := source.Header["Content-Type"][0]

	err = s.DestBucket.PutReader(key, source.Body, length, mime, s3.PublicRead)
	if err != nil {
		addError(err)
	} else {
		atomic.AddInt64(&Stats.bytes, length)
	}
}
