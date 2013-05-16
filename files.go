package main

import (
	"github.com/boourns/goamz/s3"
	"log"
	"strconv"
)

func (s *S3Connection) fileCopier(finished chan int) {
	for key := range CopyFiles {
		s.copyFile(key)
	}
	log.Printf("Worker finished")
	finished <- 1
}

func (s *S3Connection) copyFile(key string) {
	Stats.files++
	Stats.working++
	defer func() { Stats.working-- }()

	source, err := s.SourceBucket.GetResponse(key)
	if err != nil {
		addError(err)
		return
	}

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
		Stats.bytes += length
	}
}
