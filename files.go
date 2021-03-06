package main

import (
	"github.com/bouk/goamz/s3"
	"log"
)

func (s *S3Connection) fileWorker(number int) {
	for key := range FileQueue {
		if Config.Verbose {
			log.Printf("Fileworker %d started working on %s", number, key)
		}
		s.copyFile(key)
		if Config.Verbose {
			log.Printf("Fileworker %d done with %s", number, key)
		}
		fileGroup.Done()
		if Config.Verbose {
			log.Printf("Fileworker %d receiving from queue", number)
		}
	}
}

func (s *S3Connection) copyFile(key string) {
	err := s.Destination.PutHeader(key, []byte{}, map[string][]string{
		"x-amz-copy-source": {Config.Source + "/" + key},
	}, s3.PublicRead)

	if err != nil {
		addError(err)
	}
}
