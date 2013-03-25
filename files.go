package main

import (
	"github.com/boourns/goamz/s3"
	"log"
  "strconv"
)

func (s *S3Connection) fileCopier(finished chan int) {
	for key := range CopyFiles {
		Stats.files++
		Stats.working++

		source, err := s.SourceBucket.GetResponse(key)
		if err != nil {
			ErrorMutex.Lock()
			Errors.PushBack(err)
			ErrorMutex.Unlock()
			continue
		}

		log.Printf("Copying file %s\n", key)

		if source.Header["Content-Length"] == nil || len(source.Header["Content-Length"]) != 1 {
			log.Printf("Missing Content-Length for key %s\n", key)
			continue
		}

		if source.Header["Content-Type"] == nil || len(source.Header["Content-Type"]) != 1 {
			log.Printf("Missing Content-Type for key %s\n", key)
			continue
		}

		length, err := strconv.ParseInt(source.Header["Content-Length"][0], 10, 64)
		if err != nil {
			ErrorMutex.Lock()
			Errors.PushBack(err)
			ErrorMutex.Unlock()
			continue
		}

		mime := source.Header["Content-Type"][0]

		err = s.DestBucket.PutReader(key, source.Body, length, mime, s3.PublicRead)
		if err != nil {
			Errors.PushBack(err)
			Stats.errors++
		} else {
			Stats.bytes += length
		}

		Stats.working--
	}
	log.Printf("Worker finished")
	finished <- 1
}
