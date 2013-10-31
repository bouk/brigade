package main

import (
	"github.com/bouk/goamz/s3"
)

func (s *S3Connection) copyFileInWaitGroup(key string) {
	defer fileGroup.Done()
	s.copyFile(key)
}

func (s *S3Connection) copyFile(key string) {
	err := s.Destination.PutHeader(key, []byte{}, map[string][]string{
		"x-amz-copy-source": {Config.Source + "/" + key},
	}, s3.PublicRead)

	if err != nil {
		addError(err)
	}
}
