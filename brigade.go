package main

import (
	"github.com/boourns/goamz/s3"
	"launchpad.net/goamz/aws"
	"log"
	"sync"
)

var Errors []error
var ErrorMutex sync.Mutex

var CopyFiles chan string
var DeleteFiles chan string

type S3Connection struct {
	Source       *s3.S3
	Dest         *s3.S3
	SourceBucket *s3.Bucket
	DestBucket   *s3.Bucket
}

func S3Connect(t *Target) *s3.S3 {
	if t == nil {
		log.Fatalf("Target was not loaded")
	}

	auth := aws.Auth{t.AccessKey, t.SecretAccessKey}
	return s3.New(auth, aws.Region{S3Endpoint: t.Server})
}

func S3Init() *S3Connection {
	s := &S3Connection{S3Connect(Config.Source), S3Connect(Config.Dest), nil, nil}

	if s.Source == nil {
		log.Fatalf("Could not connect to S3 endpoint %s", Config.Source.Server)
	}

	if s.Dest == nil {
		log.Fatalf("Could not connect to S3 endpoint %s", Config.Dest.Server)
	}

	s.SourceBucket = s.Source.Bucket(Config.Source.BucketName)
	s.DestBucket = s.Source.Bucket(Config.Dest.BucketName)

	return s
}
