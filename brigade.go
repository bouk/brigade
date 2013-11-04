package main

import (
	"github.com/bouk/goamz/s3"
	"github.com/bouk/priority_iq"
	"github.com/mitchellh/goamz/aws"
	"log"
	"sync/atomic"
)

var (
	PendingDirectories int64

	DirCollector = make(chan priority_iq.Object)
	DirQueue     = make(chan string)
	FileQueue    chan string
)

func addError(err error) {
	atomic.AddInt64(&Stats.errors, 1)
	log.Print(err)
}

type S3Connection struct {
	Connection *s3.S3

	Source      *s3.Bucket
	Destination *s3.Bucket
}

func S3Connect() (*s3.S3, error) {
	auth, err := aws.GetAuth(Config.AccessKey, Config.SecretAccessKey)
	if err != nil {
		return nil, err
	}

	return s3.New(auth, aws.Regions[Config.Region]), nil
}

func S3Init() (*S3Connection, error) {
	connection, err := S3Connect()
	if err != nil {
		return nil, err
	}

	s := &S3Connection{
		Connection:  connection,
		Source:      connection.Bucket(Config.Source),
		Destination: connection.Bucket(Config.Destination),
	}

	return s, nil
}
