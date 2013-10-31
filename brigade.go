package main

import (
	"github.com/bouk/goamz/s3"
	"github.com/mitchellh/goamz/aws"
	"log"
	"sync"
	"sync/atomic"
)

var (
	Errors     []error
	ErrorMutex sync.RWMutex

	PendingDirectories int64

	DirCollector = make(chan string)
	DirQueue     = make(chan string)
	FileQueue    chan string
)

func addError(err error) {
	atomic.AddInt64(&Stats.errors, 1)
	ErrorMutex.Lock()
	defer ErrorMutex.Unlock()
	Errors = append(Errors, err)
}

func printErrors() {
	ErrorMutex.RLock()
	defer ErrorMutex.RUnlock()

	if len(Errors) > 0 {
		log.Printf("%v Errors:", len(Errors))
		for _, err := range Errors {
			log.Print(err.Error())
		}
	}
}

type S3Connection struct {
	Connection *s3.S3

	Source      *s3.Bucket
	Destination *s3.Bucket
}

func S3Connect() (*s3.S3, error) {
	auth := aws.Auth{Config.AccessKey, Config.SecretAccessKey, ""}
	endpoint, err := Config.Endpoint()
	if err != nil {
		return nil, err
	}

	return s3.New(auth, aws.Region{S3Endpoint: endpoint}), nil
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
