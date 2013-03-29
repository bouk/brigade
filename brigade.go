package main

import (
	"container/list"
	"github.com/boourns/goamz/s3"
	"launchpad.net/goamz/aws"
	"log"
	"sync"
)

var Errors *list.List
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

var fileWorker []*S3Connection
var dirWorker []*S3Connection

var quitChannel chan int

func Init() {
	Errors = list.New()

	CopyFiles = make(chan string, 1000)
	DeleteFiles = make(chan string, 100)

	quitChannel = make(chan int)
	fileWorker = make([]*S3Connection, Config.FileWorkers)
	dirConnections = make(chan *S3Connection, Config.DirWorkers)

	// spawn workers
	log.Printf("Spawning %d file workers", Config.FileWorkers)

	for i := 0; i < Config.FileWorkers; i++ {
		fileWorker[i] = S3Init()
		go fileWorker[i].fileCopier(quitChannel)
	}

	// N directory workers
	for i := 0; i < Config.DirWorkers; i++ {
		dirConnections<-S3Init()
	}
}

func (s *S3Connection) CopyBucket() {
  go CopyDirectory("")

	printStats()
	s.Shutdown()
}

func (s *S3Connection) Shutdown() {
	log.Printf("Shutting down..")
	close(CopyFiles)

	finished := 0
	for finished < Config.FileWorkers {
		finished += <-quitChannel
		log.Printf("File Worker quit..")
	}

	finished = 0
	for finished < Config.DirWorkers {
		finished += <-quitChannel
		log.Printf("Dir Worker quit..")
	}

	log.Printf("Final stats:")
	printStats()

	if Errors.Len() > 0 {
		log.Printf("%v Errors:", Errors.Len())
		for Errors.Len() > 0 {
			log.Printf("%v", Errors.Remove(Errors.Front()))
		}
	}
}
