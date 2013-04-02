package main

import (
	"container/list"
	"errors"
	"fmt"
	"github.com/shopify/stats/env"
	"github.com/tobi/airbrake-go"
	"log"
	"os"
)

var (
	fileWorker         []*S3Connection
	dirWorker          []*S3Connection
	PendingDirectories int

	dirConnections     chan *S3Connection
	dirWorkersFinished chan int
	quitChannel        chan int
)

func init() {
	CopyFiles = make(chan string, 1000)
	DeleteFiles = make(chan string, 100)

	quitChannel = make(chan int)
	dirWorkersFinished = make(chan int)
}

func main() {
	airbrake.Endpoint = "https://exceptions.shopify.com/notifier_api/v2/notices.xml"
	airbrake.ApiKey = "795dbf40b8743457f64fe9b9abc843fa"

	if len(env.Get("log")) > 0 {
		logFile, err := os.OpenFile(env.Get("log"), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			err = errors.New(fmt.Sprintf("Could not open log file %s for writing: %s", env.Get("log"), err.Error()))
			airbrake.Error(err, nil)
			log.Fatal(err)
		}
		log.SetOutput(logFile)
		defer logFile.Close()
	}

	log := 300

	if log > 0 {
		go statsWorker(log)
	}

	readConfig()

	Initialize()
	CopyDirectory("")

	<-dirWorkersFinished
	shutdown()
}

func Initialize() {
	fileWorker = make([]*S3Connection, Config.FileWorkers)
	dirConnections = make(chan *S3Connection, Config.DirWorkers)

	Errors = list.New()

	// spawn workers
	log.Printf("Spawning %d file workers", Config.FileWorkers)

	for i := 0; i < Config.FileWorkers; i++ {
		fileWorker[i] = S3Init()
		go fileWorker[i].fileCopier(quitChannel)
	}

	// N directory workers
	for i := 0; i < Config.DirWorkers; i++ {
		dirConnections <- S3Init()
	}
}

func shutdown() {
	log.Printf("Shutting down..")
	close(CopyFiles)

	finished := 0
	for finished < Config.FileWorkers {
		finished += <-quitChannel
		log.Printf("File Worker quit..")
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
