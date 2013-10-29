package main

import (
	"errors"
	"fmt"
	"github.com/tobi/airbrake-go"
	"log"
	"os"
	"time"
)

var (
	fileWorker         []*S3Connection
	dirWorker          []*S3Connection
	PendingDirectories int

	DirCollector chan string
	NextDir      chan string

	dirWorkersFinished chan int
	fileWorkerQuit     chan int
	dirWorkerQuit      chan int
)

func init() {
	CopyFiles = make(chan string, 1000)
	DeleteFiles = make(chan string, 100)

	fileWorkerQuit = make(chan int)
	dirWorkerQuit = make(chan int)

	dirWorkersFinished = make(chan int)

	DirCollector = make(chan string)
	NextDir = make(chan string)
}

func main() {

	if len(os.Getenv("log")) > 0 {
		logFile, err := os.OpenFile(os.Getenv("log"), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			err = errors.New(fmt.Sprintf("Could not open log file %s for writing: %s", os.Getenv("log"), err.Error()))
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

	setup()

	PendingDirectories += 1
	DirCollector <- ""

	<-dirWorkersFinished
	shutdown()
}

func setup() {
	fileWorker = make([]*S3Connection, Config.FileWorkers)
	dirWorker = make([]*S3Connection, Config.DirWorkers)

	// spawn workers
	log.Printf("Spawning %d file workers", Config.FileWorkers)

	for i := 0; i < Config.FileWorkers; i++ {
		fileWorker[i] = S3Init()
		go fileWorker[i].fileCopier(fileWorkerQuit)
	}

	// 1 worker for the directory queue manager
	go DirManager()

	// N directory workers
	for i := 0; i < Config.DirWorkers; i++ {
		dirWorker[i] = S3Init()
		go dirWorker[i].dirWorker(dirWorkerQuit)
	}
}

func kill() {
	// force death if shutting down takes > 20 minutes
	time.Sleep(20 * time.Minute)
	log.Printf("Shutdown took more than 20 minutes, forcing exit")
	finale()
	os.Exit(1)
}

func finale() {
	log.Printf("Final stats:")
	printStats()

	ErrorMutex.Lock()
	if len(Errors) > 0 {
		log.Printf("%v Errors:", len(Errors))
		for err := range Errors {
			log.Printf("%v", err)
		}
	}
	ErrorMutex.Unlock()
}

func shutdown() {
	log.Printf("Shutting down..")
	close(CopyFiles)
	close(DirCollector)

	go kill()
	printStats()
	finished := 0
	for finished < Config.FileWorkers {
		finished += <-fileWorkerQuit
		log.Printf("File Worker quit..")
	}

	printStats()
	finished = 0
	for finished < Config.DirWorkers {
		finished += <-dirWorkerQuit
		log.Printf("Directory Worker quit..")
	}
	finale()
}
