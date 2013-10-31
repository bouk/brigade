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
	fileWorker []*S3Connection
	dirWorker  []*S3Connection

	dirWorkersFinished = make(chan int)
	fileWorkerQuit     = make(chan int)
	dirWorkerQuit      = make(chan int)
)

func main() {
	if len(os.Getenv("log")) > 0 {
		logFile, err := os.OpenFile(os.Getenv("log"), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			err = errors.New(fmt.Sprintf("Could not open log file %s for writing: %s", os.Getenv("log"), err.Error()))
			airbrake.Error(err, nil)
			log.Fatal(err)
		}
		defer logFile.Close()

		log.SetOutput(logFile)
	}

	logTime := 300
	if logTime > 0 {
		go statsWorker(logTime)
	}

	if err := readConfig(); err != nil {
		log.Fatal(err)
	}

	performCopy()
	shutdown()
}

func setup() {
	var err error

	fileWorker = make([]*S3Connection, Config.FileWorkers)
	dirWorker = make([]*S3Connection, Config.DirWorkers)

	// spawn workers
	log.Printf("Spawning %d file workers", Config.FileWorkers)

	for i := 0; i < Config.FileWorkers; i++ {
		fileWorker[i], err = S3Init()
		if err != nil {
			log.Fatal(err)
		}
		go fileWorker[i].fileCopier(fileWorkerQuit)
	}

	// 1 worker for the directory queue manager
	go DirManager()

	// N directory workers
	for i := 0; i < Config.DirWorkers; i++ {
		dirWorker[i], err = S3Init()
		if err != nil {
			log.Fatal(err)
		}
		go dirWorker[i].dirWorker(dirWorkerQuit)
	}
}

func performCopy() {
	setup()
	pushDirectory("")
	<-dirWorkersFinished
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

	printErrors()
}

func stop() {
	close(CopyFiles)
	close(DirCollector)
}

func shutdown() {
	log.Printf("Shutting down..")
	stop()

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
