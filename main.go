package main

import (
	"errors"
	"fmt"
	"github.com/tobi/airbrake-go"
	"log"
	"os"
	"sync"
	"time"
)

var (
	dirWorker []*S3Connection

	dirworkerGroup     sync.WaitGroup
	dirWorkersFinished = make(chan int)
	dirWorkerQuit      = make(chan int)

	fileGroup sync.WaitGroup

	startTime time.Time
)

func statsUpdated() {
	if Config.StatsTicker {
		fmt.Print("\r\033[K")
		fmt.Printf("%+v", Stats)
	}
}

func main() {
	startTime = time.Now()
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

	statsUpdated()
	performCopy()
	shutdown()

	if Config.StatsTicker {
		fmt.Println()
	}
}

func setup() {
	var err error
	dirWorker = make([]*S3Connection, Config.DirWorkers)

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
	dirworkerGroup.Wait()
	fileGroup.Wait()
}

func kill() {
	// force death if shutting down takes > 20 minutes
	log.Printf("Shutdown took too long, forcing exit")
	finale()
	os.Exit(1)
}

func finale() {
	log.Printf("Final stats:")
	printStats()
	printErrors()

	log.Printf("Time taken: %d seconds", time.Now().Sub(startTime)/time.Second)
}

func stop() {
	close(DirCollector)
}

func shutdown() {
	log.Printf("Shutting down..")
	stop()

	time.AfterFunc(20*time.Minute, kill)

	log.Printf("%+v", Stats)

	for i := 1; i <= Config.DirWorkers; i++ {
		<-dirWorkerQuit
		log.Printf("Directory Worker %d quit", i)
	}

	finale()
}
