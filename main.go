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

	fileGroup sync.WaitGroup

	startTime time.Time

	s3Connection *S3Connection
)

func statsUpdated() {
	if Config.StatsTicker {
		fmt.Print("\r\033[K")
		fmt.Printf("%+v", Stats)
	}
}

func main() {
	startTime = time.Now()
	if len(os.Getenv("LOG")) > 0 {
		logFile, err := os.OpenFile(os.Getenv("LOG"), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			err = errors.New(fmt.Sprintf("Could not open log file %s for writing: %s", os.Getenv("LOG"), err.Error()))
			airbrake.Error(err, nil)
			log.Fatal(err)
		}
		defer logFile.Close()

		log.SetOutput(logFile)
	}

	go statsWorker()

	if err := readConfig(); err != nil {
		log.Fatal(err)
	}
	FileQueue = make(chan string, Config.FileWorkers)

	statsUpdated()
	performCopy()
	shutdown()

	if Config.StatsTicker {
		fmt.Println()
	}
}

func statsWorker() {
	for _ = range time.Tick(10 * time.Second) {
		printStats()
	}
}

func setup() {
	var err error

	// directory queue manager
	go DirManager()

	s3Connection, err = S3Init()
	if err != nil {
		log.Fatal(err)
	}

	for i := 1; i <= Config.FileWorkers; i++ {
		go s3Connection.fileWorker(i)
	}

	// N directory workers
	for i := 1; i <= Config.DirWorkers; i++ {
		go s3Connection.dirWorker(i)
	}
}

func performCopy() {
	setup()
	pushDirectory("")
	dirworkerGroup.Wait()
	fileGroup.Wait()
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

	log.Printf("%+v", Stats)
	finale()
}
