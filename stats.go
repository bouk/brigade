package main

import (
  "time"
  "log"
  "fmt"
)

var start time.Time

type StatsType struct {
	files       int
	directories int
	bytes       int64
	working     int
	errors      int
}

var Stats StatsType

var lastLog string

func printStats() {
  newLog := fmt.Sprintf("%+v", Stats)
  if newLog != lastLog {
    lastLog = newLog
    log.Printf(lastLog)
  }
}

func statsWorker(period int) {
	start = time.Now()

	delay := time.Duration(period) * time.Second
	for {
		printStats()
		time.Sleep(delay)
	}
}
