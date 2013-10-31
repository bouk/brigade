package main

import (
	"fmt"
	"log"
	"time"
)

var start time.Time

type StatsType struct {
	files       int64
	directories int64
	bytes       int64
	working     int64
	errors      int64
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
