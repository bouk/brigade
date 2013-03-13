package main

import "time"
import "log"

var start time.Time

type StatsType struct {
  files int
  directories int
  bytes int64
  working int
  errors int
  bps int
}

var Stats StatsType

func printStats() {
  duration := int64(time.Since(start).Seconds())
  if duration > 0 {
    Stats.bps = int(Stats.bytes / duration)
  } else {
    Stats.bps = 0
  }

  log.Printf("%+v", Stats)
}

func statsWorker(period int) {
  start = time.Now()

  delay := time.Duration(period) * time.Second
  for {
    printStats()
    time.Sleep(delay)
  }
}
