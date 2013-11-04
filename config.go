package main

import (
	"encoding/json"
	"io"
	"log"
	"os"
)

var Config struct {
	DirWorkers  int
	FileWorkers int

	Source, Destination        string
	AccessKey, SecretAccessKey string

	// http or https
	Region string

	StatsTicker bool
	Verbose     bool
}

func readConfig() error {
	configFile := os.Getenv("CONFIG")

	if configFile == "" {
		configFile = "config/default.json"
	}

	log.Printf("Loading environment from %s (override with CONFIG=xxx)", configFile)

	f, err := os.Open(configFile)

	if err != nil {
		return err
	}
	defer f.Close()

	return loadConfig(f)
}

func loadConfig(source io.Reader) error {
	return json.NewDecoder(source).Decode(&Config)
}
