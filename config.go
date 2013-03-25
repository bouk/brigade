package main

import "os"
import "io"
import "log"
import "encoding/json"

type Target struct {
	Server          string
	BucketName      string
	AccessKey       string
	SecretAccessKey string
}

type ConfigType struct {
	Source  *Target
	Dest    *Target
	FileWorkers int
	DirWorkers int
}

var Config ConfigType

func readConfig() {
	configFile := os.Getenv("ENV")

	if configFile == "" {
		configFile = "config/default.json"
	}

	log.Printf("Loading environment from %s (override with ENV=xxx)", configFile)

	f, err := os.Open(configFile)

	if err != nil {
		log.Printf("Error opening config file: %s", err)
		return
	}

	loadConfig(f)
}

func loadConfig(source io.Reader) {
	err := json.NewDecoder(source).Decode(&Config)

	if err != nil {
		log.Fatalf("Error parsing config file: %s", err)
		return
	}
}
