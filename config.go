package main

import (
	"encoding/json"
	"io"
	"log"
	"os"
)

type ConfigType struct {
	DirWorkers  int
	FileWorkers int

	Source, Destination        string
	AccessKey, SecretAccessKey string

	// http or https
	Protocol string
	Host     string

	StatsTicker bool
}

var Config ConfigType

func (c *ConfigType) Endpoint() (string, error) {
	return c.Protocol + "://" + c.Host, nil
}

func readConfig() error {
	configFile := os.Getenv("ENV")

	if configFile == "" {
		configFile = "config/default.json"
	}

	log.Printf("Loading environment from %s (override with ENV=xxx)", configFile)

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
