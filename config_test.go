package main

import "testing"
import "bytes"

func TestTargetLoadFromJSON(t *testing.T) {
	jsonTarget := bytes.NewBufferString(`
    {
      "Host": "s3.com",
      "Source": "source_bucket",
      "AccessKey": "1234",
      "SecretAccessKey": "MySecretAccessKey",
      "FileWorkers": 20,
      "Protocol": "https",
      "DirWorkers": 10
    }
  `)

	loadConfig(jsonTarget)

	if Config.Host != "s3.com" {
		t.Error("Config.Host incorrect")
	}

	if Config.Source != "source_bucket" {
		t.Error("Config.Source incorrect")
	}

	if Config.Protocol != "https" {
		t.Error("Config.Protocol incorrect")
	}

	if Config.AccessKey != "1234" {
		t.Error("Config.AccessKey incorrect")
	}

	if Config.SecretAccessKey != "MySecretAccessKey" {
		t.Error("Config.SecretAccessKey incorrect")
	}

	if Config.FileWorkers != 20 {
		t.Error("Config.FileWorkers incorrect")
	}

	if Config.DirWorkers != 10 {
		t.Error("Config.DirWorkers incorrect")
	}
}
