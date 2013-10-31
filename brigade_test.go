package main

import (
	"github.com/bouk/goamz/s3"
	"os"
	"testing"
)

var (
	sourceBucket, destBucket *s3.Bucket
	connection               *S3Connection
)

func LoadTestConfig() {
	Config = ConfigType{
		Source:          os.Getenv("SOURCE_BUCKET"),
		Destination:     os.Getenv("DESTINATION_BUCKET"),
		FileWorkers:     0,
		DirWorkers:      1,
		AccessKey:       os.Getenv("ACCESS_KEY"),
		SecretAccessKey: os.Getenv("SECRET_ACCESS_KEY"),
		Host:            os.Getenv("AWS_HOST"),
		Protocol:        "http",
	}
}

type fileFixture struct {
	key  string
	data []byte
	mime string
	perm s3.ACL
}

var sourceFixtures []fileFixture = []fileFixture{
	{"house", []byte("house data"), "text/plain", s3.PublicRead},
	{"house2", []byte("house2 data"), "text/plain", s3.PublicRead},
	{"house3", []byte("house3 data"), "text/xml", s3.PublicRead},
	{"house4", []byte("house4 data"), "text/plain", s3.Private},
	{"house5", []byte("house4 data"), "text/plain", s3.Private}, // only exists in source
	{"animals/cat", []byte("first cat"), "text/plain", s3.PublicRead},
	{"animals/dog", []byte("second cat"), "text/plain", s3.PublicRead}} // only exists in source

var destFixtures []fileFixture = []fileFixture{
	{"house", []byte("house data"), "text/plain", s3.PublicRead},               // identical
	{"house2", []byte("different house2 data"), "text/plain", s3.PublicRead},   // differing data
	{"house6", []byte("house6 data"), "text/plain", s3.Private},                // to be deleted
	{"animals/cat", []byte("first cat"), "text/plain", s3.PublicRead},          // identical
	{"vehicles/truck", []byte("this is a truck"), "text/plain", s3.PublicRead}} // to be deleted

var filesAlreadyInDest = 2

func uploadFixtures(bucket *s3.Bucket, fixtures []fileFixture) error {
	for i := 0; i < len(fixtures); i++ {
		err := bucket.Put(fixtures[i].key, fixtures[i].data, fixtures[i].mime, fixtures[i].perm)
		if err != nil {
			return err
		}
	}
	return nil
}

func SetupBuckets() error {
	var err error
	LoadTestConfig()
	connection, err = S3Init()
	if err != nil {
		return err
	}

	connection.Source.PutBucket(s3.PublicRead)
	connection.Destination.PutBucket(s3.PublicRead)

	if connection.Source == nil {
		panic("The fuck")
	}
	err = uploadFixtures(connection.Source, sourceFixtures)
	if err != nil {
		return err
	}

	err = uploadFixtures(connection.Destination, destFixtures)
	if err != nil {
		return err
	}

	return nil
}

func TestConnection(t *testing.T) {
	LoadTestConfig()
	_, err := S3Connect()

	if err != nil {
		t.Error(err)
	}
}

func TestSetupBuckets(t *testing.T) {
	if err := SetupBuckets(); err != nil {
		t.Error("Failed to set up buckets", err)
		t.FailNow()
	}
}

func TestFindKey(t *testing.T) {
	err := SetupBuckets()

	if err != nil {
		t.Error("Failed to set up buckets", err)
		return
	}

	sourceList, err := connection.Source.List("animals/", "/", "", 1000)
	if err != nil {
		t.Error("Failed to list animals dir", err)
	}

	key, ok := findKey("animals/cat", sourceList)
	if !ok || key.Key != "animals/cat" {
		t.Error("Failed to find animals/cat in source bucket by key")
	}
}

func TestCopyBucket(t *testing.T) {
	err := SetupBuckets()
	if err != nil {
		t.Error("Failed to set up buckets", err)
		return
	}

	performCopy()

	if len(CopyFiles) != (len(sourceFixtures) - filesAlreadyInDest) {
		t.Errorf("CopyBucket found %d files to copy but we expected %d", len(CopyFiles), len(sourceFixtures)-filesAlreadyInDest)
		return
	}

	stop()
}

func TestReadMIME(t *testing.T) {
	err := SetupBuckets()
	if err != nil {
		t.Error("Failed to set up buckets", err)
	}

	resp, err := connection.Source.GetResponse("house3")
	if err != nil {
		t.Errorf("Failed to get a response for fixture", err)
	}

	if resp.Header["Content-Type"][0] != "text/xml" {
		t.Errorf("Content-Type was incorrect")
	}
}
