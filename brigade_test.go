package main

import (
	"github.com/boourns/goamz/s3"
	"os"
	"testing"
	"time"
)

var sourceBucketName string = "brigade-test-source"
var destBucketName string = "brigade-test-destination"

func TestCredentials(t *testing.T) {
	if os.Getenv("ACCESS_KEY") == "" || os.Getenv("SECRET_ACCESS_KEY") == "" || os.Getenv("AWS_HOST") == "" {
		t.Error("Please set ACCESS_KEY, SECRET_ACCESS_KEY, and AWS_HOST variables for integration tests")
	}
}

func loadTarget(bucket string) *Target {
	return &Target{os.Getenv("AWS_HOST"), bucket, os.Getenv("ACCESS_KEY"), os.Getenv("SECRET_ACCESS_KEY")}
}

func LoadTestConfig() {
	Config = ConfigType{Source: loadTarget(sourceBucketName), Dest: loadTarget(destBucketName), FileWorkers: 0, DirWorkers: 1}
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
	source := S3Connect(loadTarget(sourceBucketName))
	dest := S3Connect(loadTarget(destBucketName))

	sourceBucket := source.Bucket(sourceBucketName)
	destBucket := dest.Bucket(destBucketName)

	err := sourceBucket.PutBucket(s3.PublicRead)
	if err != nil {
		return err
	}

	err = destBucket.PutBucket(s3.PublicRead)
	if err != nil {
		return err
	}

	err = uploadFixtures(sourceBucket, sourceFixtures)
	if err != nil {
		return err
	}

	err = uploadFixtures(destBucket, destFixtures)
	if err != nil {
		return err
	}

	return nil
}

func TestConnection(t *testing.T) {
	conn := S3Connect(loadTarget(sourceBucketName))

	if conn == nil {
		t.Error("Could not connect to S3 host.  Check network & credentials")
	}
}

func TestFindKey(t *testing.T) {
	setup()

	err := SetupBuckets()
	if err != nil {
		t.Error("Failed to set up buckets")
	}

	LoadTestConfig()
	conn := S3Init()

	sourceList, err := conn.SourceBucket.List("animals/", "/", "", 1000)
	if err != nil {
		t.Error("Failed to list animals dir")
	}

	key, ok := findKey("animals/cat", sourceList)
	if !ok || key.Key != "animals/cat" {
		t.Error("Failed to find animals/cat in source bucket by key")
	}
}

func TestCopyDirectory(t *testing.T) {
	setup()

	err := SetupBuckets()
	if err != nil {
		t.Error("Failed to set up buckets")
	}

	LoadTestConfig()

	//CopyDirectory("")

	for PendingDirectories > 0 {
		time.Sleep(time.Second)
	}
}

func TestCopyBucket(t *testing.T) {
	setup()

	err := SetupBuckets()
	if err != nil {
		t.Error("Failed to set up buckets")
	}

	LoadTestConfig()
	conn := S3Init()

	conn.CopyBucket()

	copyExpected := []string{"house2", "house5", "animals/dog"}

	if len(CopyFiles) != len(copyExpected) {
		t.Errorf("CopyBucket found %d files to copy but we expected %d", len(CopyFiles), len(copyExpected))
		return
	}

	for i := 0; i < len(copyExpected); i++ {
		file := <-CopyFiles
		if file != copyExpected[i] {
			t.Errorf("CopyBucket file #%d was %s but expected %s", i, file, copyExpected[i])
		}
	}

	delExpected := []string{"house6", "vehicles/truck"}

	if len(DeleteFiles) != len(delExpected) {
		t.Errorf("CopyBucket found %d files to delete but we expected %d", len(DeleteFiles), len(delExpected))
		return
	}

	for i := 0; i < len(delExpected); i++ {
		file := <-DeleteFiles
		if file != delExpected[i] {
			t.Errorf("CopyBucket file to delete #%d was %s but expected %s", i, file, delExpected[i])
		}
	}

}

func TestReadMIME(t *testing.T) {
	setup()

	err := SetupBuckets()
	if err != nil {
		t.Error("Failed to set up buckets")
	}

	LoadTestConfig()
	conn := S3Init()

	resp, err := conn.SourceBucket.GetResponse("house3")
	if err != nil {
		t.Errorf("Failed to get a response for fixture")
	}

	if resp.Header["Content-Type"][0] != "text/xml" {
		t.Errorf("Content-Type was incorrect")
	}
}
