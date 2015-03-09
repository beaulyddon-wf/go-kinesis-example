package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/sendgridlabs/go-kinesis"
)

var setup bool
var streamName string
var shardCount int

func init() {
	flag.BoolVar(&setup, "setup", true, "Flag determing setup or teardown.")
	flag.IntVar(&shardCount, "shards", 1, "Number of shards to setup for the stream.")
	flag.StringVar(&streamName, "stream", "", "The name of the stream to setup or teardown.")
}

const (
	streamCreateTimeout int = 30
)

func main() {
	fmt.Println("Start")

	flag.Parse()

	// set env variables AWS_ACCESS_KEY and AWS_SECRET_KEY AWS_REGION_NAME
	auth := kinesis.Auth{}
	auth.InferCredentialsFromEnv()
	ksis := kinesis.New(&auth, kinesis.Region{})

	err := ksis.CreateStream(streamName, shardCount)
	if err != nil {
		fmt.Printf("CreateStream ERROR: %v\n", err)
	}

	args := kinesis.NewArgs()
	resp2, _ := ksis.ListStreams(args)
	fmt.Printf("ListStreams: %v\n", resp2)

	resp3 := &kinesis.DescribeStreamResp{}

	timeout := make(chan bool, streamCreateTimeout)
	for {

		args = kinesis.NewArgs()
		args.Add("StreamName", streamName)
		resp3, err = ksis.DescribeStream(args)

		if err != nil {
			fmt.Printf("Describe ERROR: %v\n", err)
			return
		}

		if resp3 == nil {
			fmt.Printf("Describe is nil.")
			return
		}

		fmt.Printf("DescribeStream: %v\n", resp3)

		if resp3.StreamDescription.StreamStatus != "ACTIVE" {
			time.Sleep(4 * time.Second)
			timeout <- true
		} else {
			break
		}
	}

	resp2, _ = ksis.ListStreams(args)
	fmt.Printf("ListStreams: %v\n", resp2)

	if len(resp2.StreamNames) <= 0 {
		log.Println("Unable to setup stream ", streamName)
	} else {
		log.Println("Stream setup ", streamName)
	}
}

func killIt(streamName string, ksis *kinesis.Kinesis) {
	// Delete the stream
	err1 := ksis.DeleteStream(streamName)
	if err1 != nil {
		fmt.Printf("DeleteStream ERROR: %v\n", err1)
	}

	fmt.Println("Leaving")
}
