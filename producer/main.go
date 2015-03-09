package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/sendgridlabs/go-kinesis"
)

var batchSize int
var batchCount int
var streamName string

func init() {
	flag.IntVar(&batchSize, "batch_size", 1, "Number of messages to bundle in a batch.")
	flag.IntVar(&batchCount, "batch_count", 1, "Number of batches to produce.")
	flag.StringVar(&streamName, "stream", "", "Stream to send batches to.")
}

func main() {
	fmt.Println("Start")

	flag.Parse()

	if streamName == "" {
		log.Println("Invalid stream name")
		return
	}

	// set env variables AWS_ACCESS_KEY and AWS_SECRET_KEY AWS_REGION_NAME
	auth := kinesis.Auth{}
	auth.InferCredentialsFromEnv()
	ksis := kinesis.New(&auth, kinesis.Region{})

	args := kinesis.NewArgs()
	resp2, _ := ksis.ListStreams(args)

	if len(resp2.StreamNames) <= 0 {
		log.Println("No streams to send messages to, exiting.")
		return
	}

	fmt.Printf("ListStreams: %v\n", resp2)

	sendBatch(streamName, ksis)

	userVal := getInputValue()

	for {
		if userVal == "q" {
			fmt.Println("Quitting the app.")
			return
		} else if userVal == "s" {
			sendBatch(streamName, ksis)
			userVal = getInputValue()
		} else {
			fmt.Println("Still quitting :) ", userVal)
			return
		}
	}
}

func getInputValue() string {
	var userVal string
	_, err := fmt.Scanf("%s", &userVal)

	if err != nil {
		log.Fatal(err)
	}
	return userVal
}

func sendBatch(streamName string, ksis *kinesis.Kinesis) {
	// Put records in batch

	for i := 0; i < batchCount; i++ {
		args := kinesis.NewArgs()
		args.Add("StreamName", streamName)

		for i := 0; i < batchSize; i++ {
			args.AddRecord(
				[]byte(fmt.Sprintf("Hello AWS Kinesis - message index %d", i)),
				fmt.Sprintf("partitionKey-%d", i),
			)
		}

		resp4, err := ksis.PutRecords(args)
		if err != nil {
			fmt.Printf("PutRecords err: %v\n", err)
		} else {
			fmt.Printf("PutRecords: %v\n", resp4)
		}
	}
}
