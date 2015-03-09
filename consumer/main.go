package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/sendgridlabs/go-kinesis"
)

var (
	delay      int
	delayTime  time.Duration
	streamName string
)

var logs = make(map[string]map[string][]byte)

func init() {
	flag.IntVar(&delay, "delay", 200, "Delay in milliseconds for processor to wait.")
	flag.StringVar(&streamName, "stream", "", "The name of the stream to setup or teardown.")
}

func main() {
	log.Println("Start")

	flag.Parse()

	delayTime = time.Duration(delay) * time.Millisecond

	if streamName == "" {
		log.Println("Invalid stream name")
		return
	}

	// set env variables AWS_ACCESS_KEY and AWS_SECRET_KEY AWS_REGION_NAME
	auth := kinesis.Auth{}
	auth.InferCredentialsFromEnv()
	ksis := kinesis.New(&auth, kinesis.Region{})

	args := kinesis.NewArgs()
	resp2, err := ksis.ListStreams(args)

	if len(resp2.StreamNames) <= 0 {
		log.Println("No streams to consume messages from, exiting.")
		return
	}

	log.Println("ListStreams: %v\n", resp2)

	resp3 := &kinesis.DescribeStreamResp{}

	timeout := make(chan bool)
	for {

		args = kinesis.NewArgs()
		args.Add("StreamName", streamName)
		resp3, err = ksis.DescribeStream(args)

		if err != nil {
			log.Println("Describe ERROR: %v\n", err)
			return
		}

		if resp3 == nil {
			log.Println("Describe is nil.")
			return
		}

		log.Println("DescribeStream: %v\n", resp3)

		if resp3.StreamDescription.StreamStatus != "ACTIVE" {
			time.Sleep(4 * time.Second)
			timeout <- true
		} else {
			break
		}
	}

	log.Println("Stream ", resp3.StreamDescription.StreamName)
	log.Println("Shards ", resp3.StreamDescription.Shards)

	logChan := make(chan LogResult)

	go processRecords(logChan)

	for _, shard := range resp3.StreamDescription.Shards {
		log.Println("Inserting consumer for ", streamName, shard.ShardId)
		go getRecords(ksis, streamName, shard.ShardId, logChan)
	}

	userVal := getInputValue()

	for {
		if userVal == "q" {
			log.Println("Quitting the app.")
			return
		} else {
			log.Println("Still quitting :) ", userVal)
			return
		}
	}
}

type LogResult struct {
	streamName string
	record     kinesis.GetRecordsRecords
}

func getInputValue() string {
	var userVal string
	_, err := fmt.Scanf("%s", &userVal)

	if err != nil {
		log.Fatal(err)
	}
	return userVal
}

func processRecords(logChan chan LogResult) {
	for {
		val := <-logChan
		d := val.record

		logMap, ok := logs[val.streamName]

		if !ok {
			logMap = make(map[string][]byte)
			logs[val.streamName] = logMap
		}

		logMap[d.SequenceNumber] = d.GetData()

		log.Println("")
		log.Println("GetRecords Data BEGIN")
		log.Println("GetRecords Data: ", string(d.GetData()))
		log.Println("GetRecords Partition Key: ", d.PartitionKey)
		log.Println("GetRecords Sequence Number: ", d.SequenceNumber)
		log.Println("---------------------------------------------")
		log.Println("GetRecords Data END")
		log.Println("************************************************")
		log.Println("")
	}
}

func getRecords(ksis *kinesis.Kinesis, streamName, ShardId string, logChan chan LogResult) {
	args := kinesis.NewArgs()
	args.Add("StreamName", streamName)
	args.Add("ShardId", ShardId)
	args.Add("ShardIteratorType", "TRIM_HORIZON")
	resp10, _ := ksis.GetShardIterator(args)

	shardIterator := resp10.ShardIterator

	log.Println("")
	log.Println("Waiting...")

	for {
		args = kinesis.NewArgs()
		args.Add("ShardIterator", shardIterator)
		resp11, err := ksis.GetRecords(args)

		if err != nil {
			log.Fatal(err)
		}

		if resp11 == nil {
			log.Println("Response is nil.")
			break
		}

		if len(resp11.Records) > 0 {
			for _, d := range resp11.Records {
				log.Println("Send to chan.")
				logChan <- LogResult{
					streamName,
					d,
				}
			}
		} else if resp11.NextShardIterator == "" || shardIterator == resp11.NextShardIterator || err != nil {
			log.Println("GetRecords ERROR: %v\n", err)
			break
		}

		shardIterator = resp11.NextShardIterator

		//fmt.Printf(".")
		time.Sleep(delayTime)
	}
}
