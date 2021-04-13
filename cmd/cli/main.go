package main

import (
	"flag"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/jaytee/kinesis-stream-slurper/internal/slurper"
)

func main() {
	streamNamePtr := flag.String("stream-name", "", "name of the Kinesis Data Stream to read from")
	streamShardIdPtr := flag.String("shard-id", "", "id of the shard to read from")
	streamRegionPtr := flag.String("region", "", "region that the stream resides in")
	fromTimestampPtr := flag.String("from-timestamp", "", "RFC3339 formatted timestamp to begin reading the stream from")

	flag.Parse()

	if *streamNamePtr == "" {
		log.Fatal("stream-name was not set.")
		return
	}

	if *streamShardIdPtr == "" {
		log.Fatal("shard-id was not set.")
		return
	}

	if *streamRegionPtr == "" {
		log.Fatal("region was not set.")
		return
	}

	fromTimestamp, err := time.Parse(time.RFC3339, *fromTimestampPtr)
	if err != nil {
		log.Fatalf("from-timestamp was invalid: %v", err)
		return
	}

	sess, err := session.NewSession(&aws.Config{
		Region: streamRegionPtr,
	})
	if err != nil {
		log.Fatalf("Failed to initialize new AWS session: %v", err)
	}

	slurper := slurper.Slurper{
		KinesisSvc: kinesis.New(sess),
	}

	err = slurper.Slurp(*streamNamePtr, *streamShardIdPtr, fromTimestamp, os.Stdout)
	if err != nil {
		log.Fatalf("Failed to slurp stream %s: %v", *streamNamePtr, err)
	}
}
