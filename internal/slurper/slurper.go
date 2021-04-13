package slurper

import (
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

type Slurper struct {
	KinesisSvc *kinesis.Kinesis
}

type streamItem struct {
	Timestamp time.Time
	Payload   string
}

func (slurper *Slurper) Slurp(streamName string, shardId string, fromTimestamp time.Time, out io.Writer) error {
	iterator, err := slurper.getInitialIterator(streamName, shardId, fromTimestamp)
	if err != nil {
		return err
	}

	for {
		records, nextIterator, err := slurper.getRecords(iterator)
		if err != nil {
			return err
		}

		for _, record := range records {
			item := streamItem{
				Timestamp: *record.ApproximateArrivalTimestamp,
				Payload:   string(record.Data),
			}

			bytes, err := json.Marshal(item)
			if err != nil {
				return fmt.Errorf("Failed to marshall item to JSON: %v", err)
			}

			_, err = fmt.Fprintln(out, string(bytes))
			if err != nil {
				return fmt.Errorf("Failed to write to output: %v", err)
			}
		}

		if nextIterator != nil {
			iterator = nextIterator
		} else {
			break
		}
	}

	return nil
}

func (slurper *Slurper) getInitialIterator(streamName string, shardId string, fromTimestamp time.Time) (*string, error) {
	input := &kinesis.GetShardIteratorInput{
		StreamName:        aws.String(streamName),
		ShardId:           aws.String(shardId),
		ShardIteratorType: aws.String("AT_TIMESTAMP"),
		Timestamp:         &fromTimestamp,
	}

	iterator, err := slurper.KinesisSvc.GetShardIterator(input)
	if err != nil {
		return nil, fmt.Errorf("Failed to GetShardIterator for stream: %v", err)

	}

	return iterator.ShardIterator, nil
}

func (slurper *Slurper) getRecords(iterator *string) ([]*kinesis.Record, *string, error) {
	input := &kinesis.GetRecordsInput{
		ShardIterator: iterator,
	}

	output, err := slurper.KinesisSvc.GetRecords(input)
	if err != nil {
		return nil, nil, fmt.Errorf("Failed to GetRecords from stream: %v", err)
	}

	if len(output.Records) == 0 && *output.MillisBehindLatest == 0 {
		return nil, nil, nil
	}

	return output.Records, output.NextShardIterator, nil
}
