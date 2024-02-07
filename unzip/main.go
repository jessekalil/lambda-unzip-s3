package main

import (
	"context"
	"fmt"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

var (
	svc *s3.S3

	InvalidFileOuputKey = "/invalid"

	MaxZipSize     int64  = 1024 * 1024 * 100  // 100MB
	MaxExtractSize uint64 = 1024 * 1024 * 1000 // 1GB
	MaxFileCount   int    = 10000
)

func init() {
	session := session.Must(session.NewSession())
	svc = s3.New(session)
}

func Handler(context context.Context, s3Event events.S3Event) (string, error) {
	if len(s3Event.Records) == 0 {
		return "", fmt.Errorf("No records in event")
	}

	record := s3Event.Records[0]

  unzipFile := NewUnzipFile(svc, &record)
	if err := unzipFile.ValidateZipFile(context); err != nil {
		return "", err
	}
	err := unzipFile.Execute(context)
	if err != nil {
		return "", err
	}

	return "Hello from Lambda!", nil
}

func main() {
	lambda.Start(Handler)
}
