package main

import (
	"archive/zip"
	"bytes"
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

var (
	svc *s3.S3

	MaxZipSize     int64  = 1024 * 1024 * 100 // 100MB
	MaxExtractSize uint64 = 1024 * 1024 * 500 // 500MB
	MaxFileCount   int    = 10000
)

func init() {
	session := session.Must(session.NewSession())
	svc = s3.New(session)
}

func Handler(context context.Context, s3Event events.S3Event) (string, error) {
	for _, record := range s3Event.Records {
		s3Oject := record.S3.Object
		key := s3Oject.Key
		outputFolder := strings.TrimSuffix(key, filepath.Ext(key))

		if s3Oject.Size > MaxZipSize {
			return "", fmt.Errorf("File size too large: %d", s3Oject.Size)
		}
		getObjectInput := &s3.GetObjectInput{
			Bucket: &record.S3.Bucket.Name,
			Key:    &record.S3.Object.Key,
		}
		res, err := svc.GetObjectWithContext(context, getObjectInput)
		if err != nil {
			return "", fmt.Errorf("Error getting object: %s", err)
		}
		defer res.Body.Close()
		data, err := io.ReadAll(res.Body)
		if err != nil {
			return "", fmt.Errorf("Error reading object: %s", err)
		}
		dataReader := bytes.NewReader(data)
		zipReader, err := zip.NewReader(dataReader, *res.ContentLength)
		if err != nil {
			return "", fmt.Errorf("Error reading zip: %s", err)
		}

		totalSize := uint64(0)
		totalFiles := 0
		for _, file := range zipReader.File {
			totalFiles++
			if totalFiles > MaxFileCount {
				return "", fmt.Errorf("Too many files: %d", totalFiles)
			}
			totalSize += file.UncompressedSize64
			if totalSize > MaxExtractSize {
				return "", fmt.Errorf("Total size too large: %d", totalSize)
			}
			fileReader, err := file.Open()
			if err != nil {
				return "", fmt.Errorf("Error opening file: %s", err)
			}
			defer fileReader.Close()

			fileData, err := io.ReadAll(fileReader)
			if err != nil {
				return "", fmt.Errorf("Error reading file: %s", err)
			}
			reader := bytes.NewReader(fileData)
			outputKey := filepath.Join(outputFolder, file.Name)
			_, err = svc.PutObject(&s3.PutObjectInput{
				Bucket: &record.S3.Bucket.Name,
				Key:    &outputKey,
				Body:   reader,
			})
			if err != nil {
				return "", fmt.Errorf("Error putting object: %s", err)
			}
		}
	}

	return "Hello from Lambda!", nil
}

func main() {
	lambda.Start(Handler)
}
