package main

import (
	"archive/zip"
	"bytes"
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/service/s3"
)

type UnzipFile struct {
	svc *s3.S3

	record        *events.S3EventRecord
	s3Object      *events.S3Object
	outpputFolder string
}

func NewUnzipFile(svc *s3.S3, record *events.S3EventRecord) *UnzipFile {
	return &UnzipFile{
		svc:      svc,
		record:   record,
		s3Object: &record.S3.Object,
	}
}

func (u *UnzipFile) DeleteObject(context context.Context, bucket, key *string) error {
	_, err := svc.DeleteObjectWithContext(context, &s3.DeleteObjectInput{
		Bucket: bucket,
		Key:    key,
	})
	return err
}

func (u *UnzipFile) MoveObject(context context.Context, bucket, key, destBucket, destKey *string) error {
	copySource := filepath.Join(*bucket, *key)
	_, err := svc.CopyObjectWithContext(context, &s3.CopyObjectInput{
		Bucket:     destBucket,
		CopySource: &copySource,
		Key:        destKey,
	})
	if err != nil {
		return err
	}
	return u.DeleteObject(context, bucket, key)
}

func (u *UnzipFile) Work(ctx context.Context, jobs chan *zip.File, errCh chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()

	isDone := false
	for !isDone {
		select {
		case <-ctx.Done():
			isDone = true
		case file, ok := <-jobs:
			if !ok {
				isDone = true
			} else {
				fileReader, err := file.Open()
				if err != nil {
          SendMessageNonBlocking(errCh, fmt.Errorf("Error opening file: %s", err))
				}
				defer fileReader.Close()

				fileData, err := io.ReadAll(fileReader)
				if err != nil {
          SendMessageNonBlocking(errCh, fmt.Errorf("Error reading file: %s", err))
				}
				reader := bytes.NewReader(fileData)
				outputKey := filepath.Join(u.outpputFolder, file.Name)
				_, err = svc.PutObjectWithContext(ctx, &s3.PutObjectInput{
					Bucket: &u.record.S3.Bucket.Name,
					Key:    &outputKey,
					Body:   reader,
				})
				if err != nil {
          SendMessageNonBlocking(errCh, fmt.Errorf("Error putting object: %s", err))
				}
			}
		}
	}
}

func (u *UnzipFile) ValidateZipFile(ctx context.Context) error {
	if u.s3Object.Size > MaxZipSize {
		sizeError := fmt.Errorf("File size too large: %d", u.s3Object.Size)
		if err := u.MoveObject(ctx, &u.record.S3.Bucket.Name, &u.record.S3.Object.Key, &u.record.S3.Bucket.Name,
			&InvalidFileOuputKey); err != nil {
			return fmt.Errorf("%s\nError moving object: %s", sizeError, err)
		}
	}

	return nil
}

func (u *UnzipFile) GetZipReader(ctx context.Context) (*zip.Reader, error) {
	res, err := svc.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: &u.record.S3.Bucket.Name,
		Key:    &u.record.S3.Object.Key,
	})
	if err != nil {
		return nil, fmt.Errorf("Error getting object: %s", err)
	}
	defer res.Body.Close()
	data, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("Error reading object: %s", err)
	}
	dataReader := bytes.NewReader(data)
	zipReader, err := zip.NewReader(dataReader, *res.ContentLength)
	if err != nil {
		return nil, fmt.Errorf("Error reading zip: %s", err)
	}
	return zipReader, nil
}

func (u *UnzipFile) Execute(parentCtx context.Context) error {
	key := u.s3Object.Key
	outputFolder := strings.TrimSuffix(key, filepath.Ext(key))
	u.outpputFolder = outputFolder

	zipReader, err := u.GetZipReader(parentCtx)
	if err != nil {
		return err
	}

	wg := sync.WaitGroup{}
	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()

	errCh := make(chan error)
	jobs := make(chan *zip.File, 40)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go u.Work(ctx, jobs, errCh, &wg)
	}

	go func() {
		for {
			select {
			case err, ok := <-errCh:
				if ok {
					cancel()
					fmt.Println("Error:", err)
					return
				}
			}
		}
	}()

	totalSize := uint64(0)
	totalFiles := 0
	for _, file := range zipReader.File {
		totalFiles++
		if totalFiles > MaxFileCount {
      SendMessageNonBlocking(errCh, fmt.Errorf("Too many files: %d", totalFiles))
			break
		}
		totalSize += file.UncompressedSize64
		if totalSize > MaxExtractSize {
      SendMessageNonBlocking(errCh, fmt.Errorf("Total size too large: %d", totalSize))
			break
		}

		jobs <- file
	}

	close(jobs)
	wg.Wait()
	close(errCh)

	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

func SendMessageNonBlocking[T any](ch chan<- T, msg T) bool {
    select {
    case ch <- msg:
        return true
    default:
        return false
    }
}
