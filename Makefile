.PHONY: build clean deploy 

build: 
	env GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -ldflags="-s -w" -o bin/unzip unzip/*.go

clean:
	rm -rf ./bin

deploy: clean build
	env -u AWS_PROFILE sls deploy --verbose

deployforce: clean build
	env -u AWS_PROFILE sls deploy --force --verbose
