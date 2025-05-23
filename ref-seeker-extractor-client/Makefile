GIT_REPO=dbis/${BINARY_NAME}

APP=ref-seeker-client
APP_EXECUTABLE="./out/$(APP)"
ALL_PACKAGES=$(shell go list ./... | grep -v /vendor)

 SHELL := /bin/bash

all: fmt build 

init:
	go mod init ${GIT_REPO}

setup: 
	go mod tidy

check:
	make lint
	make vet

lint:
	golangci-lint run

vet: 
	go vet ./...

fmt: 
	gofmt -l -w .
 
build:
	mkdir -p out/
	go build -o $(APP_EXECUTABLE)
	@echo "Build passed"

run:
	make build
	chmod +x $(APP_EXECUTABLE)
	$(APP_EXECUTABLE)

clean:
	go clean
	rm -rf out/