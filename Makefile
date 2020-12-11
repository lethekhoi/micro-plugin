PROJECT_NAME=micro-plugins
BUILD_VERSION=$(shell cat VERSION)
GO_BUILD_ENV=CGO_ENABLED=0 GOOS=linux GOARCH=amd64 GO111MODULE=on
GO_FILES=$(shell go list ./... | grep -v /vendor/)
GO_FOLDERS=$(shell find * -maxdepth 1 -mindepth 1 -type d)

.SILENT:

all: build_test_all

build_test_all:
	for dir in $(GO_FOLDERS); do \
		echo '================ building' $$dir '================';\
		cd $$dir ;\
		$(GO_BUILD_ENV) go test ./... -cover -v -count=1|| exit 1; \
		$(GO_BUILD_ENV) go fmt ./... || exit 1; \
		$(GO_BUILD_ENV) go vet ./... || exit 1;\
		$(GO_BUILD_ENV) go build -v  ./... || exit 1;\
		cd ../..;\
	done