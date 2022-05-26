PKG := "github.com/cectc/dbpack"
PKG_LIST := $(shell go list ${PKG}/... | grep /pkg/)
GO_FILES := $(shell find . -name '*.go' | grep /pkg/ | grep -v _test.go)

# Environment variables set when running the Go compiler.
GOOS ?= $(shell go env GOOS)
GOARCH ?= $(shell go env GOARCH)
GO_BUILD_ENVVARS = \
	GOOS=$(GOOS) \
	GOARCH=$(GOARCH) \
	CGO_ENABLED=0 \

.DEFAULT_GOAL := build
.PHONY: all default test lint fmt fmtcheck cmt errcheck race license help msan dep build docker-build clean

all: fmt errcheck lint build
default: fmt errcheck

########################################################
fmt: ## Format the files
	@gofmt -l -w $(GO_FILES)
	@goimports -w --local $(PKG) .

########################################################
fmtcheck: ## Check and format the files
	@gofmt -l -s $(GO_FILES) | read; if [ $$? == 0 ]; then echo "gofmt check failed for:"; gofmt -l -s $(GO_FILES); fi

########################################################
lint:  ## lint check
	@hash revive 2>&- || go get -u github.com/mgechev/revive
	@revive -formatter stylish pkg/...

########################################################
cmt: ## auto comment exported Function
	@hash gocmt 2>&- || go get -u github.com/Gnouc/gocmt
	@gocmt -d pkg -i

########################################################
errcheck: ## check error
	@hash errcheck 2>&- || go get -u github.com/kisielk/errcheck
	@errcheck pkg/...

########################################################
test: ## Run unittests
	@go test -gcflags=all=-l -short ${PKG_LIST}

########################################################
race: dep ## Run data race detector
	@go test -gcflags=all=-l -race -short ${PKG_LIST}

########################################################
msan: dep ## Run memory sanitizer
	@go test -gcflags=all=-l -msan -short ${PKG_LIST}

########################################################
dep: ## Get the dependencies
	@go get -v -d ./...

########################################################
version: ## Print git revision info
	@echo $(expr substr $(git rev-parse HEAD) 1 8)

########################################################
unit-test: ## run unit test
	go test -gcflags=all=-l ./pkg/... -coverprofile=coverage.txt -covermode=atomic

########################################################
build-local:  ## build dbpack cli, and put in dist dir
	${GO_BUILD_ENVVARS} go build -o dbpack ./cmd

########################################################
docker-build: ## build docker image
	go mod download
	go mod tidy
	docker build -f docker/Dockerfile -t dbpack:latest .

########################################################
build:  ## build dbpack cli, and put in dist dir
	GOOS="linux"  GOARCH="amd64" CGO_ENABLED=0 go build -o dbpack ./cmd

########################################################
integration-test:
	sh test/cmd/test_single_db.sh
	sh test/cmd/test_read_write_splitting.sh
	sh test/cmd/test_sharding.sh

########################################################
clean: ## clean temporary build dir
	@rm -rf coverage.txt

########################################################
license: ## Add license header for all code files
	@find . -name \*.go -exec sh -c "if ! grep -q 'LICENSE' '{}'; then mv '{}' tmp && cp doc/LICENSEHEADER.txt '{}' && cat tmp >> '{}' && rm tmp; fi" \;

########################################################
help: ## Display this help screen
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
