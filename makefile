PKG := "github.com/cectc/dbpack"
PKG_LIST := $(shell go list ${PKG}/... | grep /pkg/)
GO_FILES := $(shell find . -name '*.go' | grep /pkg/ | grep -v _test.go)

.DEFAULT_GOAL := build
.PHONY: all test lint fmt fmtcheck cmt errcheck license

all: fmt errcheck lint build
default: fmt errcheck

########################################################
fmt: ## Format the files
	@gofmt -l -w $(GO_FILES)

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
	@go test -short ${PKG_LIST}

########################################################
race: dep ## Run data race detector
	@go test -race -short ${PKG_LIST}

########################################################
msan: dep ## Run memory sanitizer
	@go test -msan -short ${PKG_LIST}

########################################################
dep: ## Get the dependencies
	@go get -v -d ./...

########################################################
version: ## Print git revision info
	@echo $(expr substr $(git rev-parse HEAD) 1 8)

########################################################
unit-test: ## run unit test
	go test ./pkg/... -coverprofile=coverage.txt -covermode=atomic

########################################################
build:  ## build dbpack cli, and put in dist dir
	@mkdir -p dist
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o ./dist/dbpack ./cmd

########################################################
docker-build: build ## build docker image
	docker build -f docker/Dockerfile -t dbpack:latest .

########################################################
integration-test: build docker-build
	sh test/cmd/test_single_db.sh
	sh test/cmd/test_read_write_splitting.sh

########################################################
clean: ## clean temporary build dir
	@rm -rf coverage.txt
	@rm -rf dist

########################################################
license: ## Add license header for all code files
	@find . -name \*.go -exec sh -c "if ! grep -q 'LICENSE' '{}'; then mv '{}' tmp && cp doc/LICENSEHEADER.txt '{}' && cat tmp >> '{}' && rm tmp; fi" \;

########################################################
help: ## Display this help screen
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
