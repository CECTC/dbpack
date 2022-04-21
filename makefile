unit-test:
	go test ./pkg/... -coverprofile=coverage.txt -covermode=atomic

build:
	@mkdir -p dist
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o ./dist/dbpack ./cmd

docker-build: build
	docker build -f docker/Dockerfile -t dbpack:latest .

integration-test: build docker-build
	sh test/cmd/test_single_db.sh
	sh test/cmd/test_read_write_splitting.sh

clean:
	@rm -rf coverage.txt
	@rm -rf dist