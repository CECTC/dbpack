unit-test:
	go test ./pkg/... -coverprofile=coverage.txt -covermode=atomic

build:
	@mkdir -p dist
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o ./dist/dbpack ./cmd

docker-build: build
	docker build -f docker/Dockerfile -t dbpack:latest .

clean:
	@rm -rf coverage.txt
	@rm -rf dist