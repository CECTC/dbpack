#!/bin/sh

docker-compose -f docker/docker-compose-shd.yaml up --build -d

sleep 90

go clean -testcache
go test -tags integration -v ./test/shd/

docker logs dbpack

docker-compose -f docker/docker-compose-shd.yaml down