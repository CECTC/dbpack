#!/bin/sh

docker-compose -f docker/docker-compose-sdb.yaml up --build -d

sleep 90

go clean -testcache
go test -tags integration -v ./test/sdb/

docker logs dbpack

docker-compose -f docker/docker-compose-sdb.yaml down