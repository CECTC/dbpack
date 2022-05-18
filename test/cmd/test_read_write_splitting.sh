#!/bin/sh

docker-compose -f docker/docker-compose-rws.yaml up --build -d

sleep 90

go clean -testcache
go test -tags integration -v ./test/rws/

docker logs dbpack

docker-compose -f docker/docker-compose-rws.yaml down