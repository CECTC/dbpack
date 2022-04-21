#!/bin/sh

mkdir -p docker/data1
mkdir -p docker/mysqld1
mkdir -p docker/data2
mkdir -p docker/mysqld2
docker-compose -f docker/docker-compose-rws.yaml up -d

sleep 90

go clean -testcache
go test -tags integration -v ./test/rws/

docker logs dbpack

docker-compose -f docker/docker-compose-rws.yaml down
rm -rf docker/data1
rm -rf docker/mysqld1
rm -rf docker/data2
rm -rf docker/mysqld2