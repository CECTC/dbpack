#!/bin/sh

mkdir -p docker/data
mkdir -p docker/mysqld
docker-compose -f docker/docker-compose-sdb.yaml up -d

sleep 90

go clean -testcache
go test -tags integration -v ./test/sdb/

docker logs dbpack

docker-compose -f docker/docker-compose-sdb.yaml down
rm -rf docker/data
rm -rf docker/mysqld