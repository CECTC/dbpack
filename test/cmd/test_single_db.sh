#!/bin/sh

docker-compose -f docker/docker-compose-sdb.yaml up --build -d

sleep 90

go clean -testcache
go test -tags integration -v ./test/sdb/
echo "-----------------------------dbpack log-----------------------------"
docker logs dbpack
echo "-----------------------------audit log-----------------------------"
cat /root/dbpack/audit.log
docker-compose -f docker/docker-compose-sdb.yaml down