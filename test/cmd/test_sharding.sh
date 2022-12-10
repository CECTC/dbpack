#!/bin/sh

docker-compose -f docker/docker-compose-shd-mod.yaml up --build -d

sleep 90

go clean -testcache
go test -tags integration -v ./test/shd_mod/

docker logs dbpack

docker-compose -f docker/docker-compose-shd-mod.yaml down

docker-compose -f docker/docker-compose-shd-range.yaml up --build -d

sleep 90

go clean -testcache
go test -tags integration -v ./test/shd_range/

docker logs dbpack

docker-compose -f docker/docker-compose-shd-range.yaml down