# DBPack
[![LICENSE](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](https://github.com/cectc/dbpack/blob/dev/LICENSE)
![GitHub Workflow Status](https://github.com/cectc/dbpack/actions/workflows/main.yml/badge.svg)
[![Go Report Card](https://goreportcard.com/badge/github.com/cectc/dbpack)](https://goreportcard.com/report/github.com/cectc/dbpack)
[![codecov](https://codecov.io/gh/CECTC/dbpack/branch/dev/graph/badge.svg?token=2AVE9EHLXO)](https://codecov.io/gh/CECTC/dbpack)
[![GitHub Release](https://img.shields.io/github/release-pre/cectc/dbpack.svg)](https://github.com/cectc/dbpack/releases)
[![Docker Pulls](https://img.shields.io/docker/pulls/cectc/dbpack)](https://hub.docker.com/r/cectc/dbpack/tags)


<img src="https://cectc.github.io/dbpack-doc/images/dbpack.svg" alt="dbpack.svg"/>

> DBPack is a database proxy pack aims to provide a distributed transaction solution of high performance and has no invasion into business logic. It can be deployed as a sidecar in a pod, which can shield complex basic logic, so that business development does not need to rely on a specific SDK, simplifying the development process and improving development efficiency. It supports any programming language and is easy to use.


## Feature

+ support MYSQL protocol.
+ event driven distributed transaction solution inspired by kubernetes.
+ support read write splitting and custom SQL routing via Hint.
+ support audit log and SQL tracing.
+ sharding: support query on sharded table, support `order by` and `limit`.
+ sharding: support `insert`, `delete`, `update` on sharded tables.
+ can be deployed as a sidecar，support any programming language.
+ more features on the road.

## Metrics
using prometheus metrics, default metric server runs at :18888

## Getting Started
+ [Official Document](https://cectc.github.io/dbpack-doc/#/en-us/)
+ [Samples](https://github.com/CECTC/dbpack-samples)

## Requirements

+ Go >= 1.17
+ MYSQL >= 5.7+

## Contribution Guide
+ [Contribution Guide](CONTRIBUTING.md)

## Architecture

![architecture](https://github.com/CECTC/dbpack-doc/blob/master/images/arch-for-dbpack.drawio.png)

+ Listener: responsible for parsing the SQL protocol.
+ Executor: responsible for forwarding SQL requests to the backend physical DB.
+ Filter: responsible for metrics statistics, SQL interception, encryption and decryption of sensitive information, etc.
+ ConnectionFilter: process the intercepted SQL on the connection.

## WeChat group
To enter the WeChat discussion group, please scan the QR code below and reply with the code "join the group".

<img src="https://cectc.github.io/dbpack-doc/images/image-20220427091848831.png" alt="image-20220427091848831" width="200px"/>

## License
DBPack is licensed under the [Apache License, Version 2.0](LICENSE).
