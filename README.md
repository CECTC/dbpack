# DBPack
[![LICENSE](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](https://github.com/cectc/dbpack/blob/dev/LICENSE)
[![codecov](https://codecov.io/gh/CECTC/dbpack/branch/dev/graph/badge.svg?token=2AVE9EHLXO)](https://codecov.io/gh/CECTC/dbpack)
![GitHub Workflow Status](https://github.com/cectc/dbpack/actions/workflows/main.yml/badge.svg)
[![Go Report Card](https://goreportcard.com/badge/github.com/cectc/dbpack)](https://goreportcard.com/report/github.com/cectc/dbpack)
[![GitHub Release](https://img.shields.io/github/release-pre/cectc/dbpack.svg)](https://github.com/cectc/dbpack/releases)


<img src="https://cectc.github.io/dbpack-doc/images/dbpack.svg" alt="dbpack.svg"/>

> DBPack means a database cluster tool pack. It can be deployed as a sidecar in a pod, it shields complex basic logic, so that business development does not need to rely on a specific SDK, simplifying the development process and improving development efficiency.


## Feature

+ support MYSQL protocol.
+ simple and easy distributed transaction solution inspired by kubernetes.
+ support read write splitting，supports custom SQL routing via Hint.
+ support audit log, support sql tracing.
+ sharding: support query on sharded table, support order by，support limit.
+ sharding: support insert、delete、update on sharded table
+ deployed as a sidecar，support any language
+ more features on the road

## Metrics
using prometheus metrics, default metric server run at :18888

## Documentation
+ [Official Website](https://cectc.github.io/dbpack-doc/)
+ [Samples](https://github.com/CECTC/dbpack-samples)

## Requirements

+ Go 1.17 or higher
+ MYSQL (5.7+)

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
