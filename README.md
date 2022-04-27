# DBPack
[![LICENSE](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](https://github.com/opentrx/seata-golang/blob/v2/LICENSE)


> DBPack means a database cluster tool pack. It can be deployed as a sidecar in a pod, it shields complex basic logic, so that business development does not need to rely on a specific SDK, simplifying the development process and improving development efficiency.


## Feature

+ support MYSQL protocol
+ simple and easy distributed transaction solution
+ support read write splitting，supports custom SQL routing via Hint
+ deployed as a sidecar，support any language
+ more features on the road

## Requirements

+ Go 1.17 or higher
+ MYSQL (5.7+)

## Contribution Guide
+ [Contribution Guide](CONTRIBUTING.md)

## Architecture

![architecture](https://github.com/CECTC/dbpack-doc/blob/master/images/sc_20220418101615.png)

+ Listener: responsible for parsing the SQL protocol.
+ Executor: responsible for forwarding SQL requests to the backend physical DB.
+ Filter: responsible for metrics statistics, SQL interception, encryption and decryption of sensitive information, etc.
+ ConnectionFilter: process the intercepted SQL on the connection.

## 讨论群

进入微信讨论群，请扫描下面的二维码，回复“进群”。

<img src="https://cectc.github.io/dbpack-doc/images/image-20220427091848831.png" alt="image-20220427091848831" style="zoom:50%" align="left"/>

## License
DBPack is licensed under the [Apache License, Version 2.0](LICENSE).
