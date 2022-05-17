# dbpack-samples for php

### Prerequisite
- php version >= php7
- php pdo extension is installed
- when using pdo to connect to mysql server, should set `PDO::ATTR_EMULATE_PREPARES => false` to enable dpback distributed transaction

### Step0: Clone dbpack
```shell
git clone git@github.com:cectc/dbpack.git
cd dbpack
```

### Step1: Setup etcd

### Step2: Setup mysql, initialize the database with the following sql script
```
./samples/scripts/order.sql
./samples/scripts/product.sql
```

### Step3: run dbpack
```bash
# local build
make build-local
# production build
make build

vim ./samples/php/config1.yaml
# update distributed_transaction.etcd_config.endpoints

vim ./samples/php/config2.yaml
# update data_source_cluster.dsn
# update distributed_transaction.etcd_config.endpoints

vim ./samples/php/config3.yaml
# update data_source_cluster.dsn
# update distributed_transaction.etcd_config.endpoints

./dist/dbpack start -config ../samples/php/config1.yml

./dist/dbpack start -config ../samples/php/config2.yml

./dist/dbpack start -config ../samples/php/config3.yml
```

### Step4: setup aggregation_svc client
```bash
# config your apache/nginx to use php
# config your apache/nginx to /path/to/dbpack/samples/php/aggregation_svc
```

### Step5: setup order_svc client
```bash
# config your apache/nginx to use php
# config your apache/nginx to /path/to/dbpack/samples/php/order_svc
# update dsn in order_svc/svc/svc.php
```

### Step6: setup product_svc client
```bash
# config your apache/nginx to use php
# config your apache/nginx to /path/to/dbpack/samples/php/product_svc
# update dsn in product_svc/svc/svc.php
```

### Step7: access and test
```
# commit test
curl -XPOST http://localhost:13000/v1/order/create

# rollback test
curl -XPOST http://localhost:13000/v1/order/create2
```