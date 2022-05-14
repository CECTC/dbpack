# dbpack-samples

### Step0: Clone dbpack
```shell
git clone git@github.com:cectc/dbpack.git
cd dbpack
```

### Step1: Setup etcd

### Step2: Setup mysql, initialize the database with the following sql script
```
./samples/go/scripts/order.sql
./samples/go/scripts/product.sql
```

### Step3: run dbpack
```bash
make build-local

vim ./samples/go/config1.yaml
# update distributed_transaction.etcd_config.endpoints

vim ./samples/go/config2.yaml
# update data_source_cluster.dsn
# update distributed_transaction.etcd_config.endpoints

vim ./samples/go/config3.yaml
# update data_source_cluster.dsn
# update distributed_transaction.etcd_config.endpoints

./dist/dbpack start -config ../samples/go/config1.yml

./dist/dbpack start -config ../samples/go/config2.yml

./dist/dbpack start -config ../samples/go/config3.yml
```

### Step4: setup aggregation_svc client
```bash
cd samples/go/

go run aggregation_svc/main.go
```

### Step5: setup order_svc client
```bash
cd samples/go/
vim ./order_svc/main.go
# update dsn

go run order_svc/main.go
```

### Step6: setup product_svc client
```bash
cd samples/go/
vim ./product_svc/main.go
# update dsn

go run product_svc/main.go
```

### Step7: access and test
```
curl -XPOST http://localhost:13000/v1/order/create
```