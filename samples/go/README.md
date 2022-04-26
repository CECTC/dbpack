# dbpack-samples

### Step0: Setup etcd
### Step1: Setup mysql, initialize the database with the following sql script
```
./samples/go/scripts/order.sql
./samples/go/scripts/product.sql
```

```bash
git clone git@github.com:CECTC/dbpack.git
cd dbpack
make build

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

### Step2: setup aggregation_svc client
```bash
cd samples/go/

go run aggregation_svc/main.go
```

### Step2: setup order_svc client
```bash
cd samples/go/
vim ./order_svc/main.go
# update dsn

go run order_svc/main.go
```

### Step3: setup product_svc client
```bash
cd samples/go/
vim ./product_svc/main.go
# update dsn

go run product_svc/main.go
```

### Step4: access
```
curl -XPOST http://localhost:13000/v1/order/create
```