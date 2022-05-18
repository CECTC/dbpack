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

vim ./samples/python/config1.yaml
# update distributed_transaction.etcd_config.endpoints

vim ./samples/python/config2.yaml
# update data_source_cluster.dsn
# update distributed_transaction.etcd_config.endpoints

vim ./samples/python/config3.yaml
# update data_source_cluster.dsn
# update distributed_transaction.etcd_config.endpoints

./dist/dbpack start --config samples/python/config1.yaml

./dist/dbpack start --config samples/python/config2.yaml

./dist/dbpack start --config samples/python/config3.yaml
```

### Step4: setup python requirements
```bash
cd samples/python
pip3 install -r requirements.txt
```

### Step5: setup aggregation client
```bash
cd samples/python/aggregation

python3 app.py
```

### Step6: setup order client
```bash
cd samples/python/order

python3 app.py
```

### Step7: setup product client
```bash
cd samples/python/product

python3 app.py
```

### Step8: access and test
```
curl -XPOST http://localhost:13000/v1/order/create
```