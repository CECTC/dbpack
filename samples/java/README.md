# dbpack-samples

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
make build-local

vim ./samples/java/config1.yaml
# update distributed_transaction.etcd_config.endpoints

vim ./samples/java/config2.yaml
# update data_source_cluster.dsn
# update distributed_transaction.etcd_config.endpoints

vim ./samples/java/config3.yaml
# update data_source_cluster.dsn
# update distributed_transaction.etcd_config.endpoints

./dist/dbpack start --config ./samples/java/config1.yaml

./dist/dbpack start --config ./samples/java/config2.yaml

./dist/dbpack start --config ./samples/java/config3.yaml
```

### Step4: setup aggregation_svc client
```bash
cd samples/java/aggregation
mvnw package
java -jar target/aggregation-0.0.1-SNAPSHOT.jar
```

### Step5: setup order_svc client
```bash
cd samples/java/order
vim ./src/main/resources/application.properties
# update spring.datasource.url

mvnw package
java -jar target/order-0.0.1-SNAPSHOT.jar
```

### Step6: setup product_svc client
```bash
cd samples/java/product
vim ./src/main/resources/application.properties
# update spring.datasource.url

mvnw package
java -jar target/product-0.0.1-SNAPSHOT.jar
```

### Step7: access and test
```
curl -XPOST http://localhost:13000/v1/order/create
```