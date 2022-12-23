# ChangeLog

## v0.1.0

### Bug Fixes

* add terminationDrainDuration config ([#4](https://github.com/cectc/dbpack/issues/4)) ([5c607d4](https://github.com/cectc/dbpack/commit/5c607d48d1149218cff3988dcb00d83da571a561))
* should use db connection rather than tx connection exec sql request ([#8](https://github.com/cectc/dbpack/pull/8)) ([52d78ca](https://github.com/cectc/dbpack/commit/52d78cab0bc414d92a5c59230f2827c8332c2bde))
* when receive ComQuit request, should return connection ([#51](https://github.com/cectc/dbpack/pull/51)) ([e8f0708](https://github.com/cectc/dbpack/commit/e8f07086ccf76a7112f00512e3ed3f6e94aff410))
* close statement after read result from conn ([#71](https://github.com/cectc/dbpack/pull/71)) ([4c9a292](https://github.com/cectc/dbpack/commit/4c9a29271d73df0ff8daf92c3faebf1540b0cf01))
* ping should put resource back to the pool ([#74](https://github.com/cectc/dbpack/pull/74)) ([c1c7771](https://github.com/cectc/dbpack/commit/c1c77710398ad58d7d3809ad66312550b0931236))
* process global session after timeout should refresh global session status ([#86](https://github.com/cectc/dbpack/pull/86)) ([6bf4090](https://github.com/cectc/dbpack/commit/6bf4090fbe897c60c229ac172fdb0c14720066ee))
* release tx when undologs doesn't exists ([#93](https://github.com/cectc/dbpack/pull/93)) ([99df036](https://github.com/cectc/dbpack/commit/99df0361ca1cf7876daa66151cf6bb462d0fd3bb))

### Features

* distributed transaction support etcd watch ([#11](https://github.com/cectc/dbpack/pull/11)) ([e991050](https://github.com/cectc/dbpack/commit/e9910501e32d23741f99f5fe9ece1077ba1b348c))
* support tcc branch commit & rollback ([#12](https://github.com/cectc/dbpack/issues/12)) ([feab7ae](https://github.com/cectc/dbpack/commit/feab7aefe819bf3217363994c67515b887f8adb9))
* support GlobalLock hint ([#14](https://github.com/cectc/dbpack/issues/14)) ([5c7c967](https://github.com/cectc/dbpack/commit/5c7c96797539943ed75495d1cfa92f6094ff548e))
* support leader election, only leader can commit and rollback ([#19](https://github.com/cectc/dbpack/pull/19)) ([d7ab60b](https://github.com/cectc/dbpack/commit/d7ab60b6ed5547f1bc9a6c426e1fb9ee21d6f4f3))
* add prometheus metric ([#25](https://github.com/cectc/dbpack/issues/25)) ([627adc2](https://github.com/cectc/dbpack/commit/627adc2ced9da499e6b658f718b23417e7df9903))
* add readiness and liveness probe ([#52](https://github.com/cectc/dbpack/issues/52)) ([f43ab5f](https://github.com/cectc/dbpack/commit/f43ab5f4ed6eafaf950a73e241c536849a16e4f9))

### Changes

* update branch session process logic ([#17](https://github.com/cectc/dbpack/pull/17)) ([06d6245](https://github.com/cectc/dbpack/commit/06d624511c65a379e73dae91c2be4fb3785b9bf0))

### New Contributors
* @rocymp made their first contribution in https://github.com/CECTC/dbpack/pull/3
* @gorexlv made their first contribution in https://github.com/CECTC/dbpack/pull/5
* @zackzhangkai made their first contribution in https://github.com/CECTC/dbpack/pull/10
* @yx9o made their first contribution in https://github.com/CECTC/dbpack/pull/32
* @bohehe made their first contribution in https://github.com/CECTC/dbpack/pull/41
* @fatelei made their first contribution in https://github.com/CECTC/dbpack/pull/48
* @zhu733756 made their first contribution in https://github.com/CECTC/dbpack/pull/58
* @wybrobin made their first contribution in https://github.com/CECTC/dbpack/pull/72
* @tanryberdi made their first contribution in https://github.com/CECTC/dbpack/pull/75
* @JuwanXu made their first contribution in https://github.com/CECTC/dbpack/pull/81
* @hzliangbin made their first contribution in https://github.com/CECTC/dbpack/pull/83

## v0.1.1

### Bug Fixes

* enhance etcd client config ([112](https://github.com/CECTC/dbpack/pull/112])) ([798f124](https://github.com/cectc/dbpack/commit/798f124a6b33a4f83a734ac9a971ff8760dcffbf))
* fix global session timeout calculate ([118](https://github.com/CECTC/dbpack/pull/118)) ([bf3b084](https://github.com/cectc/dbpack/commit/bf3b08418485347da83af5c061a59839c1bace9e))
* should sort the columns ([131](https://github.com/CECTC/dbpack/pull/131)) ([f3bc97f](https://github.com/cectc/dbpack/commit/f3bc97fe095c2eaeda0f6d1eb0dabb22a9fc7020))

### Features

* transaction metric ([54](https://github.com/CECTC/dbpack/issues/54)) ([0ae8c77](https://github.com/cectc/dbpack/commit/0ae8c774106b58f5dbbfa045d4a8591fb913c929))
* support com_query update sql request ([104](https://github.com/CECTC/dbpack/issues/104)) ([9eff19f](https://github.com/cectc/dbpack/commit/9eff19fd1e40f6a63722e7e61f84218718c3b956))
* support com_query delete sql request ([104](https://github.com/CECTC/dbpack/issues/104)) ([af98362](https://github.com/cectc/dbpack/commit/af983621f0e6249bacc5a7bf6852d884a302c09e))
* support com_query insert sql request ([104](https://github.com/CECTC/dbpack/issues/104)) ([1aa20fc](https://github.com/cectc/dbpack/commit/1aa20fcb5853f144ad5044649af9112fdb540b69))
* support comQuery request globallock hint ([104](https://github.com/CECTC/dbpack/issues/104)) ([437667e](https://github.com/cectc/dbpack/commit/437667e46b70e4d3ddd91d574495d3a478812204))
* add status api ([139](https://github.com/CECTC/dbpack/pull/139)) ([93ceee0](https://github.com/cectc/dbpack/commit/93ceee08d46420b422f5490c4b6e8f2120805370))

### Changes

* refactor: update global session status, branch session status in txn ([120](https://github.com/CECTC/dbpack/pull/120)) ([9829387](https://github.com/cectc/dbpack/commit/9829387cd519551e9d8c16f1c373712c20a41e6e))

## v0.1.2

### Bug Fixes

* rollback when global session timeout ([145](https://github.com/CECTC/dbpack/pull/145])) ([1798cd1](https://github.com/cectc/dbpack/commit/1798cd1070d7b44e8ad69de70cc71c8f749d5034))
* when register branch session, global session can not change ([146](https://github.com/CECTC/dbpack/issues/146])) ([11ea3dd](https://github.com/cectc/dbpack/commit/11ea3dd1a0e1b40bb195c843b667378816236177))

### Features

* HttpDistributedTransaction filter support prefix matching ([148](https://github.com/CECTC/dbpack/pull/148])) ([cdb757a](https://github.com/cectc/dbpack/commit/cdb757ad70ccd3dc5f07fa7ff22a2c523adc0e6a))
* HttpDistributedTransaction filter support regular matching ([149](https://github.com/CECTC/dbpack/pull/149])) ([cdb757a](https://github.com/cectc/dbpack/commit/cdb757ad70ccd3dc5f07fa7ff22a2c523adc0e6a))

## v0.2.0

### Bug Fixes

* currentRecords may be nil ([157](https://github.com/CECTC/dbpack/pull/157)) ([e7c8747](https://github.com/CECTC/dbpack/commit/e7c8747747144241e9841c56b3fd88e60fc01516))
* php 8.0 pdo will check the flag bit ([163](https://github.com/CECTC/dbpack/pull/163)) ([0fd83bb](https://github.com/CECTC/dbpack/commit/0fd83bb963701efe8a35f1dbafb7b499e055c233))
* borrowed connections should be returned when not in use ([164](https://github.com/CECTC/dbpack/pull/164)) ([b1c8330](https://github.com/CECTC/dbpack/commit/b1c8330af05e2f10c2bbe5c6584f7d1f98ef0945))
* load balance algorithm unmarshal error ([169](https://github.com/CECTC/dbpack/pull/169)) ([7819ce9](https://github.com/CECTC/dbpack/commit/7819ce95250a989104c4994b004b01a7e8801dee))

## Features

* support usedb hint for customize the sql routing ([13](https://github.com/CECTC/dbpack/issues/13)) ([be37d88](https://github.com/CECTC/dbpack/commit/be37d88f356e807d90fe57ffe17296752f65b7d5))
* audit log filter ([103](https://github.com/CECTC/dbpack/issues/103)) ([e7f819d](https://github.com/CECTC/dbpack/commit/e7f819da076b7534dbc56d90dea3cd6c147eb64d))

## v0.2.1

### Bug Fixes

* in tcc mode, json.Unmarshal requestContext.Headers failed ([194](https://github.com/CECTC/dbpack/issues/194)) ([6e1f8c3](https://github.com/CECTC/dbpack/commit/6e1f8c3b7a9b558e72ed84807cfb0364e1fc89c0))

## v0.2.2

### Changes

* refactor: AuditLog filter PreHandle support more stmtnode type ([198](https://github.com/CECTC/dbpack/pull/198)) ([bb408a5](https://github.com/CECTC/dbpack/commit/bb408a5f7f1f22e5c5a5927e24b0f0316e0b6e12))
* refactor: support manipulating the same data in different transaction… ([199](https://github.com/CECTC/dbpack/issues/199)) ([040b4d1](https://github.com/CECTC/dbpack/commit/040b4d14968ab96c5933fc6dad5968668c151e45))

## v0.3.0

### Features
* add more trace span and integrate with jaeger ([177](https://github.com/CECTC/dbpack/pull/177)) ([d703959](https://github.com/CECTC/dbpack/commit/d70395948677b41759e795be60eb93fcb5dc7f74))
* add trace for BackendConnection ([204](https://github.com/CECTC/dbpack/pull/204)) ([36b3894](https://github.com/CECTC/dbpack/commit/36b38940539e8d14a8550bd1d08695591cd9c491))
* support column encryption ([24](https://github.com/CECTC/dbpack/issues/24)) ([946c190](https://github.com/CECTC/dbpack/commit/946c19013c4030f31fb2afecae7772c8546a5a01))

### Bug Fixes

* fix: construct a TraceContext with the correct traceparent ([203](https://github.com/CECTC/dbpack/pull/203)) ([3ecb2cc](https://github.com/CECTC/dbpack/commit/3ecb2cc3dfcf164f68c0cadd2ea6fe73ed49ceff))

## v0.3.1

### Features

* feature: support gin middleware and grpc interceptor ([212](https://github.com/CECTC/dbpack/issues/212)) ([a74d1d2](https://github.com/CECTC/dbpack/commit/a74d1d201457bc394bdbf0364cb25657fca3e0e2))

## v0.3.1

### Features

* feature: support customize request buffer size for http request ([218](https://github.com/CECTC/dbpack/issues/218)) ([593fd25](https://github.com/CECTC/dbpack/commit/593fd25981a28ce9c83ec49a6a1556e9961d49fe))

### Changes

* chore: print ping failed err ([216](https://github.com/CECTC/dbpack/pull/216)) ([852a0f2](https://github.com/CECTC/dbpack/commit/852a0f21f76a270c4d5e8eba60e7c155f0f8edaa))

## v0.4.0

### Features

* feature: support rate limit filter ([181](https://github.com/CECTC/dbpack/issues/181)) ([8d6877d](https://github.com/CECTC/dbpack/commit/8d6877deef23b8b5f2e793ccf18acfeb120311d5))
* feature: support circuit breaker ([182](https://github.com/CECTC/dbpack/issues/182)) ([7a1a977](https://github.com/CECTC/dbpack/commit/7a1a9779f0f60006df16d2fcde91e2f93bc455cf))
* feat: support savepoint statement ([229](https://github.com/CECTC/dbpack/issues/229)) ([146f75d](https://github.com/CECTC/dbpack/commit/146f75d4b1d9bb64b61f806d6e9e34bb2e09caac))

### Bug Fixes

* fix: mysql 8.0 set transaction isolation error ([226](https://github.com/CECTC/dbpack/pull/226)) ([246386a](https://github.com/CECTC/dbpack/commit/246386aa01810cbe33ca0216b5eb53ee474cb10e))
* fix: restore string without default charset ([227](https://github.com/CECTC/dbpack/pull/227)) ([0daddb0](https://github.com/CECTC/dbpack/commit/0daddb0f6cbe619d63d5ff867d1653c55c390a9c))
* fix: should restore xid hint ([239](https://github.com/CECTC/dbpack/pull/239)) ([66137ba](https://github.com/CECTC/dbpack/commit/66137ba7574fd211bb4102811fd4ee5ea57f8b33))

### Changes

* refactor: refactor config to support deploy as gateway ([231](https://github.com/CECTC/dbpack/pull/231)) ([f5f49bd](https://github.com/CECTC/dbpack/commit/f5f49bd810403aec6102337174e5f5bbb615ae20))
* refactor: isolate filter with appid ([233](https://github.com/CECTC/dbpack/pull/233)) ([12c94c4](https://github.com/CECTC/dbpack/commit/12c94c41919cabd4ad7168ee6917bf6d03fdf8f4))
* refactor: refactor db group ([242](https://github.com/CECTC/dbpack/pull/239)) ([a7ddbfc](https://github.com/CECTC/dbpack/commit/a7ddbfce74cb4071b98b6b63894468492454ccf3))

## v0.5.0

### Features

* feat: support delete on sharded table ([44](https://github.com/CECTC/dbpack/issues/44)) ([a0a5bda](https://github.com/CECTC/dbpack/commit/a0a5bda8b677efe571b861b2045f0f78c2d44a6d))
* feat: support update on sharded table ([45](https://github.com/CECTC/dbpack/issues/45)) ([8dd789e](https://github.com/CECTC/dbpack/commit/8dd789e89598f69c6999abcd8cc3baa5b254c3f9))
* feat: support multiple columns order by ([133](https://github.com/CECTC/dbpack/issues/133)) ([7a213c2](https://github.com/CECTC/dbpack/commit/7a213c2008c8df428dc7d6858a9c0163a578bcf4))
* feat: support limit without orderby ([168](https://github.com/CECTC/dbpack/issues/168)) ([dbff926](https://github.com/CECTC/dbpack/commit/dbff926ece6fd8ef6cd6a71b475f6b5be3b4253b))
* feat: support cross-database delete transactions ([180](https://github.com/CECTC/dbpack/issues/180)) ([90f9d42](https://github.com/CECTC/dbpack/commit/90f9d429a4ad7bb38ade4dba68ab2c6731bf253a))
* feat: support count aggregate func on sharded table ([78](https://github.com/CECTC/dbpack/issues/78)) ([8290190](https://github.com/CECTC/dbpack/commit/82901907ae1d4a7664aa6681385827b47c56b506))
* feature: support cross-database update transactions ([193](https://github.com/CECTC/dbpack/pull/193)) ([b184688](https://github.com/CECTC/dbpack/commit/b184688d24c7b28434f3c05e6933365dccb1ab41))
* feat: support show stmt ([209](https://github.com/CECTC/dbpack/pull/209)) ([1b4e902](https://github.com/CECTC/dbpack/commit/1b4e902e14f65fe784680562e6c9a6c44ad95fe0))
* feat: support query on global table ([134](https://github.com/CECTC/dbpack/issues/134)) ([1d2d32d](https://github.com/CECTC/dbpack/commit/1d2d32da06777760ea2914087df0c99f4562f81c))
* feat: support check conn liveness ([260](https://github.com/CECTC/dbpack/pull/260)) ([59b9ff1](https://github.com/CECTC/dbpack/commit/59b9ff1b5eaa8c5733a798a4f2e0842f4885d20e))
* in sharding mode, support local transaction ([236](https://github.com/CECTC/dbpack/issues/236)) ([97c5e18](https://github.com/CECTC/dbpack/commit/97c5e18f73d76a05c66afa0f7180dca46c5f3b84))
* feat: support xa protocol ([261](https://github.com/CECTC/dbpack/pull/261)) ([6611c17](https://github.com/CECTC/dbpack/commit/6611c17af707f8a95f67e0cad74bd197e77c084a))
* feature: support show table status ([189](https://github.com/CECTC/dbpack/issues/189)) ([f896fc3](https://github.com/CECTC/dbpack/commit/f896fc37609f41a4bcf44a864b1b73a81169f8c9))
* feat: support show columns and show index in sharding mode ([269](https://github.com/CECTC/dbpack/pull/269)) ([2c20554](https://github.com/CECTC/dbpack/commit/2c205547ed6f3824dcdc4eed28729d1e9040c9c9))
* feat: export dead branch session api ([264](https://github.com/CECTC/dbpack/issues/264)) ([fd87df8](https://github.com/CECTC/dbpack/commit/fd87df805d74b10dc4c64e391ca5cf54c9a6e496))
* feat: support number range sharding algorithm ([235](https://github.com/CECTC/dbpack/issues/235)) ([5177880](https://github.com/CECTC/dbpack/commit/51778805b12e0b5d97c811a5a8bdbdd14171b128))
* feat: support create index and drop index in sharding mode ([175](https://github.com/CECTC/dbpack/issues/175)) ([dc372d9](https://github.com/CECTC/dbpack/commit/dc372d94344a71f553d20768ed5ddfbd3c0203f1))
* feat: support show tables in sharding mode ([190](https://github.com/CECTC/dbpack/issues/190)) ([1c353ea](https://github.com/CECTC/dbpack/commit/1c353eabdb93af9019d1f1aac355eb4d1d44111e))
* fix: finished distributed pk feature ([42](https://github.com/CECTC/dbpack/issues/42)) ([8bb0903](https://github.com/CECTC/dbpack/commit/8bb0903267e464128a4161b8fe78abcf40c95000))
* feature: support route insert sql to shadow table ([258](https://github.com/CECTC/dbpack/issues/258)) ([5a461ec](https://github.com/CECTC/dbpack/commit/5a461ec74d55c445eab8e6f90e6cb72d484a2826))
* feat: segment sequence support define initial value ([281](https://github.com/CECTC/dbpack/pull/281)) ([6655483](https://github.com/CECTC/dbpack/commit/6655483c4c9b2cf6385d8d4ce94ebf8e90e9d2a1))

### Bug Fixes

* fix: in sharding mode, merge result miss some rows ([214](https://github.com/CECTC/dbpack/pull/214)) ([1aebf5c](https://github.com/CECTC/dbpack/commit/1aebf5c765308b36e40a18910ea654e4a30eaebb))
* fix: support release savepoint ([251](https://github.com/CECTC/dbpack/pull/251)) ([63b19cb](https://github.com/CECTC/dbpack/commit/63b19cbd241822c1b944d5d2928b9b9ec9fcaa2a))
* fix: release savepoint should not return connection ([252](https://github.com/CECTC/dbpack/pull/252)) ([47f1059](https://github.com/CECTC/dbpack/commit/47f10594935a5b30b12ba0867d0805766560fb98))
* fix: match url should support prefix and regex match type ([254](https://github.com/CECTC/dbpack/pull/254)) ([5caeb6c](https://github.com/CECTC/dbpack/commit/5caeb6c0d388c441b4111bb55d529a3f3bed57fe))
* fix: xa commit/rollback should executed by master conn in rws mode ([280](https://github.com/CECTC/dbpack/pull/280)) ([9962372](https://github.com/CECTC/dbpack/commit/9962372a7be1be62699027c1444f009a155aa5f6))

### Changes
 
* refactor: refact topology to support sharding db ([234](https://github.com/CECTC/dbpack/pull/234)) ([a8fe90d](https://github.com/CECTC/dbpack/commit/a8fe90d9671b8b6ec32a81784fe75cfeb153b7dc))
* refactor: default restore string without default charset ([253](https://github.com/CECTC/dbpack/pull/253)) ([4d6da29](https://github.com/CECTC/dbpack/commit/4d6da291c0160d94a36773a53606150570b21351))
* refactor status api ([265](https://github.com/CECTC/dbpack/issues/265)) ([dca3b50](https://github.com/CECTC/dbpack/commit/dca3b507cede4d8f339a047e39478417ed7ec95f))
* refactor: optimize sequence generator ([42](https://github.com/CECTC/dbpack/issues/42)) ([67d9506](https://github.com/CECTC/dbpack/commit/67d9506c59a4fddd3a4342ff75235d1963cd09df))
