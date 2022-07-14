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
