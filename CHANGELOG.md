# ChangeLog

# v0.1.0

### Bug Fixes

* add terminationDrainDuration config ([#4](https://github.com/cectc/dbpack/issues/4)) [6604ce8](https://github.com/cectc/dbpack/commit/5c607d48d1149218cff3988dcb00d83da571a561)
* should use db connection rather than tx connection exec sql request ([#8](https://github.com/cectc/dbpack/pull/8)) ([7e2b42d](https://github.com/cectc/dbpack/commit/52d78cab0bc414d92a5c59230f2827c8332c2bde)
* when receive ComQuit request, should return connection ([#51](https://github.com/cectc/dbpack/pull/51)) [627adc2](https://github.com/cectc/dbpack/commit/e8f07086ccf76a7112f00512e3ed3f6e94aff410)
* close statement after read result from conn ([#71](https://github.com/cectc/dbpack/pull/71)) [f924e10](https://github.com/cectc/dbpack/commit/4c9a29271d73df0ff8daf92c3faebf1540b0cf01)
* ping should put resource back to the pool ([#74](https://github.com/cectc/dbpack/pull/74)) [07de56e](https://github.com/cectc/dbpack/commit/c1c77710398ad58d7d3809ad66312550b0931236)
* process global session after timeout should refresh global session status ([#86](https://github.com/cectc/dbpack/pull/86)) [3046e17](https://github.com/cectc/dbpack/commit/6bf4090fbe897c60c229ac172fdb0c14720066ee)
* release tx when undologs doesn't exists ([#93](https://github.com/cectc/dbpack/pull/93)) [7aeaa4e](https://github.com/cectc/dbpack/commit/99df0361ca1cf7876daa66151cf6bb462d0fd3bb)

### Features

* distributed transaction support etcd watch ([#11](https://github.com/cectc/dbpack/pull/11)) ([ce10990](https://github.com/cectc/dbpack/commit/e9910501e32d23741f99f5fe9ece1077ba1b348c)
* support tcc branch commit & rollback ([#12](https://github.com/cectc/dbpack/issues/12)) ([c0bfdf9](https://github.com/cectc/dbpack/commit/feab7aefe819bf3217363994c67515b887f8adb9)
* support GlobalLock hint ([#14](https://github.com/cectc/dbpack/issues/14)) ([8369f8f](https://github.com/cectc/dbpack/commit/5c7c96797539943ed75495d1cfa92f6094ff548e)
* support leader election, only leader can commit and rollback ([#19](https://github.com/cectc/dbpack/pull/19)) ([b89c672](https://github.com/cectc/dbpack/commit/d7ab60b6ed5547f1bc9a6c426e1fb9ee21d6f4f3)
* add prometheus metric ([#25](https://github.com/cectc/dbpack/issues/25)) ([627adc2](https://github.com/cectc/dbpack/commit/627adc2ced9da499e6b658f718b23417e7df9903)
* add readiness and liveness probe ([#52](https://github.com/cectc/dbpack/issues/52)) ([fd889cc](https://github.com/cectc/dbpack/commit/f43ab5f4ed6eafaf950a73e241c536849a16e4f9)

### Changes

* update branch session process logic ([#17](https://github.com/cectc/dbpack/pull/17)) ([c6a6626](https://github.com/cectc/dbpack/commit/06d624511c65a379e73dae91c2be4fb3785b9bf0)
