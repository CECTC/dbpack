/*
 * Copyright 2022 CECTC, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dt

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/pingcap/errors"
	"k8s.io/client-go/util/workqueue"

	"github.com/cectc/dbpack/pkg/config"
	"github.com/cectc/dbpack/pkg/dt/api"
	"github.com/cectc/dbpack/pkg/dt/metrics"
	"github.com/cectc/dbpack/pkg/dt/storage"
	"github.com/cectc/dbpack/pkg/log"
	"github.com/cectc/dbpack/pkg/misc"
	"github.com/cectc/dbpack/pkg/misc/uuid"
	"github.com/cectc/dbpack/pkg/resource"
)

const (
	// CommitRequestPath represents for tcc commit request path
	CommitRequestPath = "tcc_commit_request_path"

	// RollbackRequestPath represents for tcc rollback request path
	RollbackRequestPath = "tcc_rollback_request_path"

	// DefaultRetryDeadThreshold is max retry milliseconds
	DefaultRetryDeadThreshold = 130 * 1000
)

var (
	VarHost        = "host"
	VarQueryString = "queryString"
	manager        *DistributedTransactionManager
)

func InitDistributedTransactionManager(conf *config.DistributedTransaction, storageDriver storage.Driver) {
	if conf.RetryDeadThreshold == 0 {
		conf.RetryDeadThreshold = DefaultRetryDeadThreshold
	}

	manager = &DistributedTransactionManager{
		applicationID:                    conf.ApplicationID,
		storageDriver:                    storageDriver,
		retryDeadThreshold:               conf.RetryDeadThreshold,
		rollbackRetryTimeoutUnlockEnable: conf.RollbackRetryTimeoutUnlockEnable,

		globalSessionQueue: workqueue.NewDelayingQueue(),
		branchSessionQueue: workqueue.New(),
	}
	go func() {
		if storageDriver.LeaderElection(manager.applicationID) {
			if err := manager.processGlobalSessions(); err != nil {
				log.Fatal(err)
			}
			if err := manager.processBranchSessions(); err != nil {
				log.Fatal(err)
			}
			go manager.processGlobalSessionQueue()
			go manager.processBranchSessionQueue()
			go manager.watchBranchSession()
		}
	}()
}

func GetDistributedTransactionManager() *DistributedTransactionManager {
	return manager
}

type DistributedTransactionManager struct {
	applicationID                    string
	storageDriver                    storage.Driver
	retryDeadThreshold               int64
	rollbackRetryTimeoutUnlockEnable bool

	globalSessionQueue workqueue.DelayingInterface
	branchSessionQueue workqueue.Interface
}

func (manager *DistributedTransactionManager) Begin(ctx context.Context, transactionName string, timeout int32) (string, error) {
	transactionID := uuid.NextID()
	xid := fmt.Sprintf("gs/%s/%d", manager.applicationID, transactionID)
	gt := &api.GlobalSession{
		XID:             xid,
		ApplicationID:   manager.applicationID,
		TransactionID:   transactionID,
		TransactionName: transactionName,
		Timeout:         timeout,
		BeginTime:       int64(misc.CurrentTimeMillis()),
		Status:          api.Begin,
	}
	if err := manager.storageDriver.AddGlobalSession(ctx, gt); err != nil {
		return "", err
	}
	metrics.GlobalTransactionCounter.WithLabelValues(manager.applicationID, transactionName, metrics.TransactionStatusActive).Inc()
	manager.globalSessionQueue.AddAfter(gt, time.Duration(timeout)*time.Millisecond)
	log.Infof("successfully begin global transaction xid = {%s}", gt.XID)
	return xid, nil
}

func (manager *DistributedTransactionManager) Commit(ctx context.Context, xid string) (api.GlobalSession_GlobalStatus, error) {
	return manager.storageDriver.GlobalCommit(ctx, xid)
}

func (manager *DistributedTransactionManager) Rollback(ctx context.Context, xid string) (api.GlobalSession_GlobalStatus, error) {
	return manager.storageDriver.GlobalRollback(ctx, xid)
}

func (manager *DistributedTransactionManager) BranchRegister(ctx context.Context, in *api.BranchRegisterRequest) (string, int64, error) {
	branchSessionID := uuid.NextID()
	branchID := fmt.Sprintf("bs/%s/%d", manager.applicationID, branchSessionID)
	transactionID := misc.GetTransactionID(in.XID)
	bs := &api.BranchSession{
		BranchID:        branchID,
		ApplicationID:   manager.applicationID,
		BranchSessionID: branchSessionID,
		XID:             in.XID,
		TransactionID:   transactionID,
		ResourceID:      in.ResourceID,
		LockKey:         in.LockKey,
		Type:            in.BranchType,
		Status:          api.Registered,
		ApplicationData: in.ApplicationData,
		BeginTime:       int64(misc.CurrentTimeMillis()),
	}

	if err := manager.storageDriver.AddBranchSession(ctx, bs); err != nil {
		return "", 0, err
	}
	metrics.BranchTransactionCounter.WithLabelValues(manager.applicationID, in.ResourceID, metrics.TransactionStatusActive).Inc()
	return branchID, branchSessionID, nil
}

func (manager *DistributedTransactionManager) BranchReport(ctx context.Context, branchID string, status api.BranchSession_BranchStatus) error {
	return manager.storageDriver.BranchReport(ctx, branchID, status)
}

func (manager *DistributedTransactionManager) ReleaseLockKeys(ctx context.Context, resourceID string, lockKeys []string) (bool, error) {
	return manager.storageDriver.ReleaseLockKeys(ctx, resourceID, lockKeys)
}

func (manager *DistributedTransactionManager) IsLockable(ctx context.Context, resourceID, lockKey string) (bool, error) {
	return manager.storageDriver.IsLockable(ctx, resourceID, lockKey)
}

func (manager *DistributedTransactionManager) branchCommit(bs *api.BranchSession) (api.BranchSession_BranchStatus, error) {
	var (
		status api.BranchSession_BranchStatus
		err    error
	)
	switch bs.Type {
	case api.TCC:
		status, err = manager.tccBranchCommit(bs)
	case api.AT:
		status, err = manager._branchCommit(bs)
	default:
		return bs.Status, errors.New("should never happen!")
	}
	if status == api.Complete {
		if err := manager.storageDriver.DeleteBranchSession(context.Background(), bs.BranchID); err != nil {
			log.Error(err)
		}
		log.Debugf("branch session committed, branch id: %s, lock key: %s", bs.BranchID, bs.LockKey)
	}
	return status, err
}

func (manager *DistributedTransactionManager) _branchCommit(bs *api.BranchSession) (api.BranchSession_BranchStatus, error) {
	db := resource.GetDBManager().GetDB(bs.ResourceID)
	if db == nil {
		return 0, fmt.Errorf("DB resource is not exist, db name: %s", bs.ResourceID)
	}

	if err := GetUndoLogManager().DeleteUndoLogByXID(db, bs.XID); err != nil {
		return api.PhaseTwoCommitting, err
	}
	return api.Complete, nil
}

func (manager *DistributedTransactionManager) branchRollback(bs *api.BranchSession) (api.BranchSession_BranchStatus, error) {
	var (
		status   api.BranchSession_BranchStatus
		lockKeys []string
		err      error
	)
	switch bs.Type {
	case api.TCC:
		status, err = manager.tccBranchRollback(bs)
	case api.AT:
		status, lockKeys, err = manager._branchRollback(bs)
	default:
		return bs.Status, errors.New("should never happen!")
	}
	if len(lockKeys) > 0 {
		if _, err := manager.storageDriver.ReleaseLockKeys(context.Background(), bs.ResourceID, lockKeys); err != nil {
			log.Errorf("release lock and remove branch session failed, xid = %s, resource_id = %s, lockKeys = %s",
				bs.XID, bs.ResourceID, lockKeys)
		}
	}
	if status == api.Complete {
		if err := manager.storageDriver.DeleteBranchSession(context.Background(), bs.BranchID); err != nil {
			log.Error(err)
		}
		log.Debugf("branch session rollbacked, branch id: %s, lock key: %s", bs.BranchID, bs.LockKey)
	}
	return status, err
}

func (manager *DistributedTransactionManager) _branchRollback(bs *api.BranchSession) (api.BranchSession_BranchStatus, []string, error) {
	db := resource.GetDBManager().GetDB(bs.ResourceID)
	if db == nil {
		return 0, nil, fmt.Errorf("DB resource is not exist, db name: %s", bs.ResourceID)
	}

	lockKeys, err := GetUndoLogManager().Undo(db, bs.XID)
	if err != nil {
		log.Errorf("[stacktrace]branchRollback failed. xid:[%s], branchID:[%d], resourceID:[%s], branchType:[%d], applicationData:[%s], error: %v",
			bs.XID, bs.BranchID, bs.ResourceID, bs.Type, bs.ApplicationData, err)
		return bs.Status, lockKeys, err
	}
	// branch session phase one rollbacked
	if len(lockKeys) == 0 {
		if _, err := manager.storageDriver.ReleaseLockKeys(context.Background(), bs.ResourceID, []string{bs.LockKey}); err != nil {
			return bs.Status, nil, err
		}
	}
	return api.Complete, lockKeys, nil
}

func (manager *DistributedTransactionManager) processGlobalSessions() error {
	ctx := context.Background()
	globalSessions, err := manager.storageDriver.ListGlobalSession(ctx, manager.applicationID)
	if err != nil {
		return err
	}
	for _, gs := range globalSessions {
		if gs.Status == api.Begin {
			if isGlobalSessionTimeout(gs) {
				if _, err := manager.Rollback(ctx, gs.XID); err != nil {
					return err
				}
			}

			delayAt := uint64(gs.Timeout) - (misc.CurrentTimeMillis() - uint64(gs.BeginTime))
			if delayAt > 0 {
				manager.globalSessionQueue.AddAfter(gs, time.Duration(delayAt))
			} else {
				manager.globalSessionQueue.Add(gs)
			}
		}
		if gs.Status == api.Committing || gs.Status == api.Rollbacking {
			bsKeys, err := manager.storageDriver.GetBranchSessionKeys(context.Background(), gs.XID)
			if err != nil {
				return err
			}
			// branch session has been committed or rollbacked
			if len(bsKeys) == 0 {
				if err := manager.storageDriver.DeleteGlobalSession(context.Background(), gs.XID); err != nil {
					return err
				}
				log.Debugf("global session finished, key: %s", gs.XID)
				switch gs.Status {
				case api.Committing:
					manager.recordGlobalTransactionMetric(gs.TransactionName, metrics.TransactionStatusCommitted)
				case api.Rollbacking:
					manager.recordGlobalTransactionMetric(gs.TransactionName, metrics.TransactionStatusRollbacked)
				}
			} else {
				// global transaction timeout
				manager.recordGlobalTransactionMetric(gs.TransactionName, metrics.TransactionStatusTimeout)
			}
		}
	}
	return nil
}

func (manager *DistributedTransactionManager) processGlobalSessionQueue() {
	for manager.processNextGlobalSession(context.Background()) {
	}
}

func (manager *DistributedTransactionManager) processNextGlobalSession(ctx context.Context) bool {
	obj, shutdown := manager.globalSessionQueue.Get()
	if shutdown {
		// Stop working
		return false
	}

	// We call Done here so the workqueue knows we have finished
	// processing this item. We also must remember to call Forget if we
	// do not want this work item being re-queued. For example, we do
	// not call Forget if a transient error occurs, instead the item is
	// put back on the workqueue and attempted again after a back-off
	// period.
	defer manager.globalSessionQueue.Done(obj)

	gs := obj.(*api.GlobalSession)
	newGlobalSession, err := manager.storageDriver.GetGlobalSession(ctx, gs.XID)
	if err != nil {
		log.Error(err)
		return true
	}
	if newGlobalSession.Status == api.Begin {
		if isGlobalSessionTimeout(newGlobalSession) {
			_, err = manager.Rollback(context.Background(), newGlobalSession.XID)
			if err != nil {
				log.Error(err)
			}
		}
	}
	if newGlobalSession.Status == api.Committing || newGlobalSession.Status == api.Rollbacking {
		bsKeys, err := manager.storageDriver.GetBranchSessionKeys(context.Background(), newGlobalSession.XID)
		if err != nil {
			log.Error(err)
		}
		if len(bsKeys) == 0 {
			if err := manager.storageDriver.DeleteGlobalSession(context.Background(), newGlobalSession.XID); err != nil {
				log.Error(err)
			}
			log.Debugf("global session finished, key: %s", newGlobalSession.XID)
			switch newGlobalSession.Status {
			case api.Committing:
				manager.recordGlobalTransactionMetric(gs.TransactionName, metrics.TransactionStatusCommitted)
			case api.Rollbacking:
				manager.recordGlobalTransactionMetric(gs.TransactionName, metrics.TransactionStatusRollbacked)
			}
		} else {
			// global transaction timeout.
			manager.recordGlobalTransactionMetric(gs.TransactionName, metrics.TransactionStatusRollbacked)
		}
	}
	return true
}

func (manager *DistributedTransactionManager) processBranchSessions() error {
	branchSessions, err := manager.storageDriver.ListBranchSession(context.Background(), manager.applicationID)
	if err != nil {
		return err
	}
	for _, bs := range branchSessions {
		switch bs.Status {
		case api.Registered:
		case api.PhaseOneFailed:
			if err := manager.storageDriver.DeleteBranchSession(context.Background(), bs.BranchID); err != nil {
				return err
			}
		case api.PhaseTwoCommitting:
			manager.branchSessionQueue.Add(bs)
		case api.PhaseTwoRollbacking:
			if manager.IsRollingBackDead(bs) {
				metrics.BranchTransactionCounter.WithLabelValues(bs.ApplicationID, bs.ResourceID, metrics.TransactionStatusTimeout)
				log.Debugf("branch session rollback dead, key: %s, lock key: %s", bs.BranchID, bs.LockKey)
				if manager.rollbackRetryTimeoutUnlockEnable {
					log.Debugf("branch id: %s, lock key: %s released", bs.BranchID, bs.LockKey)
					if _, err := manager.storageDriver.ReleaseLockKeys(context.Background(), bs.ResourceID, []string{bs.LockKey}); err != nil {
						return err
					}
				}
			} else {
				manager.branchSessionQueue.Add(bs)
			}
		}
	}
	return nil
}

func (manager *DistributedTransactionManager) processBranchSessionQueue() {
	for manager.processNextBranchSession(context.Background()) {
	}
}

func (manager *DistributedTransactionManager) processNextBranchSession(ctx context.Context) bool {
	obj, shutdown := manager.branchSessionQueue.Get()
	if shutdown {
		// Stop working
		return false
	}

	// We call Done here so the workqueue knows we have finished
	// processing this item. We also must remember to call Forget if we
	// do not want this work item being re-queued. For example, we do
	// not call Forget if a transient error occurs, instead the item is
	// put back on the workqueue and attempted again after a back-off
	// period.
	defer manager.branchSessionQueue.Done(obj)

	bs := obj.(*api.BranchSession)
	var (
		status            api.BranchSession_BranchStatus
		transactionStatus string
		err               error
	)
	if bs.Status == api.PhaseTwoCommitting {
		transactionStatus = metrics.TransactionStatusCommitted
		status, err = manager.branchCommit(bs)
		if err != nil {
			log.Error(err)
			manager.branchSessionQueue.Add(obj)
		}
		if status != api.Complete {
			manager.branchSessionQueue.Add(obj)
		}
	}
	if bs.Status == api.PhaseTwoRollbacking {
		transactionStatus = metrics.TransactionStatusRollbacked
		if manager.IsRollingBackDead(bs) {
			metrics.BranchTransactionCounter.WithLabelValues(bs.ApplicationID, bs.ResourceID, metrics.TransactionStatusTimeout)
			if manager.rollbackRetryTimeoutUnlockEnable {
				if _, err := manager.storageDriver.ReleaseLockKeys(ctx, bs.ResourceID, []string{bs.LockKey}); err != nil {
					log.Error(err)
				}
			}
		} else {
			status, err = manager.branchRollback(bs)
			if err != nil {
				log.Error(err)
				manager.branchSessionQueue.Add(obj)
			}
			if status != api.Complete {
				manager.branchSessionQueue.Add(obj)
			}
		}
	}

	if status == api.Complete {
		metrics.BranchTransactionTimer.WithLabelValues(manager.applicationID, bs.ResourceID, transactionStatus).Observe(
			float64(int64(misc.CurrentTimeMillis()) - bs.BeginTime))
		metrics.BranchTransactionCounter.WithLabelValues(manager.applicationID, bs.ResourceID, metrics.TransactionStatusActive).Desc()
		metrics.BranchTransactionCounter.WithLabelValues(manager.applicationID, bs.ResourceID, transactionStatus).Inc()
	}

	return true
}

func (manager *DistributedTransactionManager) watchBranchSession() {
	watcher := manager.storageDriver.WatchBranchSessions(context.Background(), manager.applicationID)
	for {
		bs := <-watcher.ResultChan()
		manager.branchSessionQueue.Add(bs)
	}
}

func (manager *DistributedTransactionManager) recordGlobalTransactionMetric(transactionName string, transactionStatus string) {
	metrics.GlobalTransactionCounter.WithLabelValues(manager.applicationID, transactionName, metrics.TransactionStatusActive).Desc()
	metrics.GlobalTransactionCounter.WithLabelValues(manager.applicationID, transactionName, transactionStatus).Inc()
}

func isGlobalSessionTimeout(gs *api.GlobalSession) bool {
	return (misc.CurrentTimeMillis() - uint64(gs.BeginTime)) > uint64(gs.Timeout)
}

func (manager *DistributedTransactionManager) IsRollingBackDead(bs *api.BranchSession) bool {
	return (misc.CurrentTimeMillis() - uint64(bs.BeginTime)) > uint64(manager.retryDeadThreshold)
}

func (manager *DistributedTransactionManager) tccBranchCommit(bs *api.BranchSession) (api.BranchSession_BranchStatus, error) {
	requestContext := &RequestContext{
		ActionContext: make(map[string]string),
		Headers:       []byte{},
		Body:          []byte{},
	}
	err := requestContext.Decode(bs.ApplicationData)
	if err != nil {
		return api.PhaseTwoCommitting, fmt.Errorf("error decoding bs.ApplicationData: %v", err)
	}

	resp, err := manager.doHttpRequest(requestContext, true)
	if err != nil {
		return api.PhaseTwoCommitting, fmt.Errorf("error doHttpRequest for tccBranchCommit: %v", err)
	}
	if resp.StatusCode() != http.StatusOK {
		return api.PhaseTwoCommitting, fmt.Errorf("error tccBranchCommit response code %d", resp.StatusCode())
	}
	return api.Complete, nil
}

func (manager *DistributedTransactionManager) tccBranchRollback(bs *api.BranchSession) (api.BranchSession_BranchStatus, error) {
	requestContext := &RequestContext{
		ActionContext: make(map[string]string),
		Headers:       []byte{},
		Body:          []byte{},
	}
	err := requestContext.Decode(bs.ApplicationData)
	if err != nil {
		return api.PhaseTwoRollbacking, fmt.Errorf("error decoding bs.ApplicationData: %v", err)
	}

	resp, err := manager.doHttpRequest(requestContext, false)
	if err != nil {
		return api.PhaseTwoRollbacking, fmt.Errorf("error doHttpRequest for tccBranchRollback: %v", err)
	}
	if resp.StatusCode() != http.StatusOK {
		return api.PhaseTwoRollbacking, fmt.Errorf("error tccBranchRollback response code %d", resp.StatusCode())
	}
	return api.Complete, nil
}

func (manager *DistributedTransactionManager) doHttpRequest(requestContext *RequestContext, commit bool) (*resty.Response, error) {
	var (
		host        string
		path        string
		queryString string
	)
	host = requestContext.ActionContext[VarHost]
	if commit {
		path = requestContext.ActionContext[CommitRequestPath]
	} else {
		path = requestContext.ActionContext[RollbackRequestPath]
	}

	u := url.URL{
		Scheme: "http",
		Path:   path,
		Host:   host,
	}
	queryString, ok := requestContext.ActionContext[VarQueryString]
	if ok {
		u.RawQuery = queryString
	}

	client := resty.New()
	request := client.R()

	headers := make(map[string]string)
	err := json.Unmarshal(requestContext.Headers, &headers)
	if err != nil {
		return nil, fmt.Errorf("error json.Unmarshal requestContext.Headers: %v", err)
	}
	request.SetHeaders(headers)
	request.SetBody(requestContext.Body)

	return request.Post(u.String())
}
