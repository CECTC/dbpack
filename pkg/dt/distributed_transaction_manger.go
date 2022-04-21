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
	"fmt"
	"sync"
	"time"

	"github.com/opentrx/seata-golang/v2/pkg/util/common"
	timeUtils "github.com/opentrx/seata-golang/v2/pkg/util/time"
	"github.com/opentrx/seata-golang/v2/pkg/util/uuid"
	"github.com/uber-go/atomic"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"github.com/cectc/dbpack/pkg/config"
	"github.com/cectc/dbpack/pkg/dt/api"
	"github.com/cectc/dbpack/pkg/dt/storage"
	"github.com/cectc/dbpack/pkg/errors"
	"github.com/cectc/dbpack/pkg/log"
	"github.com/cectc/dbpack/pkg/resource"
)

const AlwaysRetryBoundary = 0
const DefaultRetryDeadThreshold = 130 * 1000

var manager *DistributedTransactionManager

func InitDistributedTransactionManager(conf *config.DistributedTransaction, storageDriver storage.Driver) {
	if conf.RetryDeadThreshold == 0 {
		conf.RetryDeadThreshold = DefaultRetryDeadThreshold
	}
	manager = &DistributedTransactionManager{
		addressing:    conf.Addressing,
		storageDriver: storageDriver,
		keepaliveClientParameters: keepalive.ClientParameters{
			Time:                conf.ClientParameters.Time,
			Timeout:             conf.ClientParameters.Timeout,
			PermitWithoutStream: conf.ClientParameters.PermitWithoutStream,
		},
		tcServiceClients: make(map[string]api.ResourceManagerServiceClient),

		retryDeadThreshold:               conf.RetryDeadThreshold,
		maxCommitRetryTimeout:            conf.MaxCommitRetryTimeout,
		maxRollbackRetryTimeout:          conf.MaxRollbackRetryTimeout,
		rollbackRetryTimeoutUnlockEnable: conf.RollbackRetryTimeoutUnlockEnable,

		asyncCommittingRetryPeriod: conf.AsyncCommittingRetryPeriod,
		committingRetryPeriod:      conf.CommittingRetryPeriod,
		rollingBackRetryPeriod:     conf.RollingBackRetryPeriod,
		timeoutRetryPeriod:         conf.TimeoutRetryPeriod,
	}

	globalTransactionCount, err := storageDriver.CountGlobalSessions(conf.Addressing)
	if err != nil {
		panic(err)
	}
	manager.globalTransactionCount = atomic.NewUint64(uint64(globalTransactionCount))

	go manager.timeoutCheck()
	go manager.processAsyncCommitting()
	go manager.processRetryCommitting()
	go manager.processRetryRollingBack()
}

func GetDistributedTransactionManager() *DistributedTransactionManager {
	return manager
}

type DistributedTransactionManager struct {
	sync.Mutex
	addressing                string
	storageDriver             storage.Driver
	keepaliveClientParameters keepalive.ClientParameters
	tcServiceClients          map[string]api.ResourceManagerServiceClient
	globalTransactionCount    *atomic.Uint64

	retryDeadThreshold               int64
	maxCommitRetryTimeout            int64
	maxRollbackRetryTimeout          int64
	rollbackRetryTimeoutUnlockEnable bool

	asyncCommittingRetryPeriod time.Duration
	committingRetryPeriod      time.Duration
	rollingBackRetryPeriod     time.Duration
	timeoutRetryPeriod         time.Duration
}

// Begin return xid
func (manager *DistributedTransactionManager) Begin(ctx context.Context, in *api.GlobalBeginRequest) (*api.GlobalBeginResponse, error) {
	xid, err := manager.BeginLocal(ctx, in)
	if err != nil {
		return &api.GlobalBeginResponse{
			ResultCode: api.ResultCodeFailed,
			Message:    err.Error(),
		}, nil
	}
	return &api.GlobalBeginResponse{
		ResultCode: api.ResultCodeSuccess,
		XID:        xid,
	}, nil
}

func (manager *DistributedTransactionManager) Commit(ctx context.Context, in *api.GlobalCommitRequest) (*api.GlobalCommitResponse, error) {
	status, err := manager.CommitLocal(ctx, in)
	if err != nil {
		return &api.GlobalCommitResponse{
			ResultCode:   api.ResultCodeFailed,
			Message:      err.Error(),
			GlobalStatus: status,
		}, nil
	}
	return &api.GlobalCommitResponse{
		ResultCode:   api.ResultCodeSuccess,
		GlobalStatus: status,
	}, nil
}

func (manager *DistributedTransactionManager) Rollback(ctx context.Context, in *api.GlobalRollbackRequest) (*api.GlobalRollbackResponse, error) {
	status, err := manager.RollbackLocal(ctx, in)
	if err != nil {
		return &api.GlobalRollbackResponse{
			ResultCode:   api.ResultCodeFailed,
			Message:      err.Error(),
			GlobalStatus: status,
		}, nil
	}
	return &api.GlobalRollbackResponse{
		ResultCode:   api.ResultCodeSuccess,
		GlobalStatus: status,
	}, nil
}

// BranchRegister return branchID
func (manager *DistributedTransactionManager) BranchRegister(ctx context.Context, in *api.BranchRegisterRequest) (*api.BranchRegisterResponse, error) {
	branchID, err := manager.BranchRegisterLocal(ctx, in)
	if err != nil {
		return &api.BranchRegisterResponse{
			ResultCode: api.ResultCodeFailed,
			Message:    err.Error(),
		}, nil
	}
	return &api.BranchRegisterResponse{
		ResultCode: api.ResultCodeSuccess,
		BranchID:   branchID,
	}, nil
}

func (manager *DistributedTransactionManager) BranchReport(ctx context.Context, in *api.BranchReportRequest) (*api.BranchReportResponse, error) {
	err := manager.BranchReportLocal(ctx, in)
	if err != nil {
		return &api.BranchReportResponse{
			ResultCode: api.ResultCodeFailed,
			Message:    err.Error(),
		}, nil
	}

	return &api.BranchReportResponse{
		ResultCode: api.ResultCodeSuccess,
	}, nil
}

func (manager *DistributedTransactionManager) BranchCommit(ctx context.Context, in *api.BranchCommitRequest) (*api.BranchCommitResponse, error) {
	status, err := manager._branchCommit(in)
	if err != nil {
		return &api.BranchCommitResponse{
			ResultCode:   api.ResultCodeFailed,
			Message:      err.Error(),
			XID:          in.XID,
			BranchID:     in.BranchID,
			BranchStatus: status,
		}, nil
	}
	return &api.BranchCommitResponse{
		ResultCode:   api.ResultCodeSuccess,
		XID:          in.XID,
		BranchID:     in.BranchID,
		BranchStatus: status,
	}, nil
}

func (manager *DistributedTransactionManager) BranchRollback(ctx context.Context, in *api.BranchRollbackRequest) (*api.BranchRollbackResponse, error) {
	status, lockKeys, err := manager._branchRollback(in)
	if err != nil {
		return &api.BranchRollbackResponse{
			ResultCode:   api.ResultCodeFailed,
			Message:      err.Error(),
			XID:          in.XID,
			BranchID:     in.BranchID,
			BranchStatus: status,
			LockKeys:     lockKeys,
		}, nil
	}
	return &api.BranchRollbackResponse{
		ResultCode:   api.ResultCodeSuccess,
		XID:          in.XID,
		BranchID:     in.BranchID,
		BranchStatus: status,
		LockKeys:     lockKeys,
	}, nil
}

func (manager *DistributedTransactionManager) BeginLocal(ctx context.Context, request *api.GlobalBeginRequest) (string, error) {
	transactionID := uuid.NextID()
	xid := common.GenerateXID(request.Addressing, transactionID)
	gt := &api.GlobalSession{
		Addressing:      request.Addressing,
		XID:             xid,
		TransactionID:   transactionID,
		TransactionName: request.TransactionName,
		Timeout:         request.Timeout,
		BeginTime:       int64(timeUtils.CurrentTimeMillis()),
		Status:          api.Begin,
		Active:          true,
	}
	err := manager.storageDriver.AddGlobalSession(gt)
	if err != nil {
		return "", err
	}
	log.Infof("successfully BeginLocal global transaction xid = {}", gt.XID)
	manager.globalTransactionCount.Inc()
	return xid, nil
}

func (manager *DistributedTransactionManager) CommitLocal(ctx context.Context, request *api.GlobalCommitRequest) (api.GlobalSession_GlobalStatus, error) {
	gs := manager.storageDriver.FindGlobalSession(request.XID)
	if gs == nil {
		return api.Finished, nil
	}
	shouldCommit, err := func(gs *api.GlobalSession) (bool, error) {
		if gs.Active {
			// Active need persistence
			// Highlight: Firstly, close the session, then no more branch can be registered.
			if err := manager.storageDriver.InactiveGlobalSession(gs); err != nil {
				return false, err
			}
		}
		if err := manager.storageDriver.ReleaseGlobalSessionLock(gs); err != nil {
			return false, err
		}
		if gs.Status == api.Begin {
			if err := manager.storageDriver.UpdateGlobalSessionStatus(gs, api.Committing); err != nil {
				return false, err
			}
			return true, nil
		}
		return false, nil
	}(gs)

	if err != nil {
		return gs.Status, err
	}

	if !shouldCommit {
		if gs.Status == api.AsyncCommitting {
			return api.Committed, nil
		}
		return gs.Status, fmt.Errorf("can not CommitLocal global transaction, xid = %s, status = %s", gs.XID, gs.Status)
	}

	if manager.storageDriver.CanBeAsyncCommitOrRollback(gs) {
		if err = manager.storageDriver.UpdateGlobalSessionStatus(gs, api.AsyncCommitting); err != nil {
			return gs.Status, err
		}
		return api.Committed, nil
	}

	_, err = manager.doGlobalCommit(gs, false)
	if err != nil {
		return gs.Status, err
	}
	return api.Committed, nil
}

func (manager *DistributedTransactionManager) RollbackLocal(ctx context.Context, request *api.GlobalRollbackRequest) (api.GlobalSession_GlobalStatus, error) {
	gs := manager.storageDriver.FindGlobalSession(request.XID)
	if gs == nil {
		return api.Finished, nil
	}
	shouldRollBack, err := func(gt *api.GlobalSession) (bool, error) {
		if gt.Active {
			// Active need persistence
			// Highlight: Firstly, close the session, then no more branch can be registered.
			if err := manager.storageDriver.InactiveGlobalSession(gs); err != nil {
				return false, err
			}
		}
		if gt.Status == api.Begin {
			if err := manager.storageDriver.UpdateGlobalSessionStatus(gs, api.RollingBack); err != nil {
				return false, err
			}
			return true, nil
		}
		return false, nil
	}(gs)

	if err != nil {
		return gs.Status, err
	}

	if !shouldRollBack {
		return gs.Status, fmt.Errorf("can not RollbackLocal global transaction, xid = %s, status = %s", gs.XID, gs.Status)
	}

	_, err = manager.doGlobalRollback(gs, false)
	if err != nil {
		return gs.Status, err
	}
	return gs.Status, nil
}

func (manager *DistributedTransactionManager) BranchRegisterLocal(ctx context.Context, request *api.BranchRegisterRequest) (int64, error) {
	gs := manager.storageDriver.FindGlobalSession(request.XID)
	if gs == nil {
		log.Errorf("could not found global transaction xid = %s", request.XID)
		return 0, errors.CouldNotFoundGlobalTransaction
	}

	if !gs.Active {
		log.Errorf("could not register branch into global session xid = %s status = %d", gs.XID, gs.Status)
		return 0, errors.GlobalTransactionNotActive
	}
	if gs.Status != api.Begin {
		return 0, fmt.Errorf("could not register branch into global session xid = %s status = %d while expecting %d",
			gs.XID, gs.Status, api.Begin)
	}

	bs := &api.BranchSession{
		Addressing:      request.Addressing,
		XID:             request.XID,
		BranchID:        uuid.NextID(),
		TransactionID:   gs.TransactionID,
		ResourceID:      request.ResourceID,
		LockKey:         request.LockKey,
		Type:            request.BranchType,
		Status:          api.Registered,
		ApplicationData: request.ApplicationData,
		Async:           request.Async,
	}

	if bs.Type == api.AT {
		result := manager.storageDriver.IsLockable(bs.XID, bs.ResourceID, bs.LockKey)
		if !result {
			log.Errorf("branch lock acquire failed xid = %s resourceId = %s, lockKey = %s",
				request.XID, request.ResourceID, request.LockKey)
			return 0, errors.BranchLockAcquireFailed
		}
		bs.Async = true
	}

	err := manager.storageDriver.AddBranchSession(gs, bs)
	if err != nil {
		log.Error(err)
		return 0, fmt.Errorf("branch register failed, xid = %s, branchid = %d, err: %s", gs.XID, bs.BranchID, err.Error())
	}

	return bs.BranchID, nil
}

func (manager *DistributedTransactionManager) BranchReportLocal(ctx context.Context, in *api.BranchReportRequest) error {
	gs := manager.storageDriver.FindGlobalSession(in.XID)
	if gs == nil {
		log.Errorf("could not found global transaction xid = %s", in.XID)
		return errors.CouldNotFoundGlobalTransaction
	}
	bs := manager.storageDriver.FindBranchSessionByBranchID(in.BranchID)
	if bs == nil {
		return errors.CouldNotFoundBranchTransaction
	}
	return manager.storageDriver.UpdateBranchSessionStatus(bs, in.BranchStatus)
}

// LockQuery return lockable
func (manager *DistributedTransactionManager) LockQuery(ctx context.Context, request *api.GlobalLockQueryRequest) (bool, error) {
	result := manager.storageDriver.IsLockable(request.XID, request.ResourceID, request.LockKey)
	return result, nil
}

func (manager *DistributedTransactionManager) doGlobalCommit(gs *api.GlobalSession, retrying bool) (bool, error) {
	branchSessions := manager.storageDriver.FindBranchSessions(gs.XID)
	branchSessionsFiltered := filterBranchSessions(branchSessions)
	for i := 0; i < len(branchSessionsFiltered); i++ {
		bs := branchSessionsFiltered[i]
		branchStatus, err := manager.branchCommit(bs)
		if err != nil {
			log.Errorf("exception committing branch xid=%d branchID=%d, err: %v", bs.GetXID(), bs.BranchID, err)
			if !retrying {
				gs.Status = api.CommitRetrying
				if err := manager.storageDriver.UpdateGlobalSessionStatus(gs, api.CommitRetrying); err != nil {
					return false, err
				}
			}
			return false, err
		}
		switch branchStatus {
		case api.PhaseTwoCommitted:
			err := manager.storageDriver.RemoveBranchSession(gs, bs)
			if err != nil {
				return false, err
			}
			continue
		case api.PhaseTwoCommitFailedCanNotRetry:
			{
				if manager.storageDriver.CanBeAsyncCommitOrRollback(gs) {
					log.Errorf("by [%s], failed to CommitLocal branch %v", bs.Status.String(), bs)
					continue
				} else {
					// change status first, if need retention global session data,
					// might not remove global session, then, the status is very important.
					gs.Status = api.CommitFailed
					if err = manager.storageDriver.UpdateGlobalSessionStatus(gs, api.CommitFailed); err != nil {
						return false, err
					}
					if err := manager.storageDriver.RemoveGlobalSession(gs); err != nil {
						return false, err
					}
					log.Errorf("finally, failed to CommitLocal global[%d] since branch[%d] CommitLocal failed", gs.XID, bs.BranchID)
					return false, nil
				}
			}
		default:
			{
				if !retrying {
					gs.Status = api.CommitRetrying
					if err := manager.storageDriver.UpdateGlobalSessionStatus(gs, api.CommitRetrying); err != nil {
						return false, err
					}
					return false, nil
				}
				if manager.storageDriver.CanBeAsyncCommitOrRollback(gs) {
					log.Errorf("by [%s], failed to CommitLocal branch %v", bs.Status.String(), bs)
					continue
				} else {
					log.Errorf("failed to CommitLocal global[%d] since branch[%d] CommitLocal failed, will retry later.", gs.XID, bs.BranchID)
					return false, nil
				}
			}
		}
	}
	branchSessions = manager.storageDriver.FindBranchSessions(gs.XID)
	if len(branchSessions) > 0 {
		log.Infof("global[%d] committing is NOT done.", gs.XID)
		return false, nil
	}

	// change status first, if need retention global session data,
	// might not remove global session, then, the status is very important.
	gs.Status = api.Committed
	if err := manager.storageDriver.UpdateGlobalSessionStatus(gs, api.Committed); err != nil {
		return false, err
	}
	if err := manager.storageDriver.RemoveGlobalSession(gs); err != nil {
		return false, err
	}
	log.Infof("global[%d] committing is successfully done.", gs.XID)
	manager.globalTransactionCount.Dec()
	return true, nil
}

func (manager *DistributedTransactionManager) branchCommit(bs *api.BranchSession) (api.BranchSession_BranchStatus, error) {
	request := &api.BranchCommitRequest{
		XID:             bs.XID,
		BranchID:        bs.BranchID,
		ResourceID:      bs.ResourceID,
		LockKey:         bs.LockKey,
		BranchType:      bs.Type,
		ApplicationData: bs.ApplicationData,
	}

	if bs.Type == api.AT && bs.Addressing == manager.addressing {
		return manager._branchCommit(request)
	}

	client, err := manager.getTransactionCoordinatorServiceClient(bs.Addressing)
	if err != nil {
		return bs.Status, err
	}

	response, err := client.BranchCommit(context.Background(), request)

	if err != nil {
		return bs.Status, err
	}

	if response.ResultCode == api.ResultCodeSuccess {
		return response.BranchStatus, nil
	} else {
		return bs.Status, fmt.Errorf(response.Message)
	}
}

func (manager *DistributedTransactionManager) _branchCommit(in *api.BranchCommitRequest) (api.BranchSession_BranchStatus, error) {
	db := resource.GetDBManager().GetDB(in.ResourceID)
	if db == nil {
		return 0, fmt.Errorf("DB resource is not exist, db name: %s", in.ResourceID)
	}

	if err := GetUndoLogManager().DeleteUndoLogByXID(db, in.XID); err != nil {
		return api.PhaseTwoCommitFailedRetryable, err
	}
	return api.PhaseTwoCommitted, nil
}

func (manager *DistributedTransactionManager) doGlobalRollback(gs *api.GlobalSession, retrying bool) (bool, error) {
	branchSessions := manager.storageDriver.FindBranchSessions(gs.XID)
	branchSessionsFiltered := filterBranchSessions(branchSessions)
	for i := 0; i < len(branchSessionsFiltered); i++ {
		bs := branchSessionsFiltered[i]
		branchStatus, err := manager.branchRollback(bs)
		if err != nil {
			log.Errorf("exception rolling back branch xid=%d branchID=%d, err: %v", gs.XID, bs.BranchID, err)
			if !retrying {
				if isTimeoutGlobalStatus(gs) {
					gs.Status = api.TimeoutRollbackRetrying
					if err := manager.storageDriver.UpdateGlobalSessionStatus(gs, api.TimeoutRollbackRetrying); err != nil {
						return false, err
					}
				} else {
					gs.Status = api.RollbackRetrying
					if err := manager.storageDriver.UpdateGlobalSessionStatus(gs, api.RollbackRetrying); err != nil {
						return false, err
					}
				}
			}
			return false, err
		}
		switch branchStatus {
		case api.PhaseTwoRolledBack:
			if bs.Type == api.TCC {
				if err := manager.storageDriver.RemoveBranchSession(gs, bs); err != nil {
					return false, err
				}
				log.Infof("successfully RollbackLocal branch xid=%d branchID=%d", gs, bs.BranchID)
			}
			continue
		case api.PhaseTwoRollbackFailedCanNotRetry:
			if isTimeoutGlobalStatus(gs) {
				gs.Status = api.TimeoutRollbackFailed
				if err := manager.storageDriver.UpdateGlobalSessionStatus(gs, api.TimeoutRollbackFailed); err != nil {
					return false, err
				}
			} else {
				gs.Status = api.RollbackFailed
				if err := manager.storageDriver.UpdateGlobalSessionStatus(gs, api.RollbackFailed); err != nil {
					return false, err
				}
			}
			if err := manager.storageDriver.ReleaseGlobalSessionLock(gs); err != nil {
				return false, err
			}
			if err := manager.storageDriver.RemoveGlobalSession(gs); err != nil {
				return false, err
			}
			log.Infof("failed to RollbackLocal branch and stop retry xid=%d branchID=%d", gs.XID, bs.BranchID)
			return false, nil
		default:
			log.Infof("failed to RollbackLocal branch xid=%d branchID=%d", gs.XID, bs.BranchID)
			if !retrying {
				if isTimeoutGlobalStatus(gs) {
					gs.Status = api.TimeoutRollbackRetrying
					if err := manager.storageDriver.UpdateGlobalSessionStatus(gs, api.TimeoutRollbackRetrying); err != nil {
						return false, err
					}
				} else {
					gs.Status = api.RollbackRetrying
					if err := manager.storageDriver.UpdateGlobalSessionStatus(gs, api.RollbackRetrying); err != nil {
						return false, err
					}
				}
			}
			return false, nil
		}
	}

	// In db mode, there is a problem of inconsistent data in multiple copies, resulting in new branch
	// transaction registration when rolling back.
	// 1. New branch transaction and RollbackLocal branch transaction have no data association
	// 2. New branch transaction has data association with RollbackLocal branch transaction
	// The second query can solve the first problem, and if it is the second problem, it may cause a RollbackLocal
	// failure due to data changes.
	branchSessions = manager.storageDriver.FindBranchSessions(gs.XID)
	if len(branchSessions) > 0 {
		log.Infof("Global[%d] rolling back is NOT done.", gs.XID)
		return false, nil
	}

	if isTimeoutGlobalStatus(gs) {
		gs.Status = api.TimeoutRolledBack
		if err := manager.storageDriver.UpdateGlobalSessionStatus(gs, api.TimeoutRolledBack); err != nil {
			return false, err
		}
	} else {
		gs.Status = api.RolledBack
		if err := manager.storageDriver.UpdateGlobalSessionStatus(gs, api.RolledBack); err != nil {
			return false, err
		}
	}
	if err := manager.storageDriver.ReleaseGlobalSessionLock(gs); err != nil {
		return false, err
	}
	if err := manager.storageDriver.RemoveGlobalSession(gs); err != nil {
		return false, err
	}
	log.Infof("successfully RollbackLocal global, xid = %d", gs.XID)
	manager.globalTransactionCount.Dec()
	return true, nil
}

func (manager *DistributedTransactionManager) branchRollback(bs *api.BranchSession) (api.BranchSession_BranchStatus, error) {
	request := &api.BranchRollbackRequest{
		XID:             bs.XID,
		BranchID:        bs.BranchID,
		ResourceID:      bs.ResourceID,
		LockKey:         bs.LockKey,
		BranchType:      bs.Type,
		ApplicationData: bs.ApplicationData,
	}

	if bs.Type == api.AT && bs.Addressing == manager.addressing {
		status, lockKeys, err := manager._branchRollback(request)
		if len(lockKeys) > 0 {
			if err := manager.storageDriver.ReleaseLockAndRemoveBranchSession(bs.XID, bs.ResourceID, lockKeys); err != nil {
				log.Errorf("release lock and remove branch session failed, xid = %s, resource_id = %s, lockKeys = %s, error: %s",
					bs.XID, bs.ResourceID, lockKeys, err)
			}
		}
		return status, err
	}

	client, err := manager.getTransactionCoordinatorServiceClient(bs.Addressing)
	if err != nil {
		return bs.Status, err
	}

	response, err := client.BranchRollback(context.Background(), request)

	if response != nil && len(response.LockKeys) > 0 {
		if err := manager.storageDriver.ReleaseLockAndRemoveBranchSession(bs.XID, bs.ResourceID, response.LockKeys); err != nil {
			log.Errorf("release lock and remove branch session failed, xid = %s, resource_id = %s, lockKeys = %s",
				bs.XID, bs.ResourceID, response.LockKeys)
		}
	}

	if err != nil {
		return bs.Status, err
	}

	if response.ResultCode == api.ResultCodeSuccess {
		return response.BranchStatus, nil
	} else {
		return bs.Status, fmt.Errorf(response.Message)
	}
}

func (manager *DistributedTransactionManager) _branchRollback(in *api.BranchRollbackRequest) (api.BranchSession_BranchStatus, []string, error) {
	db := resource.GetDBManager().GetDB(in.ResourceID)
	if db == nil {
		return 0, nil, fmt.Errorf("DB resource is not exist, db name: %s", in.ResourceID)
	}

	lockKeys, err := GetUndoLogManager().Undo(db, in.XID)

	if err != nil {
		log.Errorf("[stacktrace]branchRollback failed. xid:[%s], branchID:[%d], resourceID:[%s], branchType:[%d], applicationData:[%v], error: %v",
			in.XID, in.BranchID, in.ResourceID, in.BranchType, in.ApplicationData, err)
		return api.PhaseTwoRollbackFailedRetryable, lockKeys, err
	}

	return api.PhaseTwoRolledBack, lockKeys, nil
}

func filterBranchSessions(sessions []*api.BranchSession) []*api.BranchSession {
	length := len(sessions)
	if length == 1 {
		return sessions
	}
	var result []*api.BranchSession
	var branchIdentifierMap = make(map[string]bool)
	for i := 0; i < length; i++ {
		if sessions[i].Status == api.PhaseOneFailed {
			continue
		}
		_, ok := branchIdentifierMap[sessions[i].ResourceID]
		if !ok {
			result = append(result, sessions[i])
			branchIdentifierMap[sessions[i].ResourceID] = true
		}
	}
	return result
}

func (manager *DistributedTransactionManager) getTransactionCoordinatorServiceClient(addressing string) (api.ResourceManagerServiceClient, error) {
	client1, ok1 := manager.tcServiceClients[addressing]
	if ok1 {
		return client1, nil
	}

	manager.Mutex.Lock()
	defer manager.Mutex.Unlock()
	client2, ok2 := manager.tcServiceClients[addressing]
	if ok2 {
		return client2, nil
	}

	conn, err := grpc.Dial(addressing, grpc.WithInsecure(), grpc.WithKeepaliveParams(manager.keepaliveClientParameters))
	if err != nil {
		return nil, err
	}
	client := api.NewResourceManagerServiceClient(conn)
	manager.tcServiceClients[addressing] = client
	return client, nil
}

func isTimeoutGlobalStatus(gs *api.GlobalSession) bool {
	return gs.Status == api.TimeoutRolledBack ||
		gs.Status == api.TimeoutRollbackFailed ||
		gs.Status == api.TimeoutRollingBack ||
		gs.Status == api.TimeoutRollbackRetrying
}

func (manager *DistributedTransactionManager) processTimeoutCheck() {
	for {
		timer := time.NewTimer(manager.timeoutRetryPeriod)

		<-timer.C
		manager.timeoutCheck()

		timer.Stop()
	}
}

func (manager *DistributedTransactionManager) processRetryRollingBack() {
	for {
		timer := time.NewTimer(manager.rollingBackRetryPeriod)

		<-timer.C
		manager.handleRetryRollingBack()

		timer.Stop()
	}
}

func (manager *DistributedTransactionManager) processRetryCommitting() {
	for {
		timer := time.NewTimer(manager.committingRetryPeriod)

		<-timer.C
		manager.handleRetryCommitting()

		timer.Stop()
	}
}

func (manager *DistributedTransactionManager) processAsyncCommitting() {
	for {
		timer := time.NewTimer(manager.asyncCommittingRetryPeriod)

		<-timer.C
		manager.handleAsyncCommitting()

		timer.Stop()
	}
}

func (manager *DistributedTransactionManager) timeoutCheck() {
	if manager.globalTransactionCount.Load() == 0 {
		return
	}
	sessions := manager.storageDriver.FindGlobalSessions(
		[]api.GlobalSession_GlobalStatus{api.Begin}, manager.addressing)
	for _, globalSession := range sessions {
		if isGlobalSessionTimeout(globalSession) {
			if globalSession.Active {
				// Active need persistence
				// Highlight: Firstly, close the session, then no more branch can be registered.
				if err := manager.storageDriver.InactiveGlobalSession(globalSession); err != nil {
					return
				}
			}

			if err := manager.storageDriver.UpdateGlobalSessionStatus(globalSession, api.TimeoutRollingBack); err != nil {
				return
			}
		}
	}
}

func (manager *DistributedTransactionManager) handleRetryRollingBack() {
	if manager.globalTransactionCount.Load() == 0 {
		return
	}
	sessions := manager.storageDriver.FindGlobalSessions([]api.GlobalSession_GlobalStatus{
		api.RollingBack, api.RollbackRetrying, api.TimeoutRollingBack, api.TimeoutRollbackRetrying,
	}, manager.addressing)
	now := timeUtils.CurrentTimeMillis()
	for _, session := range sessions {
		if session.Status == api.RollingBack && !manager.IsRollingBackDead(session) {
			continue
		}
		if isRetryTimeout(int64(now), manager.maxRollbackRetryTimeout, session.BeginTime) {
			if manager.rollbackRetryTimeoutUnlockEnable {
				if err := manager.storageDriver.ReleaseGlobalSessionLock(session); err != nil {
					log.Error(err)
				}
			}
			if err := manager.storageDriver.RemoveGlobalSession(session); err != nil {
				log.Error(err)
			}
			log.Errorf("GlobalSession RollbackLocal retry timeout and removed [%s]", session.XID)
			continue
		}
		_, err := manager.doGlobalRollback(session, true)
		if err != nil {
			log.Errorf("failed to retry RollbackLocal [%s]", session.XID)
		}
	}
}

func isRetryTimeout(now int64, timeout int64, beginTime int64) bool {
	if timeout >= AlwaysRetryBoundary && now-beginTime > timeout {
		return true
	}
	return false
}

func (manager *DistributedTransactionManager) handleRetryCommitting() {
	if manager.globalTransactionCount.Load() == 0 {
		return
	}
	sessions := manager.storageDriver.FindGlobalSessions(
		[]api.GlobalSession_GlobalStatus{api.CommitRetrying}, manager.addressing)
	if len(sessions) == 0 {
		return
	}
	now := timeUtils.CurrentTimeMillis()
	for _, session := range sessions {
		if isRetryTimeout(int64(now), manager.maxCommitRetryTimeout, session.BeginTime) {
			if err := manager.storageDriver.RemoveGlobalSession(session); err != nil {
				log.Error(err)
			}
			log.Errorf("GlobalSession CommitLocal retry timeout and removed [%s]", session.XID)
			continue
		}
		_, err := manager.doGlobalCommit(session, true)
		if err != nil {
			log.Errorf("failed to retry committing [%s]", session.XID)
		}
	}
}

func (manager *DistributedTransactionManager) handleAsyncCommitting() {
	if manager.globalTransactionCount.Load() == 0 {
		return
	}
	sessions := manager.storageDriver.FindGlobalSessions(
		[]api.GlobalSession_GlobalStatus{api.AsyncCommitting}, manager.addressing)
	for _, session := range sessions {
		_, err := manager.doGlobalCommit(session, true)
		if err != nil {
			log.Errorf("failed to async committing [%s]", session.XID)
		}
	}
}

func isGlobalSessionTimeout(gs *api.GlobalSession) bool {
	return (timeUtils.CurrentTimeMillis() - uint64(gs.BeginTime)) > uint64(gs.Timeout)
}

func (manager *DistributedTransactionManager) IsRollingBackDead(gs *api.GlobalSession) bool {
	return (timeUtils.CurrentTimeMillis() - uint64(gs.BeginTime)) > uint64(manager.retryDeadThreshold)
}
