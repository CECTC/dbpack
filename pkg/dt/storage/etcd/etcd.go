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

package etcd

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"google.golang.org/grpc"

	"github.com/cectc/dbpack/pkg/dt/api"
	"github.com/cectc/dbpack/pkg/dt/storage"
	err2 "github.com/cectc/dbpack/pkg/errors"
	"github.com/cectc/dbpack/pkg/log"
	"github.com/cectc/dbpack/pkg/misc"
)

const (
	// We have set a buffer in order to reduce times of context switches.
	incomingBufSize = 100
	outgoingBufSize = 100
)

type store struct {
	client                    *clientv3.Client
	session                   *concurrency.Session
	initGlobalSessionRevision int64
	initBranchSessionRevision int64
}

func NewEtcdStore(config clientv3.Config) *store {
	if config.DialTimeout == 0 {
		config.DialTimeout = 5 * time.Second
	}
	config.DialOptions = append(config.DialOptions, []grpc.DialOption{grpc.WithBlock()}...)
	client, err := clientv3.New(config)
	if err != nil {
		log.Fatal(err)
	}
	session, err := concurrency.NewSession(client)
	if err != nil {
		log.Fatal(err)
	}
	return &store{
		client:                    client,
		session:                   session,
		initGlobalSessionRevision: 0,
		initBranchSessionRevision: 0,
	}
}

// watchChan implements watch.Interface.
type watchChan struct {
	client            *clientv3.Client
	key               string
	initialRev        int64
	recursive         bool
	ctx               context.Context
	cancel            context.CancelFunc
	incomingEventChan chan *event
	resultChan        chan storage.TransactionSession
	isGlobalSession   bool
}

func (s *store) LeaderElection(applicationID string) bool {
	e := concurrency.NewElection(s.session, fmt.Sprintf("%s/leader-election/", applicationID))
	ctx := context.Background()
	// Elect a leader (or wait that the leader resign)
	if err := e.Campaign(ctx, "e"); err != nil {
		log.Fatal(err)
	}
	return true
}

func (s *store) AddGlobalSession(ctx context.Context, globalSession *api.GlobalSession) error {
	data, err := globalSession.Marshal()
	if err != nil {
		return err
	}
	_, err = s.client.Put(ctx, globalSession.XID, string(data))
	return err
}

func (s *store) AddBranchSession(ctx context.Context, branchSession *api.BranchSession) error {
	if branchSession.Type == api.AT && branchSession.LockKey != "" {
		rowKeys := misc.CollectRowKeys(branchSession.LockKey, branchSession.ResourceID)

		txn := s.client.Txn(ctx)
		var cmpSlice []clientv3.Cmp
		for _, rowKey := range rowKeys {
			cmpSlice = append(cmpSlice, notFound(rowKey))
		}
		txn = txn.If(cmpSlice...)

		var ops []clientv3.Op
		for _, rowKey := range rowKeys {
			lockKey := fmt.Sprintf("lk/%s/%s", branchSession.XID, rowKey)
			ops = append(ops, clientv3.OpPut(lockKey, rowKey))
			ops = append(ops, clientv3.OpPut(rowKey, lockKey))
		}
		txn.Then(ops...)

		txnResp, err := txn.Commit()
		if err != nil {
			return err
		}
		if !txnResp.Succeeded {
			return err2.BranchLockAcquireFailed
		}
	}

	data, err := branchSession.Marshal()
	if err != nil {
		return err
	}
	_, err = s.client.Put(ctx, branchSession.BranchID, string(data))
	if err != nil {
		return err
	}

	// 全局事务关联的事务分支
	globalBranchKey := fmt.Sprintf("bs/%s/%d", branchSession.XID, branchSession.BranchSessionID)
	_, err = s.client.Put(ctx, globalBranchKey, branchSession.BranchID)
	return err
}

func (s *store) GlobalCommit(ctx context.Context, xid string) (api.GlobalSession_GlobalStatus, error) {
	released, err := s.releaseGlobalLocks(ctx, xid)
	if err != nil {
		return api.Begin, err
	}
	if !released {
		return api.Begin, errors.New("release global lock failed")
	}

	gs, err := s.GetGlobalSession(ctx, xid)
	if err != nil {
		if errors.Is(err, err2.CouldNotFoundGlobalTransaction) {
			return api.Finished, nil
		}
		return api.Begin, err
	}
	if gs.Status > api.Begin {
		return gs.Status, nil
	}
	gs.Status = api.Committing
	data, err := gs.Marshal()
	if err != nil {
		return gs.Status, err
	}
	_, err = s.client.Put(ctx, xid, string(data))
	if err != nil {
		return api.Begin, err
	}

	go func() {
		branchKeys, err := s.GetBranchSessionKeys(ctx, xid)
		if err != nil {
			log.Error(err)
		}

		err = s.commitBranchSessions(ctx, branchKeys)
		if err != nil {
			log.Error(err)
		}
	}()
	return api.Committing, nil
}

func (s *store) GlobalRollback(ctx context.Context, xid string) (api.GlobalSession_GlobalStatus, error) {
	gs, err := s.GetGlobalSession(ctx, xid)
	if err != nil {
		if errors.Is(err, err2.CouldNotFoundGlobalTransaction) {
			return api.Finished, nil
		}
		return api.Begin, err
	}
	if gs.Status > api.Begin {
		return gs.Status, nil
	}
	gs.Status = api.Rollbacking
	data, err := gs.Marshal()
	if err != nil {
		return gs.Status, err
	}
	_, err = s.client.Put(ctx, xid, string(data))
	if err != nil {
		return api.Begin, err
	}

	branchKeys, err := s.GetBranchSessionKeys(ctx, xid)
	if err != nil {
		log.Error(err)
	}

	err = s.rollbackBranchSessions(ctx, branchKeys)
	if err != nil {
		log.Error(err)
	}

	return api.Rollbacking, nil
}

func (s *store) GetGlobalSession(ctx context.Context, xid string) (*api.GlobalSession, error) {
	resp, err := s.client.Get(ctx, xid, clientv3.WithSerializable())
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) == 0 {
		return nil, err2.CouldNotFoundGlobalTransaction
	}
	globalSession := &api.GlobalSession{}
	err = globalSession.Unmarshal(resp.Kvs[0].Value)
	if err != nil {
		return nil, err
	}
	return globalSession, nil
}

func (s *store) ListGlobalSession(ctx context.Context, applicationID string) ([]*api.GlobalSession, error) {
	prefix := fmt.Sprintf("gs/%s", applicationID)
	resp, err := s.client.Get(ctx, prefix, clientv3.WithSerializable(), clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	s.initGlobalSessionRevision = resp.Header.Revision
	if len(resp.Kvs) == 0 {
		return nil, nil
	}
	var result []*api.GlobalSession
	for _, kv := range resp.Kvs {
		globalSession := &api.GlobalSession{}
		err = globalSession.Unmarshal(kv.Value)
		if err != nil {
			return nil, err
		}
		result = append(result, globalSession)
	}
	return result, nil
}

func (s *store) DeleteGlobalSession(ctx context.Context, xid string) error {
	_, err := s.client.Delete(ctx, xid)
	return err
}

func (s *store) GetBranchSession(ctx context.Context, branchID string) (*api.BranchSession, error) {
	resp, err := s.client.Get(ctx, branchID, clientv3.WithSerializable())
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) == 0 {
		return nil, err2.CouldNotFoundBranchTransaction
	}
	branchSession := &api.BranchSession{}
	err = branchSession.Unmarshal(resp.Kvs[0].Value)
	if err != nil {
		return nil, err
	}
	return branchSession, nil
}

func (s *store) ListBranchSession(ctx context.Context, applicationID string) ([]*api.BranchSession, error) {
	prefix := fmt.Sprintf("bs/%s", applicationID)
	resp, err := s.client.Get(ctx, prefix, clientv3.WithSerializable(), clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	s.initBranchSessionRevision = resp.Header.Revision
	if len(resp.Kvs) == 0 {
		return nil, nil
	}
	var result []*api.BranchSession
	for _, kv := range resp.Kvs {
		branchSession := &api.BranchSession{}
		err = branchSession.Unmarshal(kv.Value)
		if err != nil {
			return nil, err
		}
		result = append(result, branchSession)
	}
	return result, nil
}

func (s *store) DeleteBranchSession(ctx context.Context, branchID string) error {
	resp, err := s.client.Delete(ctx, branchID, clientv3.WithPrevKV())
	for _, kv := range resp.PrevKvs {
		branchSession := &api.BranchSession{}
		err = branchSession.Unmarshal(kv.Value)
		if err != nil {
			return err
		}
		globalBranchKey := fmt.Sprintf("bs/%s/%d", branchSession.XID, branchSession.BranchSessionID)
		_, err := s.client.Delete(ctx, globalBranchKey)
		if err != nil {
			return err
		}
	}
	return err
}

func (s *store) GetBranchSessionKeys(ctx context.Context, xid string) ([]string, error) {
	prefix := fmt.Sprintf("bs/%s", xid)
	branchKeyResp, err := s.client.Get(ctx, prefix, clientv3.WithSerializable(), clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	var result []string
	for _, kv := range branchKeyResp.Kvs {
		result = append(result, string(kv.Value))
	}
	return result, nil
}

func (s *store) BranchReport(ctx context.Context, branchID string, status api.BranchSession_BranchStatus) error {
	bs, err := s.GetBranchSession(ctx, branchID)
	if err != nil {
		return err
	}
	if bs.Status == api.Registered {
		bs.Status = status
		data, err := bs.Marshal()
		if err != nil {
			return err
		}
		_, err = s.client.Put(ctx, branchID, string(data))
		if err != nil {
			return err
		}
	}
	if bs.Status == api.PhaseOneFailed {
		if err := s.DeleteBranchSession(ctx, branchID); err != nil {
			return err
		}
	}
	return nil
}

func (s *store) releaseGlobalLocks(ctx context.Context, xid string) (bool, error) {
	prefix := fmt.Sprintf("lk/%s", xid)
	resp, err := s.client.Delete(ctx, prefix, clientv3.WithPrefix(), clientv3.WithPrevKV())
	if err != nil {
		return false, err
	}
	var ops []clientv3.Op
	for _, kv := range resp.PrevKvs {
		ops = append(ops, clientv3.OpDelete(string(kv.Value)))
	}
	if len(ops) != 0 {
		deleteResp, err := s.client.Txn(ctx).Then(ops...).Commit()
		if err != nil {
			return false, err
		}
		if !deleteResp.Succeeded {
			return false, nil
		}
	}
	return true, nil
}

func (s *store) commitBranchSessions(ctx context.Context, branchSessionKeys []string) error {
	for _, key := range branchSessionKeys {
		bs, err := s.GetBranchSession(ctx, key)
		if err != nil {
			return err
		}
		if bs.Status == api.Registered {
			bs.Status = api.PhaseTwoCommitting
			data, err := bs.Marshal()
			if err != nil {
				return err
			}
			_, err = s.client.Put(ctx, key, string(data))
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *store) rollbackBranchSessions(ctx context.Context, branchSessionKeys []string) error {
	for _, key := range branchSessionKeys {
		bs, err := s.GetBranchSession(ctx, key)
		if err != nil {
			return err
		}
		if bs.Status == api.Registered {
			bs.Status = api.PhaseTwoRollbacking
			data, err := bs.Marshal()
			if err != nil {
				return err
			}
			_, err = s.client.Put(ctx, key, string(data))
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *store) IsLockable(ctx context.Context, resourceID string, lockKey string) (bool, error) {
	rowKeys := misc.CollectRowKeys(lockKey, resourceID)

	for _, rowKey := range rowKeys {
		resp, err := s.client.Get(ctx, rowKey, clientv3.WithSerializable())
		if err != nil {
			return false, err
		}
		if len(resp.Kvs) > 0 {
			return false, nil
		}
	}
	return true, nil
}

func (s *store) ReleaseLockKeys(ctx context.Context, resourceID string, lockKeys []string) (bool, error) {
	var ops []clientv3.Op
	for _, lockKey := range lockKeys {
		rowKeys := misc.CollectRowKeys(lockKey, resourceID)
		for _, rowKey := range rowKeys {
			resp, err := s.client.Delete(ctx, rowKey, clientv3.WithPrevKV())
			if err != nil {
				return false, err
			}
			for _, kv := range resp.PrevKvs {
				ops = append(ops, clientv3.OpDelete(string(kv.Value)))
			}
		}
	}

	if len(ops) != 0 {
		_, err := s.client.Txn(ctx).Then(ops...).Commit()
		if err != nil {
			return false, err
		}
	}
	return true, nil
}

func notFound(key string) clientv3.Cmp {
	return clientv3.Compare(clientv3.ModRevision(key), "=", 0)
}

func (s *store) WatchGlobalSessions(ctx context.Context, applicationID string) storage.Watcher {
	prefix := fmt.Sprintf("gs/%s", applicationID)
	wc := s.createWatchChan(ctx, prefix, s.initGlobalSessionRevision, true, true)
	go wc.run()
	return wc
}

func (s *store) WatchBranchSessions(ctx context.Context, applicationID string) storage.Watcher {
	prefix := fmt.Sprintf("bs/%s", applicationID)
	wc := s.createWatchChan(ctx, prefix, s.initBranchSessionRevision, true, false)
	go wc.run()
	return wc
}

func (s *store) createWatchChan(ctx context.Context, key string, rev int64, recursive, isGlobalSession bool) *watchChan {
	wc := &watchChan{
		client:            s.client,
		key:               key,
		initialRev:        rev,
		recursive:         recursive,
		incomingEventChan: make(chan *event, incomingBufSize),
		resultChan:        make(chan storage.TransactionSession, outgoingBufSize),
		isGlobalSession:   isGlobalSession,
	}

	// The etcd server waits until it cannot find a leader for 3 election
	// timeouts to cancel existing streams. 3 is currently a hard coded
	// constant. The election timeout defaults to 1000ms. If the cluster is
	// healthy, when the leader is stopped, the leadership transfer should be
	// smooth. (leader transfers its leadership before stopping). If leader is
	// hard killed, other servers will take an election timeout to realize
	// leader lost and start campaign.
	wc.ctx, wc.cancel = context.WithCancel(clientv3.WithRequireLeader(ctx))
	return wc
}

func (wc *watchChan) run() {
	watchClosedCh := make(chan struct{})
	go wc.startWatching(watchClosedCh)

	var resultChanWG sync.WaitGroup
	resultChanWG.Add(1)
	go wc.processEvent(&resultChanWG)

	select {
	case <-watchClosedCh:
	case <-wc.ctx.Done(): // user cancel
	}

	// We use wc.ctx to reap all goroutines. Under whatever condition, we should stop them all.
	// It's fine to double cancel.
	wc.cancel()

	// we need to wait until resultChan wouldn't be used anymore
	resultChanWG.Wait()
	close(wc.resultChan)
}

func (wc *watchChan) Stop() {
	wc.cancel()
}

func (wc *watchChan) ResultChan() <-chan storage.TransactionSession {
	return wc.resultChan
}

// - watch on given key and send events to process.
func (wc *watchChan) startWatching(watchClosedCh chan struct{}) {
	opts := []clientv3.OpOption{clientv3.WithRev(wc.initialRev + 1), clientv3.WithPrevKV()}
	if wc.recursive {
		opts = append(opts, clientv3.WithPrefix())
	}
	wch := wc.client.Watch(wc.ctx, wc.key, opts...)
	for wres := range wch {
		if wres.Err() != nil {
			err := wres.Err()
			// If there is an error on server (e.g. compaction), the channel will return it before closed.
			log.Errorf("watch chan error: %v", err)
			return
		}

		for _, e := range wres.Events {
			parsedEvent, err := parseEvent(e)
			if err != nil {
				log.Errorf("watch chan error: %v", err)
				return
			}
			wc.sendEvent(parsedEvent)
		}
	}
	// When we come to this point, it's only possible that client side ends the watch.
	// e.g. cancel the context, close the client.
	// If this watch chan is broken and context isn't cancelled, other goroutines will still hang.
	// We should notify the main thread that this goroutine has exited.
	close(watchClosedCh)
}

// processEvent processes events from etcd watcher and sends results to resultChan.
func (wc *watchChan) processEvent(wg *sync.WaitGroup) {
	var objectType = "api.GlobalSession"
	if !wc.isGlobalSession {
		objectType = "api.BranchSession"
	}
	defer wg.Done()

	for {
		select {
		case e := <-wc.incomingEventChan:
			res := wc.transform(e)
			if res == nil {
				continue
			}
			if len(wc.resultChan) == outgoingBufSize {
				log.Infof("Fast watcher, slow processing. Probably caused by slow dispatching events to watchers", "outgoingEvents", outgoingBufSize, "objectType", objectType)
			}
			// If user couldn't receive results fast enough, we also block incoming events from watcher.
			// Because storing events in local will cause more memory usage.
			// The worst case would be closing the fast watcher.
			select {
			case wc.resultChan <- res:
			case <-wc.ctx.Done():
				return
			}
		case <-wc.ctx.Done():
			return
		}
	}
}

// transform an event into a result for user.
func (wc *watchChan) transform(e *event) (res storage.TransactionSession) {
	switch {
	case e.isDeleted:
	case e.isCreated:
	default:
		if wc.isGlobalSession {
			res = &api.GlobalSession{}
			err := res.Unmarshal(e.value)
			if err != nil {
				log.Error(err)
			}
		} else {
			res = &api.BranchSession{}
			err := res.Unmarshal(e.value)
			if err != nil {
				log.Error(err)
			}
		}
	}
	return res
}

func (wc *watchChan) sendEvent(e *event) {
	var objectType = "api.GlobalSession"
	if !wc.isGlobalSession {
		objectType = "api.BranchSession"
	}
	if len(wc.incomingEventChan) == incomingBufSize {
		log.Info("Fast watcher, slow processing. Probably caused by slow decoding, user not receiving fast, or other processing logic", "incomingEvents", incomingBufSize, "objectType", objectType)
	}
	select {
	case wc.incomingEventChan <- e:
	case <-wc.ctx.Done():
	}
}
