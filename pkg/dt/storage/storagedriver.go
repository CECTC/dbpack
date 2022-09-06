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

package storage

import (
	"context"

	"github.com/cectc/dbpack/pkg/dt/api"
)

type Driver interface {
	LeaderElection(applicationID string) bool
	AddGlobalSession(ctx context.Context, globalSession *api.GlobalSession) error
	AddBranchSession(ctx context.Context, branchSession *api.BranchSession) error
	GlobalCommit(ctx context.Context, xid string) (api.GlobalSession_GlobalStatus, error)
	GlobalRollback(ctx context.Context, xid string) (api.GlobalSession_GlobalStatus, error)
	GetGlobalSession(ctx context.Context, xid string) (*api.GlobalSession, error)
	ListGlobalSession(ctx context.Context, applicationID string) ([]*api.GlobalSession, error)
	DeleteGlobalSession(ctx context.Context, xid string) error
	GetBranchSession(ctx context.Context, branchID string) (*api.BranchSession, error)
	ListBranchSession(ctx context.Context, applicationID string) ([]*api.BranchSession, error)
	DeleteBranchSession(ctx context.Context, branchID string) error
	GetBranchSessionKeys(ctx context.Context, xid string) ([]string, error)
	BranchReport(ctx context.Context, branchID string, status api.BranchSession_BranchStatus) error
	IsLockable(ctx context.Context, resourceID string, lockKey string) (bool, error)
	IsLockableWithXID(ctx context.Context, resourceID string, lockKey string, xid string) (bool, error)
	ReleaseLockKeys(ctx context.Context, resourceID string, lockKeys []string) (bool, error)
	SetBranchSessionDead(ctx context.Context, branchSession *api.BranchSession) error
	ListDeadBranchSession(ctx context.Context, applicationID string) ([]*api.BranchSession, error)
	WatchGlobalSessions(ctx context.Context, applicationID string) Watcher
	WatchBranchSessions(ctx context.Context, applicationID string) Watcher
}

// Watcher can be implemented by anything that knows how to watch and report changes.
type Watcher interface {
	// Stop watching. Will close the channel returned by ResultChan(). Releases
	// any resources used by the watch.
	Stop()

	// ResultChan return a chan which will receive all the TransactionSessions. If an error occurs
	// or Stop() is called, the implementation will close this channel and
	// release any resources used by the watch.
	ResultChan() <-chan TransactionSession
}

type TransactionSession interface {
	Marshal() (data []byte, err error)
	Unmarshal(data []byte) error
}
