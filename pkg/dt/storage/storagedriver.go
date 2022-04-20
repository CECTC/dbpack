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
	"github.com/cectc/dbpack/pkg/dt/api"
)

type Driver interface {
	AddGlobalSession(session *api.GlobalSession) error

	FindGlobalSession(xid string) *api.GlobalSession

	FindGlobalSessions(statuses []api.GlobalSession_GlobalStatus, addressing string) []*api.GlobalSession

	CountGlobalSessions(addressing string) (int, error)

	UpdateGlobalSessionStatus(session *api.GlobalSession, status api.GlobalSession_GlobalStatus) error

	InactiveGlobalSession(session *api.GlobalSession) error

	CanBeAsyncCommitOrRollback(session *api.GlobalSession) bool

	ReleaseGlobalSessionLock(session *api.GlobalSession) error

	RemoveGlobalSession(session *api.GlobalSession) error

	AddBranchSession(globalSession *api.GlobalSession, session *api.BranchSession) error

	FindBranchSessions(xid string) []*api.BranchSession

	FindBranchSessionByBranchID(branchID int64) *api.BranchSession

	ReleaseBranchSessionLock(session *api.BranchSession) error

	ReleaseLockAndRemoveBranchSession(xid string, resourceID string, lockKeys []string) error

	UpdateBranchSessionStatus(session *api.BranchSession, status api.BranchSession_BranchStatus) error

	RemoveBranchSession(globalSession *api.GlobalSession, session *api.BranchSession) error

	IsLockable(xid string, resourceID string, lockKey string) bool
}
