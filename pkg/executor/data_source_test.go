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

package executor

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"github.com/cectc/dbpack/pkg/resource"
	"github.com/cectc/dbpack/testdata"
)

func TestCastToDataSourceBrief(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	manager := testdata.NewMockDBManager(ctrl)
	manager.EXPECT().GetDB(gomock.Any()).AnyTimes().Return(nil)
	resource.SetDBManager(manager)

	testCases := []struct {
		in     *DataSourceRef
		expect *DataSourceBrief
	}{
		{
			in:     &DataSourceRef{Name: "employee", Weight: "r0w10"},
			expect: &DataSourceBrief{Name: "employee", WriteWeight: 10, ReadWeight: 0, IsMaster: true},
		},
		{
			in:     &DataSourceRef{Name: "employee", Weight: "r5w5"},
			expect: &DataSourceBrief{Name: "employee", WriteWeight: 5, ReadWeight: 5, IsMaster: true},
		},
		{
			in:     &DataSourceRef{Name: "employee", Weight: "r10w0"},
			expect: &DataSourceBrief{Name: "employee", WriteWeight: 0, ReadWeight: 10, IsMaster: false},
		},
	}

	for _, testCase := range testCases {
		brief, err := testCase.in.castToDataSourceBrief()
		assert.Nil(t, err)
		assert.Equal(t, testCase.expect.Name, brief.Name)
		assert.Equal(t, testCase.expect.WriteWeight, brief.WriteWeight)
		assert.Equal(t, testCase.expect.ReadWeight, brief.ReadWeight)
		assert.Equal(t, testCase.expect.IsMaster, brief.IsMaster)
	}
}

func TestGroupDataSourceRefs(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	manager := testdata.NewMockDBManager(ctrl)
	manager.EXPECT().GetDB(gomock.Any()).AnyTimes().Return(nil)
	resource.SetDBManager(manager)

	testCases := []struct {
		in            []*DataSourceRef
		expectMasters []*DataSourceBrief
		expectReads   []*DataSourceBrief
	}{
		{
			in: []*DataSourceRef{
				{Name: "employee0", Weight: "r0w5"},
				{Name: "employee1", Weight: "r5w5"},
				{Name: "employee2", Weight: "r5w0"},
			},
			expectMasters: []*DataSourceBrief{
				{Name: "employee0", WriteWeight: 5, ReadWeight: 0, IsMaster: true},
				{Name: "employee1", WriteWeight: 5, ReadWeight: 5, IsMaster: true},
			},
			expectReads: []*DataSourceBrief{
				{Name: "employee1", WriteWeight: 5, ReadWeight: 5, IsMaster: true},
				{Name: "employee2", WriteWeight: 0, ReadWeight: 5, IsMaster: false},
			},
		},
		{
			in: []*DataSourceRef{
				{Name: "employee0", Weight: "r0w5"},
			},
			expectMasters: []*DataSourceBrief{
				{Name: "employee0", WriteWeight: 5, ReadWeight: 0, IsMaster: true},
			},
			expectReads: nil,
		},
		{
			in: []*DataSourceRef{
				{Name: "employee2", Weight: "r5w0"},
			},
			expectMasters: nil,
			expectReads: []*DataSourceBrief{
				{Name: "employee2", WriteWeight: 0, ReadWeight: 5, IsMaster: false},
			},
		},
	}

	for _, testCase := range testCases {
		masters, reads, err := groupDataSourceRefs(testCase.in)
		assert.Nil(t, err)
		if testCase.expectMasters == nil {
			assert.Equal(t, testCase.expectMasters, masters)
		} else {
			for i, master := range testCase.expectMasters {
				assert.Equal(t, master.Name, masters[i].Name)
				assert.Equal(t, master.WriteWeight, masters[i].WriteWeight)
				assert.Equal(t, master.ReadWeight, masters[i].ReadWeight)
				assert.Equal(t, master.IsMaster, masters[i].IsMaster)
			}
		}
		if testCase.expectReads == nil {
			assert.Equal(t, testCase.expectReads, reads)
		} else {
			for i, read := range testCase.expectReads {
				assert.Equal(t, read.Name, reads[i].Name)
				assert.Equal(t, read.WriteWeight, reads[i].WriteWeight)
				assert.Equal(t, read.ReadWeight, reads[i].ReadWeight)
				assert.Equal(t, read.IsMaster, reads[i].IsMaster)
			}
		}
	}
}
