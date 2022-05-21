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

package misc

import (
	"time"
)

const (
	TimeFormat         = "2006-01-02 15:04:05"
	DateFormat         = "2006-01-02"
	UnixTimeUnitOffset = uint64(time.Millisecond / time.Nanosecond)
)

// FormatTimeMillis converts Millisecond to time string
// tsMillis accurate to the millisecond，otherwise, an error will occur
func FormatTimeMillis(tsMillis uint64) string {
	return time.Unix(0, int64(tsMillis*UnixTimeUnitOffset)).UTC().Format(TimeFormat)
}

// FormatDate converts to date string
// tsMillis accurate to the millisecond，otherwise, an error will occur
func FormatDate(tsMillis uint64) string {
	return time.Unix(0, int64(tsMillis*UnixTimeUnitOffset)).UTC().Format(DateFormat)
}

// CurrentTimeMillis Returns the current Unix timestamp in milliseconds.
func CurrentTimeMillis() uint64 {
	return uint64(time.Now().UTC().UnixNano()) / UnixTimeUnitOffset
}

// CurrentTimeNano Returns the current Unix timestamp in nanoseconds.
func CurrentTimeNano() uint64 {
	return uint64(time.Now().UTC().UnixNano())
}
