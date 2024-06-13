// Copyright © 2024 OpenIM open source community. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package idutil

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestGetMsgIDByMD5 checks if GetMsgIDByMD5 returns a valid MD5 hash string.
func TestGetMsgIDByMD5(t *testing.T) {
	sendID := "12345"
	msgID := GetMsgIDByMD5(sendID)

	// MD5 hash is a 32 character hexadecimal number.
	assert.Regexp(t, regexp.MustCompile("^[a-fA-F0-9]{32}$"), msgID, "The returned msgID should be a valid MD5 hash")
}

// TestOperationIDGenerator checks if OperationIDGenerator returns a string that looks like a valid operation ID.
func TestOperationIDGenerator(t *testing.T) {
	opID := OperationIDGenerator()

	// The operation ID should be a long number (timestamp + random number).
	// Just check if it is numeric and has a reasonable length.
	assert.Regexp(t, regexp.MustCompile("^[0-9]{13,}$"), opID, "The returned operation ID should be numeric and long")
}
