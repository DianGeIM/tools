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

package version

import (
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestGet verifies that the Get function returns expected fields correctly set.
func TestGet(t *testing.T) {
	v := Get()

	assert.NotEmpty(t, v.GoVersion, "GoVersion should not be empty")
	assert.NotEmpty(t, v.Compiler, "Compiler should not be empty")
	assert.NotEmpty(t, v.Platform, "Platform should not be empty")
	assert.True(t, strings.Contains(v.Platform, runtime.GOOS), "Platform should contain runtime.GOOS")
	assert.True(t, strings.Contains(v.Platform, runtime.GOARCH), "Platform should contain runtime.GOARCH")
}

// TestGetSingleVersion verifies that the GetSingleVersion function returns the gitVersion.
func TestGetSingleVersion(t *testing.T) {
	version := GetSingleVersion()
	assert.Equal(t, gitVersion, version, "gitVersion should match the global gitVersion variable")
}
