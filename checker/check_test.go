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

package checker_test

import (
	"testing"

	"github.com/DianGeIM/tools/checker"
	"github.com/DianGeIM/tools/errs"
	"github.com/stretchr/testify/assert"
)

type mockChecker struct {
	err error
}

func (m mockChecker) Check() error {
	return m.err
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name      string
		arg       any
		wantError error
	}{
		{
			name:      "non-checker argument",
			arg:       "non-checker",
			wantError: nil,
		},
		{
			name:      "checker with no error",
			arg:       mockChecker{nil},
			wantError: nil,
		},
		{
			name:      "checker with generic error",
			arg:       mockChecker{errs.New("generic error")},
			wantError: errs.ErrArgs,
		},
		{
			name:      "checker with CodeError",
			arg:       mockChecker{errs.NewCodeError(400, "bad request")},
			wantError: errs.NewCodeError(400, "bad request"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := checker.Validate(tt.arg)
			if tt.wantError != nil {
				assert.ErrorIs(t, err, tt.wantError)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
