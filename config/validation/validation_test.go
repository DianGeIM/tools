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

package validation

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestSimpleValidator_ValidateSuccess
func TestSimpleValidator_ValidateSuccess(t *testing.T) {
	validator := NewSimpleValidator()
	type Config struct {
		Name string
		Age  int
	}
	config := Config{Name: "Test", Age: 1}

	err := validator.Validate(config)
	assert.Nil(t, err)
}

// TestSimpleValidator_ValidateFailure
func TestSimpleValidator_ValidateFailure(t *testing.T) {
	validator := NewSimpleValidator()
	type Config struct {
		Name string
		Age  int
	}
	config := Config{Name: "", Age: 0}

	err := validator.Validate(config)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "validation failed")
}

// TestSimpleValidator_ValidateNonStruct
func TestSimpleValidator_ValidateNonStruct(t *testing.T) {
	validator := NewSimpleValidator()
	config := "I am not a struct"

	err := validator.Validate(config)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "validation failed: config must be a struct or a pointer to struct")
}
