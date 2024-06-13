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

package discovery

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"
)

// MockSvcDiscoveryRegistry is a mock of SvcDiscoveryRegistry interface
type MockSvcDiscoveryRegistry struct {
	mock.Mock
}

// Here we define all the methods we want to mock for SvcDiscoveryRegistry interface
func (m *MockSvcDiscoveryRegistry) GetConns(ctx context.Context, serviceName string, opts ...grpc.DialOption) ([]*grpc.ClientConn, error) {
	args := m.Called(ctx, serviceName, opts)
	return args.Get(0).([]*grpc.ClientConn), args.Error(1)
}

// Implement other methods of SvcDiscoveryRegistry similarly...

func TestGetConns(t *testing.T) {
	mockSvcDiscovery := new(MockSvcDiscoveryRegistry)
	ctx := context.Background()
	serviceName := "exampleService"
	dummyConns := []*grpc.ClientConn{nil} // Simplified for demonstration; in real test, you'd use actual or mocked connections

	// Setup expectations
	mockSvcDiscovery.On("GetConns", ctx, serviceName, mock.Anything).Return(dummyConns, nil)

	// Test the function
	conns, err := mockSvcDiscovery.GetConns(ctx, serviceName)

	// Assertions
	assert.NoError(t, err)
	assert.Equal(t, dummyConns, conns)

	// Assert that the expectations were met
	mockSvcDiscovery.AssertExpectations(t)
}
