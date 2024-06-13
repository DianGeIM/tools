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

package memamq

import (
	"sync"
	"testing"
	"time"
)

func TestNewMemoryQueue(t *testing.T) {
	workerCount := 3
	bufferSize := 10
	queue := NewMemoryQueue(workerCount, bufferSize)

	if cap(queue.taskChan) != bufferSize {
		t.Errorf("Expected buffer size %d, got %d", bufferSize, cap(queue.taskChan))
	}

	if queue.isStopped {
		t.Errorf("New queue is prematurely stopped")
	}

	if len(queue.taskChan) != 0 {
		t.Errorf("New queue should be empty, found %d items", len(queue.taskChan))
	}
}

func TestPushAndStop(t *testing.T) {
	queue := NewMemoryQueue(1, 5)

	var wg sync.WaitGroup
	wg.Add(1)
	queue.Push(func() {
		time.Sleep(50 * time.Millisecond) // Simulate task delay
		wg.Done()
	})

	queue.Stop()
	wg.Wait()

	if err := queue.Push(func() {}); err == nil {
		t.Error("Expected error when pushing to stopped queue, got none")
	}
}

func TestPushTimeout(t *testing.T) {
	queue := NewMemoryQueue(1, 1) // Small buffer and worker to force full queue

	done := make(chan bool)
	go func() {
		queue.Push(func() {
			time.Sleep(200 * time.Millisecond) // Long enough to cause the second push to timeout
		})
		done <- true
	}()

	<-done // Ensure first task is pushed

	if err := queue.Push(func() {}); err != nil {
		t.Error("Expected timeout error, got nil")
	}
}
