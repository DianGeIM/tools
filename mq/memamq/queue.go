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
	"errors"
	"sync"
	"time"
)

// AsyncQueue is the interface responsible for asynchronous processing of functions.
type AsyncQueue interface {
	Initialize(processFunc func(), workerCount int, bufferSize int)
	Push(task func()) error
}

// MemoryQueue is an implementation of the AsyncQueue interface using a channel to process functions.
type MemoryQueue struct {
	taskChan  chan func()
	wg        sync.WaitGroup
	isStopped bool
	stopMutex sync.Mutex // Mutex to protect access to isStopped
}

func NewMemoryQueue(workerCount int, bufferSize int) *MemoryQueue {
	mq := &MemoryQueue{}                   // Create a new instance of MemoryQueue
	mq.Initialize(workerCount, bufferSize) // Initialize it with specified parameters
	return mq
}

// Initialize sets up the worker nodes and the buffer size of the channel,
// starting internal goroutines to handle tasks from the channel.
func (mq *MemoryQueue) Initialize(workerCount int, bufferSize int) {
	mq.taskChan = make(chan func(), bufferSize) // Initialize the channel with the provided buffer size.
	mq.isStopped = false

	// Start multiple goroutines based on the specified workerCount.
	for i := 0; i < workerCount; i++ {
		mq.wg.Add(1)
		go func(workerID int) {
			defer mq.wg.Done()
			for task := range mq.taskChan {
				task() // Execute the function
			}
		}(i)
	}
}

// Push submits a function to the queue.
// Returns an error if the queue is stopped or if the queue is full.
func (mq *MemoryQueue) Push(task func()) error {
	mq.stopMutex.Lock()
	if mq.isStopped {
		mq.stopMutex.Unlock()
		return errors.New("push failed: queue is stopped")
	}
	mq.stopMutex.Unlock()

	select {
	case mq.taskChan <- task:
		return nil
	case <-time.After(time.Millisecond * 100): // Timeout to prevent deadlock/blocking
		return errors.New("push failed: queue is full")
	}
}

// Stop is used to terminate the internal goroutines and close the channel.
func (mq *MemoryQueue) Stop() {
	mq.stopMutex.Lock()
	mq.isStopped = true
	close(mq.taskChan)
	mq.stopMutex.Unlock()
	mq.wg.Wait()
}
