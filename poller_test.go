package poller

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPollerOnStartShouldProcessTasksImmediately(t *testing.T) {
	done := false
	poller := New(3*time.Second, 2, 2, 2, testTaskIterator(func(_ context.Context) {
		done = true
	}, 5))

	go poller.Start()
	defer poller.Stop()
	time.Sleep(time.Second)
	assert.True(t, done)
}

func TestPollerOnStartShouldProcessTasksPeriodically(t *testing.T) {
	var counter int64
	poller := New(2*time.Second, 2, 2, 2, testTaskIterator(func(_ context.Context) {
		atomic.AddInt64(&counter, 1)
	}, 2, 2, 1))

	go poller.Start()
	defer poller.Stop()
	time.Sleep(time.Second)
	assert.Equal(t, int64(2), atomic.LoadInt64(&counter))
	time.Sleep(2 * time.Second)
	assert.Equal(t, int64(4), atomic.LoadInt64(&counter))
	time.Sleep(2 * time.Second)
	assert.Equal(t, int64(5), atomic.LoadInt64(&counter))
}

func TestPollerOnStartShouldProcessTasksWithSpecifiedConcurrency(t *testing.T) {
	var counter int64
	poller := New(2*time.Second, 2, 2, 2, testTaskIterator(func(_ context.Context) {
		time.Sleep(2 * time.Second)
		atomic.AddInt64(&counter, 1)
	}, 5))

	go poller.Start()
	defer poller.Stop()
	time.Sleep(time.Second)
	assert.Equal(t, int64(0), atomic.LoadInt64(&counter))
	time.Sleep(2 * time.Second)
	assert.Equal(t, int64(2), atomic.LoadInt64(&counter))
	time.Sleep(2 * time.Second)
	assert.Equal(t, int64(4), atomic.LoadInt64(&counter))
	time.Sleep(2 * time.Second)
	assert.Equal(t, int64(5), atomic.LoadInt64(&counter))
}

func TestPollerOnStartShouldProcessTasksWithSpecifiedPeriod(t *testing.T) {
	var counter int64
	poller := New(2*time.Second, 2, 2, 2, testTaskIterator(func(_ context.Context) {
		if atomic.AddInt64(&counter, 1) <= 2 {
			time.Sleep(3 * time.Second)
		}
	}, 3, 3, 3))

	go poller.Start()
	defer poller.Stop()
	time.Sleep(time.Second)
	assert.Equal(t, int64(2), atomic.LoadInt64(&counter))
	time.Sleep(3 * time.Second)
	assert.Equal(t, int64(3), atomic.LoadInt64(&counter))
	time.Sleep(2 * time.Second)
	assert.Equal(t, int64(6), atomic.LoadInt64(&counter))
	time.Sleep(2 * time.Second)
	assert.Equal(t, int64(9), atomic.LoadInt64(&counter))
}

func TestPollerOnStopShouldShutdownGracefully(t *testing.T) {
	var counter int64
	poller := New(3*time.Second, 2, 2, 2, testTaskIterator(func(ctx context.Context) {
		<-ctx.Done()
		atomic.AddInt64(&counter, 1)
	}, 5))

	go poller.Start()
	time.Sleep(time.Second)
	poller.Stop()
	assert.Equal(t, int64(2), atomic.LoadInt64(&counter))
}

func testTaskIterator(f func(ctx context.Context), chunkLength ...int) (iter func(ctx context.Context, limit int) (tasks []Task, hasMore bool)) {
	chunkIndex := 0
	chunkOffset := 0
	return func(ctx context.Context, limit int) (tasks []Task, hasMore bool) {
		if chunkIndex >= len(chunkLength) {
			return nil, false
		}
		currentChunkLength := chunkLength[chunkIndex]
		tasksCount := currentChunkLength - chunkOffset
		if tasksCount > limit {
			tasksCount = limit
			hasMore = true
			chunkOffset = chunkOffset + tasksCount
		} else {
			hasMore = false
			chunkIndex += 1
			chunkOffset = 0
		}
		tasks = make([]Task, tasksCount)
		for i := range tasks {
			tasks[i] = testTask{f}
		}
		return tasks, hasMore
	}
}

type testTask struct {
	f func(ctx context.Context)
}

func (t testTask) Run(ctx context.Context) {
	t.f(ctx)
}
