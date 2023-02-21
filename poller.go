package poller

import (
	"context"
	job "github.com/CherkashinEvgeny/gojob"
	pool "github.com/CherkashinEvgeny/gopool"
	"github.com/pkg/errors"
	"time"
)

type Task interface {
	Run(ctx context.Context)
}

type Poller struct {
	job       *job.Job
	n         int
	size      int
	threshold int
	pool      *pool.Pool
	ch        chan struct{}
	next      func(ctx context.Context, limit int) (tasks []Task, hasMore bool)
}

var invalidSizeParameterError = errors.New("size should be >= 1")

func New(
	period time.Duration,
	size int,
	threshold int,
	concurrency int,
	next func(ctx context.Context, limit int) (tasks []Task, hasMore bool),
) (poller *Poller) {
	if size < 1 {
		panic(invalidSizeParameterError)
	}
	if concurrency <= 0 {
		concurrency = max(0, size+concurrency)
	} else {
		concurrency = min(concurrency, size)
	}
	if threshold <= 0 {
		threshold = max(0, size+threshold)
	} else {
		threshold = min(threshold, size)
	}
	poller = &Poller{}
	poller.job = job.New(poller.payload, job.Delay(0, job.Period(period)))
	poller.n = 0
	poller.size = size
	poller.threshold = threshold
	poller.pool = pool.New(concurrency)
	poller.ch = make(chan struct{}, size)
	poller.next = next
	return
}

func max(values ...int) (result int) {
	result = values[0]
	for i := 1; i < len(values); i++ {
		value := values[i]
		if result < value {
			result = value
		}
	}
	return
}

func min(values ...int) (result int) {
	result = values[0]
	for i := 1; i < len(values); i++ {
		value := values[i]
		if result > value {
			result = value
		}
	}
	return
}

func (k *Poller) Start() {
	k.StartContext(context.Background())
}

func (k *Poller) StartContext(ctx context.Context) {
	k.job.StartContext(ctx)
}

func (k *Poller) payload(ctx context.Context) {
	for k.waitForThresholdCondition(ctx) {
		tasks, hasMore := k.next(ctx, k.size-k.n)
		for _, task := range tasks {
			k.scheduleTask(ctx, task)
		}
		if !hasMore {
			return
		}
	}
}

func (k *Poller) waitForThresholdCondition(ctx context.Context) (success bool) {
	return k.waitN(ctx, k.n-(k.threshold-1))
}

func (k *Poller) scheduleTask(ctx context.Context, task Task) {
	k.n++
	k.pool.Exec(func() {
		defer func() {
			k.ch <- struct{}{}
		}()
		task.Run(ctx)
	})
}

func (k *Poller) Stop() {
	k.StopContext(context.Background())
}

func (k *Poller) StopContext(ctx context.Context) {
	k.job.StopContext(ctx)
	if !isContextCanceled(ctx) {
		k.waitForGracefulShutdown(ctx)
	}
}

func (k *Poller) waitForGracefulShutdown(ctx context.Context) {
	_ = k.waitN(ctx, k.n)
}

func (k *Poller) waitN(ctx context.Context, n int) (success bool) {
	if n > k.n {
		n = k.n
	}
	for i := 0; i < n; i++ {
		select {
		case <-ctx.Done():
			return
		case <-k.ch:
			k.n--
			break
		}
	}
	success = true
	return
}

func isContextCanceled(ctx context.Context) (canceled bool) {
	select {
	case <-ctx.Done():
		canceled = true
	default:
		break
	}
	return
}
