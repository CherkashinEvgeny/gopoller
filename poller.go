package poller

import (
	"context"
	"errors"
	"time"

	"github.com/CherkashinEvgeny/gojob"
	"github.com/CherkashinEvgeny/gopool"
)

type Task interface {
	Run(ctx context.Context)
}

type Poller struct {
	job       *job.Job
	n         int
	size      int
	threshold int
	worker    *pool.Pool
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
	poller.worker = pool.New(concurrency)
	poller.ch = make(chan struct{}, size)
	poller.next = next
	return poller
}

func max(values ...int) (result int) {
	result = values[0]
	for i := 1; i < len(values); i++ {
		value := values[i]
		if result < value {
			result = value
		}
	}
	return result
}

func min(values ...int) (result int) {
	result = values[0]
	for i := 1; i < len(values); i++ {
		value := values[i]
		if result > value {
			result = value
		}
	}
	return result
}

func (p *Poller) Start() {
	p.StartContext(context.Background())
}

func (p *Poller) StartContext(ctx context.Context) {
	p.job.StartContext(ctx)
}

func (p *Poller) payload(ctx context.Context) {
	for p.waitForThresholdCondition(ctx) {
		tasks, hasMore := p.next(ctx, p.size-p.n)
		for _, task := range tasks {
			p.scheduleTask(ctx, task)
		}
		if !hasMore {
			return
		}
	}
}

func (p *Poller) waitForThresholdCondition(ctx context.Context) (success bool) {
	return p.waitN(ctx, p.n-(p.threshold-1))
}

func (p *Poller) scheduleTask(ctx context.Context, task Task) {
	p.n++
	p.worker.Exec(func() {
		defer func() {
			p.ch <- struct{}{}
		}()
		task.Run(ctx)
	})
}

func (p *Poller) Done() <-chan struct{} {
	return p.job.Done()
}

func (p *Poller) Stop() {
	p.StopContext(context.Background())
}

func (p *Poller) StopContext(ctx context.Context) {
	p.job.StopContext(ctx)
	if !isContextCanceled(ctx) {
		p.waitForGracefulShutdown(ctx)
	}
}

func (p *Poller) waitForGracefulShutdown(ctx context.Context) {
	_ = p.waitN(ctx, p.n)
}

func (p *Poller) waitN(ctx context.Context, n int) (success bool) {
	if n > p.n {
		n = p.n
	}
	for i := 0; i < n; i++ {
		select {
		case <-ctx.Done():
			return false
		case <-p.ch:
			p.n--
			break
		}
	}
	return true
}

func isContextCanceled(ctx context.Context) (canceled bool) {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}
