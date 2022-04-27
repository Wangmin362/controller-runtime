/*
Copyright 2018 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/go-logr/logr"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/internal/controller/metrics"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/runtime/inject"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

var _ inject.Injector = &Controller{}

// Controller implements controller.Controller.
type Controller struct {
	// Name is used to uniquely identify a Controller in tracing, logging and monitoring.  Name is required.
	Name string

	// MaxConcurrentReconciles is the maximum number of concurrent Reconciles which can be run. Defaults to 1.
	MaxConcurrentReconciles int

	// Reconciler is a function that can be called at any time with the Name / Namespace of an object and
	// ensures that the state of the system matches the state specified in the object.
	// Defaults to the DefaultReconcileFunc.
	Do reconcile.Reconciler

	// MakeQueue constructs the queue for this controller once the controller is ready to start.
	// This exists because the standard Kubernetes workqueues start themselves immediately, which
	// leads to goroutine leaks if something calls controller.New repeatedly.
	MakeQueue func() workqueue.RateLimitingInterface

	// Queue is an listeningQueue that listens for events from Informers and adds object keys to
	// the Queue for processing
	Queue workqueue.RateLimitingInterface

	// SetFields is used to inject dependencies into other objects such as Sources, EventHandlers and Predicates
	// Deprecated: the caller should handle injected fields itself.
	SetFields func(i interface{}) error

	// mu is used to synchronize Controller setup
	mu sync.Mutex

	// Started is true if the Controller has been Started
	// TODO 该值什么时候被设置为true的？ Manger.Start()??
	Started bool

	// ctx is the context that was passed to Start() and used when starting watches.
	//
	// According to the docs, contexts should not be stored in a struct: https://golang.org/pkg/context,
	// while we usually always strive to follow best practices, we consider this a legacy case and it should
	// undergo a major refactoring and redesign to allow for context to not be stored in a struct.
	ctx context.Context

	// CacheSyncTimeout refers to the time limit set on waiting for cache to sync
	// Defaults to 2 minutes if not set.
	CacheSyncTimeout time.Duration

	// startWatches maintains a list of sources, handlers, and predicates to start when the controller is started.
	// 保存了所有监听对象的处理，每个Controller至少都有一个CRD对象监听源，除此之外还有可能监听通过Builder.For(), Builder.Watches()方法添加的对象
	startWatches []watchDescription

	// Log is used to log messages to users during reconciliation, or for example when a watch is started.
	Log logr.Logger

	// RecoverPanic indicates whether the panic caused by reconcile should be recovered.
	RecoverPanic bool
}

// watchDescription contains all the information necessary to start a watch.
type watchDescription struct {
	src        source.Source
	handler    handler.EventHandler
	predicates []predicate.Predicate
}

// Reconcile implements reconcile.Reconciler.
func (c *Controller) Reconcile(ctx context.Context, req reconcile.Request) (_ reconcile.Result, err error) {
	if c.RecoverPanic {
		defer func() {
			if r := recover(); r != nil {
				for _, fn := range utilruntime.PanicHandlers {
					fn(r)
				}
				err = fmt.Errorf("panic: %v [recovered]", r)
			}
		}()
	}
	log := c.Log.WithValues("name", req.Name, "namespace", req.Namespace)
	ctx = logf.IntoContext(ctx, log)
	return c.Do.Reconcile(ctx, req)
}

// Watch implements controller.Controller.
func (c *Controller) Watch(src source.Source, evthdler handler.EventHandler, prct ...predicate.Predicate) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Inject Cache into arguments
	// 这里向Source中注入了informerMap,也就是Kind.cache字段被填充，监听资源的informer就从里面获取
	if err := c.SetFields(src); err != nil {
		return err
	}
	if err := c.SetFields(evthdler); err != nil {
		return err
	}
	for _, pr := range prct {
		if err := c.SetFields(pr); err != nil {
			return err
		}
	}

	// Controller hasn't started yet, store the watches locally and return.
	//
	// These watches are going to be held on the controller struct until the manager or user calls Start(...).
	if !c.Started {
		// TODO 刚开始newController的时候，肯定还没有启动Controller，Controller的启动肯定是在Manger.Start()当中完成的
		c.startWatches = append(c.startWatches, watchDescription{src: src, handler: evthdler, predicates: prct})
		return nil
	}

	c.Log.Info("Starting EventSource", "source", src)
	return src.Start(c.ctx, evthdler, c.Queue, prct...)
}

// Start implements controller.Controller.
func (c *Controller) Start(ctx context.Context) error {
	// use an IIFE to get proper lock handling
	// but lock outside to get proper handling of the queue shutdown
	c.mu.Lock()
	if c.Started {
		return errors.New("controller was started more than once. This is likely to be caused by being added to a manager multiple times")
	}

	c.initMetrics()

	// Set the internal context.
	c.ctx = ctx

	// 创建workqueue
	c.Queue = c.MakeQueue()
	go func() {
		<-ctx.Done()
		c.Queue.ShutDown()
	}()

	wg := &sync.WaitGroup{}
	err := func() error {
		defer c.mu.Unlock()

		// TODO(pwittrock): Reconsider HandleCrash
		defer utilruntime.HandleCrash()

		// NB(directxman12): launch the sources *before* trying to wait for the
		// caches to sync so that they have a chance to register their intendeded
		// caches.
		for _, watch := range c.startWatches {
			c.Log.Info("Starting EventSource", "source", fmt.Sprintf("%s", watch.src))

			// 构建Controller的时候也执行过doWatch()方法，但是这里面并没有执行Source.Start()，因为此时Controller还没有启动
			// 到了这里才把每个监听资源的EventHandler注册到Informer当中
			if err := watch.src.Start(ctx, watch.handler, c.Queue, watch.predicates...); err != nil {
				return err
			}
		}

		// Start the SharedIndexInformer factories to begin populating the SharedIndexInformer caches
		c.Log.Info("Starting Controller")

		// 这里实际上在等待所有的监听对象的Informer准备好，只要有一个监听对象的Informer回去失败，当前Controller就认为启动失败了
		for _, watch := range c.startWatches {
			// 目前使用的Source都是Kind,因此肯定是SyncingSource类型
			syncingSource, ok := watch.src.(source.SyncingSource)
			if !ok {
				continue
			}

			if err := func() error {
				// use a context with timeout for launching sources and syncing caches.
				sourceStartCtx, cancel := context.WithTimeout(ctx, c.CacheSyncTimeout)
				defer cancel()

				// WaitForSync waits for a definitive timeout, and returns if there
				// is an error or a timeout
				// 等待Kind.started channel中写入数据
				if err := syncingSource.WaitForSync(sourceStartCtx); err != nil {
					err := fmt.Errorf("failed to wait for %s caches to sync: %w", c.Name, err)
					c.Log.Error(err, "Could not wait for Cache to sync")
					return err
				}

				return nil
			}(); err != nil {
				return err
			}
		}

		// All the watches have been started, we can reset the local slice.
		//
		// We should never hold watches more than necessary, each watch source can hold a backing cache,
		// which won't be garbage collected if we hold a reference to it.
		c.startWatches = nil

		// Launch workers to process resources
		c.Log.Info("Starting workers", "worker count", c.MaxConcurrentReconciles)
		wg.Add(c.MaxConcurrentReconciles)
		// 启动多个Reconcile消费Informer放入workqueue中的事件
		for i := 0; i < c.MaxConcurrentReconciles; i++ {
			go func() {
				defer wg.Done()
				// Run a worker thread that just dequeues items, processes them, and marks them done.
				// It enforces that the reconcileHandler is never invoked concurrently with the same object.
				for c.processNextWorkItem(ctx) {
				}
			}()
		}

		c.Started = true
		return nil
	}()
	if err != nil {
		return err
	}

	<-ctx.Done()
	c.Log.Info("Shutdown signal received, waiting for all workers to finish")
	// 等待所有的Reconcile处理完毕
	wg.Wait()
	c.Log.Info("All workers finished")
	return nil
}

// processNextWorkItem will read a single work item off the workqueue and
// attempt to process it, by calling the reconcileHandler.
func (c *Controller) processNextWorkItem(ctx context.Context) bool {
	obj, shutdown := c.Queue.Get()
	if shutdown {
		// Stop working
		return false
	}

	// We call Done here so the workqueue knows we have finished
	// processing this item. We also must remember to call Forget if we
	// do not want this work item being re-queued. For example, we do
	// not call Forget if a transient error occurs, instead the item is
	// put back on the workqueue and attempted again after a back-off
	// period.
	defer c.Queue.Done(obj)

	ctrlmetrics.ActiveWorkers.WithLabelValues(c.Name).Add(1)
	defer ctrlmetrics.ActiveWorkers.WithLabelValues(c.Name).Add(-1)

	c.reconcileHandler(ctx, obj)
	return true
}

const (
	labelError        = "error"
	labelRequeueAfter = "requeue_after"
	labelRequeue      = "requeue"
	labelSuccess      = "success"
)

func (c *Controller) initMetrics() {
	ctrlmetrics.ActiveWorkers.WithLabelValues(c.Name).Set(0)
	ctrlmetrics.ReconcileErrors.WithLabelValues(c.Name).Add(0)
	ctrlmetrics.ReconcileTotal.WithLabelValues(c.Name, labelError).Add(0)
	ctrlmetrics.ReconcileTotal.WithLabelValues(c.Name, labelRequeueAfter).Add(0)
	ctrlmetrics.ReconcileTotal.WithLabelValues(c.Name, labelRequeue).Add(0)
	ctrlmetrics.ReconcileTotal.WithLabelValues(c.Name, labelSuccess).Add(0)
	ctrlmetrics.WorkerCount.WithLabelValues(c.Name).Set(float64(c.MaxConcurrentReconciles))
}

func (c *Controller) reconcileHandler(ctx context.Context, obj interface{}) {
	// Update metrics after processing each item
	reconcileStartTS := time.Now()
	defer func() {
		c.updateMetrics(time.Since(reconcileStartTS))
	}()

	// Make sure that the the object is a valid request.
	req, ok := obj.(reconcile.Request)
	if !ok {
		// As the item in the workqueue is actually invalid, we call
		// Forget here else we'd go into a loop of attempting to
		// process a work item that is invalid.
		c.Queue.Forget(obj)
		c.Log.Error(nil, "Queue item was not a Request", "type", fmt.Sprintf("%T", obj), "value", obj)
		// Return true, don't take a break
		return
	}

	log := c.Log.WithValues("name", req.Name, "namespace", req.Namespace)
	ctx = logf.IntoContext(ctx, log)

	// RunInformersAndControllers the syncHandler, passing it the Namespace/Name string of the
	// resource to be synced.
	// 这里最终就进入到用户自己实现的Reconcile当中了，可以看出其返回值非常重要，如果返回值有错误，那么直接重新入队
	// 当然，用户也可以选择延时一会儿重新入队
	result, err := c.Reconcile(ctx, req)
	switch {
	case err != nil:
		c.Queue.AddRateLimited(req)
		ctrlmetrics.ReconcileErrors.WithLabelValues(c.Name).Inc()
		ctrlmetrics.ReconcileTotal.WithLabelValues(c.Name, labelError).Inc()
		log.Error(err, "Reconciler error")
	case result.RequeueAfter > 0:
		// The result.RequeueAfter request will be lost, if it is returned
		// along with a non-nil error. But this is intended as
		// We need to drive to stable reconcile loops before queuing due
		// to result.RequestAfter
		c.Queue.Forget(obj)
		c.Queue.AddAfter(req, result.RequeueAfter)
		ctrlmetrics.ReconcileTotal.WithLabelValues(c.Name, labelRequeueAfter).Inc()
	case result.Requeue:
		c.Queue.AddRateLimited(req)
		ctrlmetrics.ReconcileTotal.WithLabelValues(c.Name, labelRequeue).Inc()
	default:
		// Finally, if no error occurs we Forget this item so it does not
		// get queued again until another change happens.
		// 如果用户自定义的Reconcile既没有重新入队的需求，也没有错误，那么就任务当前事件已经处理完毕，就可以直接从队列当中丢弃
		c.Queue.Forget(obj)
		ctrlmetrics.ReconcileTotal.WithLabelValues(c.Name, labelSuccess).Inc()
	}
}

// GetLogger returns this controller's logger.
func (c *Controller) GetLogger() logr.Logger {
	return c.Log
}

// InjectFunc implement SetFields.Injector.
func (c *Controller) InjectFunc(f inject.Func) error {
	c.SetFields = f
	return nil
}

// updateMetrics updates prometheus metrics within the controller.
func (c *Controller) updateMetrics(reconcileTime time.Duration) {
	ctrlmetrics.ReconcileTime.WithLabelValues(c.Name).Observe(reconcileTime.Seconds())
}
