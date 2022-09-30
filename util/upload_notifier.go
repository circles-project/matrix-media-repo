package util

import (
	"sync"
	"time"

	"github.com/turt2live/matrix-media-repo/common/rcontext"
)

type mediaSet map[chan struct{}]struct{}

var waiterLock = &sync.Mutex{}
var waiters = map[string]mediaSet{}

func StartWaitForUpload(ctx rcontext.RequestContext, origin string, mediaID string) chan struct{} {
	key := origin + mediaID
	ch := make(chan struct{})

	ctx.Log.Info("acquiring waiterLock for StartWaitForUpload")
	start := time.Now()
	waiterLock.Lock()
	ctx.Log.Info("acquired waiterLock for StartWaitForUpload in ", time.Since(start))

	var set mediaSet
	var ok bool
	if set, ok = waiters[key]; !ok {
		set = make(mediaSet)
		waiters[key] = set
		ctx.Log.Info("adding new waiter set")
	}
	set[ch] = struct{}{}

	ctx.Log.Info("StartWaitForUpload releasing waiterLock")
	waiterLock.Unlock()

	return ch
}

func CancelWaitForUpload(ctx rcontext.RequestContext, ch chan struct{}, origin string, mediaID string) {
	key := origin + mediaID

	ctx.Log.Info("acquiring waiterLock for CancelWaitForUpload")
	start := time.Now()
	waiterLock.Lock()
	ctx.Log.Info("acquired waiterLock for CancelWaitForUpload in ", time.Since(start))

	defer func() {
		ctx.Log.Info("CancelWaitForUpload releasing waiterLock")
		waiterLock.Unlock()
	}()

	var set mediaSet
	var ok bool
	if set, ok = waiters[key]; !ok {
		// no waiter in set
		ctx.Log.Info("CancelWaitForUpload called for key that has no waiters, doing nothing")
		return
	}

	delete(set, ch)
	close(ch)

	if len(set) == 0 {
		delete(waiters, key)
	}
}

func WaitForUpload(ctx rcontext.RequestContext, ch chan struct{}, origin string, mediaID string, timeout time.Duration) bool {
	key := origin + mediaID

	ctx.Log.Info("acquiring waiterLock for WaitForUpload")
	start := time.Now()
	waiterLock.Lock()
	ctx.Log.Info("acquired waiterLock for WaitForUpload in ", time.Since(start))

	var set mediaSet
	var ok bool
	if set, ok = waiters[key]; !ok {
		set = make(mediaSet)
		waiters[key] = set
	}
	ctx.Log.Info("WaitForUpload releasing waiterLock")
	waiterLock.Unlock()

	defer func() {
		ctx.Log.Info("acquiring waiterLock for WaitForUpload end")
		start := time.Now()
		waiterLock.Lock()
		ctx.Log.Info("acquired waiterLock for WaitForUpload end in ", time.Since(start))

		delete(set, ch)
		close(ch)

		if len(set) == 0 {
			delete(waiters, key)
		}

		ctx.Log.Info("WaitForUpload end release waiterLock")
		waiterLock.Unlock()
	}()

	select {
	case <-ch:
		return true
	case <-time.After(timeout):
		return false
	}
}

func NotifyUpload(ctx rcontext.RequestContext, origin string, mediaId string) {
	ctx.Log.Info("acquiring waiterLock for WaitForUpload")
	start := time.Now()
	waiterLock.Lock()
	ctx.Log.Info("acquired waiterLock for WaitForUpload in ", time.Since(start))

	defer func() {
		ctx.Log.Info("NotifyUpload releasing waiterLock")
		waiterLock.Unlock()
	}()

	set := waiters[origin+mediaId]

	if set == nil {
		ctx.Log.Info("no waiters")
		return
	}

	ctx.Log.Infof("notifying %d listeners of upload complete", len(set))
	for channel := range set {
		start := time.Now()
		channel <- struct{}{}
		ctx.Log.Info("upload listener notified in ", time.Since(start))
	}
}
