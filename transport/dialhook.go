package transport

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/p1neappleXpress/OpenFlux/netbind"
)

type DialFunc func(ctx context.Context, network, address string) (net.Conn, error)

type DialHook func(ctx context.Context, network, address string, direct DialFunc) (net.Conn, error)

var (
	dialHookMu sync.RWMutex
	dialHook   DialHook
	dialDirect = netbind.Wrap(ProtectedDialer())
)

type DialHint struct {
	Subject string
	Spread  bool
}

type dialHintKey struct{}

func WithDialHint(ctx context.Context, h DialHint) context.Context {
	return context.WithValue(ctx, dialHintKey{}, h)
}

func DialHintFrom(ctx context.Context) (DialHint, bool) {
	h, ok := ctx.Value(dialHintKey{}).(DialHint)
	return h, ok && h.Subject != ""
}

type DialFeedback func(subject string, err error)

var dialFeedback atomic.Pointer[DialFeedback]

func SetDialFeedback(f DialFeedback) {
	if f == nil {
		dialFeedback.Store(nil)
		return
	}
	dialFeedback.Store(&f)
}

func ReportDial(ctx context.Context, err error) {
	h, ok := DialHintFrom(ctx)
	if !ok || err == nil {
		return
	}
	if f := dialFeedback.Load(); f != nil {
		(*f)(h.Subject, err)
	}
}

func SetDialHook(h DialHook) {
	dialHookMu.Lock()
	defer dialHookMu.Unlock()
	dialHook = h
}

func DialHookInstalled() bool {
	dialHookMu.RLock()
	defer dialHookMu.RUnlock()
	return dialHook != nil
}

func DirectDialContext(ctx context.Context, network, address string) (net.Conn, error) {
	return dialDirect.DialContext(ctx, network, address)
}

func DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	dialHookMu.RLock()
	h := dialHook
	dialHookMu.RUnlock()
	if h == nil {
		return DirectDialContext(ctx, network, address)
	}
	return h(ctx, network, address, DirectDialContext)
}

func DialTimeout(network, address string, timeout time.Duration) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return DialContext(ctx, network, address)
}