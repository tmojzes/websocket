//go:build !js

package websocket_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/internal/test/assert"
)

// countingConn counts Write calls on the client's network connection.
type countingConn struct {
	net.Conn
	writes *atomic.Int64
}

func (c countingConn) Write(p []byte) (int, error) {
	c.writes.Add(1)
	return c.Conn.Write(p)
}

// dialCounting dials an echo server, counting writes on the client connection.
func dialCounting(t testing.TB, ctx context.Context, writeBufferSize int) (*websocket.Conn, *atomic.Int64) {
	t.Helper()
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		c.SetReadLimit(-1)
		for {
			typ, p, err := c.Read(ctx)
			if err != nil {
				return
			}
			if err := c.Write(ctx, typ, p); err != nil {
				return
			}
		}
	}))
	t.Cleanup(s.Close)

	writes := &atomic.Int64{}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := (&net.Dialer{}).DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		return countingConn{Conn: conn, writes: writes}, nil
	}
	c, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(s.URL, "http"), &websocket.DialOptions{
		HTTPClient:      &http.Client{Transport: transport},
		WriteBufferSize: writeBufferSize,
	})
	assert.Success(t, err)
	c.SetReadLimit(-1)
	t.Cleanup(func() { c.CloseNow() })
	return c, writes
}

func TestDialWriteBufferSize(t *testing.T) {
	t.Parallel()

	const messageSize = 64 << 10
	tests := []struct {
		name            string
		writeBufferSize int
		maxWrites       int64
		minWrites       int64
	}{
		// The default 4096-byte buffer flushes once per 4 KiB of masked payload.
		{name: "default", writeBufferSize: 0, minWrites: messageSize / 4096},
		{name: "explicitDefault", writeBufferSize: 4096, minWrites: messageSize / 4096},
		// A buffer larger than the frame sends it in a single write.
		{name: "large", writeBufferSize: 128 << 10, maxWrites: 2},
		// A buffer smaller than the default still works, with more writes.
		{name: "small", writeBufferSize: 1024, minWrites: messageSize / 1024},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			c, writes := dialCounting(t, ctx, tc.writeBufferSize)

			msg := make([]byte, messageSize)
			_, err := rand.Read(msg)
			assert.Success(t, err)

			before := writes.Load()
			err = c.Write(ctx, websocket.MessageBinary, msg)
			assert.Success(t, err)
			got := writes.Load() - before

			if tc.maxWrites > 0 && got > tc.maxWrites {
				t.Fatalf("%d writes for a %d byte message, want at most %d", got, messageSize, tc.maxWrites)
			}
			if tc.minWrites > 0 && got < tc.minWrites {
				t.Fatalf("%d writes for a %d byte message, want at least %d", got, messageSize, tc.minWrites)
			}

			typ, echoed, err := c.Read(ctx)
			assert.Success(t, err)
			assert.Equal(t, "message type", websocket.MessageBinary, typ)
			if !bytes.Equal(msg, echoed) {
				t.Fatal("echoed message differs from the one sent")
			}

			// The connection keeps working after a large write.
			err = c.Write(ctx, websocket.MessageText, []byte("small"))
			assert.Success(t, err)
			_, echoed, err = c.Read(ctx)
			assert.Success(t, err)
			assert.Equal(t, "small message", "small", string(echoed))

			assert.Success(t, c.Close(websocket.StatusNormalClosure, ""))
		})
	}
}

func BenchmarkDialWriteBufferSize(b *testing.B) {
	const messageSize = 64 << 10
	for _, size := range []int{0, 128 << 10} {
		name := "default"
		if size > 0 {
			name = "128KiB"
		}
		b.Run(name, func(b *testing.B) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			c, writes := dialCounting(b, ctx, size)
			msg := make([]byte, messageSize)

			b.SetBytes(messageSize)
			b.ReportAllocs()
			b.ResetTimer()
			before := writes.Load()
			for i := 0; i < b.N; i++ {
				if err := c.Write(ctx, websocket.MessageBinary, msg); err != nil {
					b.Fatal(err)
				}
				if _, _, err := c.Read(ctx); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(writes.Load()-before)/float64(b.N), "writes/op")
		})
	}
}
