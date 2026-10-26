package redis

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/tr1v3r/airlock/protocol"
)

func TestConstructorSocketDeadlines(t *testing.T) {
	cases := []struct {
		name        string
		read, write time.Duration
		accept      bool
	}{
		{"defaults", 0, 0, true},
		{"positive", time.Second, time.Second, true},
		{"no_configured_read_timeout", -1, 0, true},
		{"no_configured_write_timeout", 0, -1, true},
		{"context_only", -1, -1, true},
		{"disabled_read", -2, 0, false},
		{"disabled_write", 0, -2, false},
		{"disabled_both", -2, -2, false},
		{"disabled_read_context_write", -2, -1, false},
		{"context_read_disabled_write", -1, -2, false},
		{"other_negative_read", -3, 0, false},
		{"other_negative_write", 0, -3, false},
		{"negative_duration_read", -time.Second, 0, false},
		{"negative_duration_write", 0, -time.Second, false},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			standalone := goredis.NewClient(&goredis.Options{ContextTimeoutEnabled: true, ReadTimeout: test.read, WriteTimeout: test.write})
			defer func() { _ = standalone.Close() }()
			cluster := goredis.NewClusterClient(&goredis.ClusterOptions{ContextTimeoutEnabled: true, ReadTimeout: test.read, WriteTimeout: test.write})
			defer func() { _ = cluster.Close() }()
			for name, c := range map[string]goredis.UniversalClient{"standalone": standalone, "cluster": cluster} {
				_, err := New(c, "")
				if (err == nil) != test.accept {
					t.Errorf("%s raw read=%v write=%v accepted=%v want=%v: %v", name, test.read, test.write, err == nil, test.accept, err)
				}
			}
		})
	}
}

// No configured socket timeout (-1) must still respect the operation deadline
// for both black-hole reads and writes. Pipe closure is a bounded test escape,
// not the mechanism expected to complete Submit.
func TestContextOnlySocketDeadlineBoundsBlackHole(t *testing.T) {
	for _, configured := range []time.Duration{0, -1} {
		for _, drain := range []bool{false, true} {
			operation := "write"
			if drain {
				operation = "read"
			}
			t.Run(fmt.Sprintf("%s_timeout_%d", operation, configured), func(t *testing.T) {
				local, remote := net.Pipe()
				defer func() { _ = local.Close(); _ = remote.Close() }()
				if drain {
					go func() { _, _ = io.Copy(io.Discard, remote) }()
				}
				client := goredis.NewClient(&goredis.Options{
					ContextTimeoutEnabled: true, ReadTimeout: configured, WriteTimeout: configured,
					Dialer: func(context.Context, string, string) (net.Conn, error) { return local, nil },
				})
				defer func() { _ = client.Close() }()
				d, err := New(client, "", WithOperationTimeout(30*time.Millisecond))
				if err != nil {
					t.Fatal(err)
				}
				type outcome struct {
					result protocol.Result
					err    error
				}
				done := make(chan outcome, 1)
				go func() {
					result, err := d.Submit(context.Background(), request())
					done <- outcome{result, err}
				}()
				select {
				case got := <-done:
					if got.result != (protocol.Result{}) {
						t.Fatalf("black-hole I/O authorized result: %+v", got.result)
					}
					if !errors.Is(got.err, context.DeadlineExceeded) && !errors.Is(got.err, os.ErrDeadlineExceeded) {
						t.Fatalf("black-hole I/O must fail with a timeout, got %v", got.err)
					}
				case <-time.After(2 * time.Second):
					_ = local.Close()
					_ = remote.Close()
					<-done
					t.Fatal("operation deadline failed to bound black-hole I/O")
				}
			})
		}
	}
}
