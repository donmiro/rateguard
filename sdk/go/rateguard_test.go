package rateguard

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// A fake sidecar: answers /v1/check with what answer returns for the body.
type fake struct {
	*httptest.Server
	mu    sync.Mutex
	seen  []map[string]string
	peers map[string]bool
}

func newFake(t *testing.T, status int, delay time.Duration, answer func(map[string]string) any) *fake {
	f := &fake{peers: map[string]bool{}}
	f.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]string
		_ = json.NewDecoder(r.Body).Decode(&body)
		f.mu.Lock()
		f.seen = append(f.seen, body)
		f.peers[r.RemoteAddr] = true
		f.mu.Unlock()
		time.Sleep(delay)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(answer(body))
	}))
	t.Cleanup(f.Close)
	return f
}

func allow(map[string]string) any { return map[string]any{"allowed": true} }

func TestAnAllowedCheck(t *testing.T) {
	f := newFake(t, 200, 0, allow)
	d, err := New(Options{URL: f.URL}).Check(context.Background(), "tenant:42")
	if err != nil || !d.Allowed || d.RetryAfter != 0 {
		t.Fatalf("got %+v, %v", d, err)
	}
	if f.seen[0]["key"] != "tenant:42" || len(f.seen[0]) != 1 {
		t.Fatalf("sent %v", f.seen[0])
	}
}

func TestADeniedCheckSaysWhenToRetry(t *testing.T) {
	f := newFake(t, 200, 0, func(map[string]string) any {
		return map[string]any{"allowed": false, "retry_after_ms": 1500}
	})
	d, err := New(Options{URL: f.URL}).Check(context.Background(), "k")
	if err != nil || d.Allowed || d.RetryAfter != 1500*time.Millisecond {
		t.Fatalf("got %+v, %v", d, err)
	}
}

func TestACheckInAClassSendsTheClass(t *testing.T) {
	f := newFake(t, 200, 0, allow)
	_, _ = New(Options{URL: f.URL}).CheckIn(context.Background(), "pro", "tenant:42")
	if f.seen[0]["class"] != "pro" || f.seen[0]["key"] != "tenant:42" {
		t.Fatalf("sent %v", f.seen[0])
	}
}

func TestOneConnectionServesManyChecks(t *testing.T) {
	f := newFake(t, 200, 0, allow)
	c := New(Options{URL: f.URL})
	for i := 0; i < 50; i++ {
		if d, err := c.Check(context.Background(), "k"); err != nil || !d.Allowed {
			t.Fatalf("check %d: %+v, %v", i, d, err)
		}
	}
	if len(f.peers) != 1 {
		t.Fatalf("%d connections, want 1 kept alive", len(f.peers))
	}
}

func TestASlowSidecarIsGivenUpOnAndTheRequestAllowed(t *testing.T) {
	f := newFake(t, 200, 500*time.Millisecond, func(map[string]string) any {
		return map[string]any{"allowed": false}
	})
	start := time.Now()
	d, err := New(Options{URL: f.URL, Timeout: 50 * time.Millisecond}).Check(context.Background(), "k")
	if err == nil || !d.Allowed {
		t.Fatalf("got %+v, %v: want allowed with the error", d, err)
	}
	if time.Since(start) > 400*time.Millisecond {
		t.Fatalf("waited %v", time.Since(start))
	}
}

func TestOnErrorDenyDeniesWhenTheSidecarIsGone(t *testing.T) {
	d, err := New(Options{URL: "http://127.0.0.1:1", OnError: Deny}).Check(context.Background(), "k")
	if err == nil || d.Allowed {
		t.Fatalf("got %+v, %v", d, err)
	}
}

func TestASidecarThatAnswersBadlyCountsAsAnError(t *testing.T) {
	f := newFake(t, 400, 0, func(map[string]string) any { return map[string]any{"error": "no key"} })
	if d, err := New(Options{URL: f.URL}).Check(context.Background(), "k"); err == nil || !d.Allowed {
		t.Fatalf("got %+v, %v", d, err)
	}
	if d, err := New(Options{URL: f.URL, OnError: Deny}).Check(context.Background(), "k"); err == nil || d.Allowed {
		t.Fatalf("got %+v, %v", d, err)
	}
}

func TestAKeyOverTheSidecarsLimitIsDeniedWhateverOnErrorSays(t *testing.T) {
	f := newFake(t, 413, 0, func(map[string]string) any { return map[string]any{"error": "the body is too large"} })
	d, err := New(Options{URL: f.URL}).Check(context.Background(), "k")
	if err == nil || d.Allowed || d.RetryAfter != 0 || !strings.Contains(err.Error(), "64 KiB") {
		t.Fatalf("got %+v, %v", d, err)
	}
}

func TestAServerErrorFollowsOnError(t *testing.T) {
	f := newFake(t, 500, 0, func(map[string]string) any { return map[string]any{"error": "boom"} })
	if d, err := New(Options{URL: f.URL}).Check(context.Background(), "k"); err == nil || !d.Allowed {
		t.Fatalf("got %+v, %v", d, err)
	}
	if d, err := New(Options{URL: f.URL, OnError: Deny}).Check(context.Background(), "k"); err == nil || d.Allowed {
		t.Fatalf("got %+v, %v", d, err)
	}
}

func TestTheURLComesFromTheEnvironmentByDefault(t *testing.T) {
	f := newFake(t, 200, 0, allow)
	t.Setenv("RATEGUARD_SIDECAR_URL", f.URL)
	if d, err := New(Options{}).Check(context.Background(), "k"); err != nil || !d.Allowed {
		t.Fatalf("got %+v, %v", d, err)
	}
}

func TestAPathInTheURLIsKept(t *testing.T) {
	var path string
	f := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path = r.URL.Path
		_, _ = w.Write([]byte(`{"allowed":true}`))
	}))
	t.Cleanup(f.Close)
	_, _ = New(Options{URL: f.URL + "/rateguard/"}).Check(context.Background(), "k")
	if path != "/rateguard/v1/check" {
		t.Fatalf("posted to %q", path)
	}
}

func TestADenialWithNoRetryAfterIsNoDecision(t *testing.T) {
	f := newFake(t, 200, 0, func(map[string]string) any { return map[string]any{"allowed": false} })
	if d, err := New(Options{URL: f.URL}).Check(context.Background(), "k"); err == nil || !d.Allowed {
		t.Fatalf("got %+v, %v: want the OnError decision and an error", d, err)
	}
}

func TestFailingChecksKeepTheirConnection(t *testing.T) {
	f := newFake(t, 400, 0, func(map[string]string) any { return map[string]any{"error": "no key"} })
	c := New(Options{URL: f.URL})
	for i := 0; i < 20; i++ {
		_, _ = c.Check(context.Background(), "k")
	}
	if len(f.peers) != 1 {
		t.Fatalf("%d connections for 20 failing checks, want 1", len(f.peers))
	}
}

// Against a real `rateguard-sidecar --limit 2 --burst 2`: see
// sdk/live/run.sh. Go cannot send a lone surrogate (its JSON encoder
// replaces it), so that live case is left to the other clients.
func TestLiveTheBurstIsAllowedAndTheNextDenied(t *testing.T) {
	url := os.Getenv("RATEGUARD_LIVE_URL")
	if url == "" {
		t.Skip("RATEGUARD_LIVE_URL is not set")
	}
	c := New(Options{URL: url, Timeout: 2 * time.Second})
	key := fmt.Sprintf("go-live-%d", os.Getpid())
	for i := 0; i < 2; i++ {
		if d, err := c.Check(context.Background(), key); err != nil || !d.Allowed {
			t.Fatalf("check %d: got %+v, %v", i, d, err)
		}
	}
	d, err := c.Check(context.Background(), key)
	if err != nil || d.Allowed || d.RetryAfter <= 0 {
		t.Fatalf("third check: got %+v, %v", d, err)
	}
}
