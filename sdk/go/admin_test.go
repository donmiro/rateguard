package rateguard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// A fake sidecar for the calls off the request path: answers every request
// with status and reply, and keeps what it was sent.
type adminFake struct {
	*httptest.Server
	mu   sync.Mutex
	seen []string // "METHOD path body"
}

func newAdminFake(t *testing.T, status int, reply string) *adminFake {
	f := &adminFake{}
	f.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		f.mu.Lock()
		f.seen = append(f.seen, strings.TrimSpace(r.Method+" "+r.URL.RequestURI()+" "+string(body)))
		f.mu.Unlock()
		w.WriteHeader(status)
		_, _ = w.Write([]byte(reply))
	}))
	t.Cleanup(f.Close)
	return f
}

func TestEachChangeIsAPutOfItsJSON(t *testing.T) {
	f := newAdminFake(t, 204, "")
	c, ctx := New(Options{URL: f.URL}), context.Background()
	steps := []error{
		c.SetClasses(ctx, map[string]Limit{"pro": PerMinute(60000).WithBurst(50)}),
		c.SetLimit(ctx, PerSecond(100)),
		c.SetKeyLimits(ctx, []KeyLimit{OwnLimit("pro", "tenant:42", PerHour(5000)), FollowClass("", "tenant:7", "")}),
		c.SetShadows(ctx, nil),
		c.SetDryRun(ctx, true),
	}
	for i, err := range steps {
		if err != nil {
			t.Fatalf("step %d: %v", i, err)
		}
	}
	want := []string{
		`PUT /v1/classes {"pro":{"per_minute":60000,"burst":50}}`,
		`PUT /v1/limit {"per_second":100}`,
		`PUT /v1/key_limits [{"class":"pro","key":"tenant:42","limit":{"per_hour":5000}},{"key":"tenant:7","as_class":""}]`,
		`PUT /v1/shadows {}`,
		`PUT /v1/dry_run {"on":true}`,
	}
	if strings.Join(f.seen, "\n") != strings.Join(want, "\n") {
		t.Fatalf("sent\n%s\nwant\n%s", strings.Join(f.seen, "\n"), strings.Join(want, "\n"))
	}
}

func TestStatsAreDecoded(t *testing.T) {
	f := newAdminFake(t, 200, `{"admitted": 5, "since_last_round_ms": null, "most_denied": [{"key_hash": "18446744073709551615", "denied_per_sec": 1.5, "key": "tenant:42", "class": ""}], "rejected_datagrams": {"bad_tag": 2}}`)
	s, err := New(Options{URL: f.URL}).Stats(context.Background())
	if err != nil || s.Admitted != 5 || s.SinceLastRoundMs != nil || s.RejectedDatagrams.BadTag != 2 {
		t.Fatalf("got %+v, %v", s, err)
	}
	if d := s.MostDenied[0]; d.KeyHash != "18446744073709551615" || d.Key != "tenant:42" || d.DeniedPerSec != 1.5 {
		t.Fatalf("got %+v", d)
	}
	if f.seen[0] != "GET /v1/stats" {
		t.Fatalf("sent %q", f.seen[0])
	}
}

func TestKeyStatsAskForTheKeyAndItsClassEncoded(t *testing.T) {
	f := newAdminFake(t, 200, `{"share": 1.5, "limit": {"per_second": 10, "burst": 1}, "shadow": null}`)
	c := New(Options{URL: f.URL})
	s, err := c.KeyStats(context.Background(), "pro", "tenant 42&x")
	if err != nil || s.Share != 1.5 || s.Limit != PerSecond(10).WithBurst(1) || s.Shadow != nil {
		t.Fatalf("got %+v, %v", s, err)
	}
	_, _ = c.KeyStats(context.Background(), "", "k")
	if f.seen[0] != "GET /v1/keys?class=pro&key=tenant+42%26x" || f.seen[1] != "GET /v1/keys?key=k" {
		t.Fatalf("sent %q", f.seen)
	}
}

func TestAKeyTheSidecarDoesNotTrackIsNil(t *testing.T) {
	f := newAdminFake(t, 404, `{"error": "this node does not track the key"}`)
	if s, err := New(Options{URL: f.URL}).KeyStats(context.Background(), "", "k"); s != nil || err != nil {
		t.Fatalf("got %+v, %v", s, err)
	}
}

func TestStatsFromSomethingElseAreAnError(t *testing.T) {
	f := newAdminFake(t, 404, "not found")
	if _, err := New(Options{URL: f.URL}).Stats(context.Background()); err == nil {
		t.Fatal("no error for a 404")
	}
}

func TestARefusedChangeHasTheSidecarsReason(t *testing.T) {
	f := newAdminFake(t, 400, `{"error": "a limit needs one of per_second, per_minute and per_hour"}`)
	err := New(Options{URL: f.URL}).SetLimit(context.Background(), Limit{Burst: 5})
	var sidecar *SidecarError
	if !errors.As(err, &sidecar) || sidecar.Status != 400 || !strings.Contains(err.Error(), "400: a limit needs one of") {
		t.Fatalf("got %v", err)
	}
}

func TestASidecarWithoutAdminSaysSo(t *testing.T) {
	f := newAdminFake(t, 404, "")
	err := New(Options{URL: f.URL}).SetDryRun(context.Background(), false)
	if err == nil || !strings.Contains(err.Error(), "--admin") {
		t.Fatalf("got %v", err)
	}
}

func TestASidecarThatIsGoneIsAnErrorWithNoStatus(t *testing.T) {
	_, err := New(Options{URL: "http://127.0.0.1:1", AdminTimeout: 200 * time.Millisecond}).Stats(context.Background())
	var sidecar *SidecarError
	if !errors.As(err, &sidecar) || sidecar.Status != 0 {
		t.Fatalf("got %v", err)
	}
}

// eventually returns what read gives once done holds of it: the sidecar's
// counts and a new key's state follow its background task, a tick behind.
func eventually[T any](t *testing.T, read func() (T, error), done func(T) bool) T {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		value, err := read()
		if err != nil {
			t.Fatal(err)
		}
		if done(value) || time.Now().After(deadline) {
			return value
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Against a real `rateguard-sidecar --limit 2 --burst 2 --admin`: see
// sdk/live/run.sh. Each change is put back as it was.
func TestLiveAdmin(t *testing.T) {
	url := os.Getenv("RATEGUARD_LIVE_URL")
	if url == "" {
		t.Skip("RATEGUARD_LIVE_URL is not set")
	}
	c, ctx := New(Options{URL: url, Timeout: 2 * time.Second}), context.Background()

	t.Run("a key limit lets its key past the default", func(t *testing.T) {
		key := fmt.Sprintf("go-live-own-%d", os.Getpid())
		if err := c.SetKeyLimits(ctx, []KeyLimit{OwnLimit("", key, PerSecond(1).WithBurst(4))}); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = c.SetKeyLimits(ctx, nil) })
		var allowed []bool
		for i := 0; i < 5; i++ {
			d, err := c.Check(ctx, key)
			if err != nil {
				t.Fatal(err)
			}
			allowed = append(allowed, d.Allowed)
		}
		if fmt.Sprint(allowed) != "[true true true true false]" {
			t.Fatalf("decisions %v", allowed)
		}
		s := eventually(t, func() (*KeyStats, error) { return c.KeyStats(ctx, "", key) }, func(s *KeyStats) bool { return s != nil })
		if s == nil || s.Limit != PerSecond(1).WithBurst(4) || s.Shadow != nil {
			t.Fatalf("key stats %+v", s)
		}
		if s, err := c.KeyStats(ctx, "", fmt.Sprintf("go-live-never-%d", os.Getpid())); s != nil || err != nil {
			t.Fatalf("an unseen key: %+v, %v", s, err)
		}
	})

	t.Run("a shadow counts what it would deny", func(t *testing.T) {
		key := fmt.Sprintf("go-live-shadow-%d", os.Getpid())
		before, err := c.Stats(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := c.SetShadows(ctx, map[string]Limit{"": PerSecond(1).WithBurst(1)}); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = c.SetShadows(ctx, nil) })
		for i := 0; i < 2; i++ {
			if d, err := c.Check(ctx, key); err != nil || !d.Allowed {
				t.Fatalf("check %d: %+v, %v", i, d, err)
			}
		}
		after := eventually(t, func() (Stats, error) { return c.Stats(ctx) }, func(s Stats) bool { return s.ShadowDenied > before.ShadowDenied })
		if after.ShadowDenied <= before.ShadowDenied {
			t.Fatalf("shadow_denied %d, was %d", after.ShadowDenied, before.ShadowDenied)
		}
	})

	t.Run("a refused limit is an error and a good one is taken", func(t *testing.T) {
		var sidecar *SidecarError
		if err := c.SetLimit(ctx, PerSecond(0)); !errors.As(err, &sidecar) || sidecar.Status != 400 {
			t.Fatalf("got %v", err)
		}
		for _, err := range []error{c.SetLimit(ctx, PerSecond(2).WithBurst(2)), c.SetClasses(ctx, nil), c.SetDryRun(ctx, false)} {
			if err != nil {
				t.Fatal(err)
			}
		}
		if s, err := c.Stats(ctx); err != nil || s.DryRun {
			t.Fatalf("got %+v, %v", s, err)
		}
	})
}

// Guards the JSON forms against the README's: one of the three periods.
func TestALimitMarshalsWithOnePeriod(t *testing.T) {
	data, _ := json.Marshal(PerMinute(1000))
	if string(data) != `{"per_minute":1000}` {
		t.Fatalf("got %s", data)
	}
}
