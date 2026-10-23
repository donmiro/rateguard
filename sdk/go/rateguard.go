// Package rateguard is a client for rateguard-sidecar: one global rate
// limit across a fleet, from Go.
//
//	limiter := rateguard.New(rateguard.Options{}) // http://127.0.0.1:7947, or $RATEGUARD_SIDECAR_URL
//	d, err := limiter.CheckIn(ctx, "pro", "tenant:42")
//	if err != nil {
//		log.Printf("rateguard: %v", err) // d follows Options.OnError
//	}
//	if !d.Allowed {
//		w.Header().Set("Retry-After", strconv.Itoa(int(math.Ceil(d.RetryAfter.Seconds()))))
//		w.WriteHeader(http.StatusTooManyRequests)
//		return
//	}
//
// Standard library only; connections are kept alive. If the sidecar cannot
// be reached in time, or answers with anything but a decision, the
// returned error says why, and the decision is to allow the request: a
// broken limiter should not take the service down with it. OnError: Deny
// denies instead.
package rateguard

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

// DefaultURL is where the sidecar listens unless told otherwise.
const DefaultURL = "http://127.0.0.1:7947"

// OnError is what a check decides when the sidecar cannot answer.
type OnError int

const (
	// Allow the request: the default.
	Allow OnError = iota
	// Deny the request.
	Deny
)

// Options configure a Client; every field has a default.
type Options struct {
	// The sidecar: $RATEGUARD_SIDECAR_URL, or DefaultURL.
	URL string
	// How long to wait for an answer: 50 ms by default, the sidecar
	// answering in a fraction of a millisecond on loopback.
	Timeout time.Duration
	// What to decide when the sidecar cannot answer: Allow by default.
	OnError OnError
	// The HTTP client to send through; one of its own by default.
	HTTPClient *http.Client
}

// Decision says whether to serve a request, and if not, how long until a
// retry may be admitted.
type Decision struct {
	Allowed    bool
	RetryAfter time.Duration
}

// Client checks keys through a rateguard-sidecar. Safe for concurrent use.
type Client struct {
	url     string
	timeout time.Duration
	onError Decision
	http    *http.Client
}

// New makes a Client.
func New(o Options) *Client {
	url := o.URL
	if url == "" {
		url = os.Getenv("RATEGUARD_SIDECAR_URL")
	}
	if url == "" {
		url = DefaultURL
	}
	timeout := o.Timeout
	if timeout == 0 {
		timeout = 50 * time.Millisecond
	}
	client := o.HTTPClient
	if client == nil {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		// One service, one sidecar: keep enough connections for its
		// concurrency.
		transport.MaxIdleConnsPerHost = 64
		client = &http.Client{Transport: transport}
	}
	return &Client{
		url:     strings.TrimSuffix(url, "/") + "/v1/check",
		timeout: timeout,
		onError: Decision{Allowed: o.OnError == Allow},
		http:    client,
	}
}

// Check admits or denies one request for key in the default class.
func (c *Client) Check(ctx context.Context, key string) (Decision, error) {
	return c.check(ctx, map[string]string{"key": key})
}

// CheckIn admits or denies one request for key in the class class.
func (c *Client) CheckIn(ctx context.Context, class, key string) (Decision, error) {
	return c.check(ctx, map[string]string{"key": key, "class": class})
}

type answer struct {
	Allowed      *bool  `json:"allowed"`
	RetryAfterMs *int64 `json:"retry_after_ms"`
}

func (c *Client) check(ctx context.Context, body map[string]string) (Decision, error) {
	payload, err := json.Marshal(body)
	if err != nil {
		return c.onError, err
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(payload))
	if err != nil {
		return c.onError, err
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := c.http.Do(req)
	if err != nil {
		return c.onError, fmt.Errorf("rateguard: the sidecar did not answer: %w", err)
	}
	defer func() {
		// Read to the end, or the transport drops the connection.
		_, _ = io.Copy(io.Discard, res.Body)
		_ = res.Body.Close()
	}()
	if res.StatusCode == http.StatusRequestEntityTooLarge {
		// The sidecar refuses a key this large on every attempt: allowing it
		// would let the key past the limit for good.
		return Decision{}, fmt.Errorf("rateguard: the key is over the sidecar's 64 KiB limit")
	}
	if res.StatusCode != http.StatusOK {
		return c.onError, fmt.Errorf("rateguard: the sidecar answered %s", res.Status)
	}
	var a answer
	if err := json.NewDecoder(res.Body).Decode(&a); err != nil || a.Allowed == nil {
		return c.onError, fmt.Errorf("rateguard: not a decision from the sidecar: %v", err)
	}
	if *a.Allowed {
		return Decision{Allowed: true}, nil
	}
	if a.RetryAfterMs == nil {
		return c.onError, fmt.Errorf("rateguard: a denial from the sidecar with no retry_after_ms")
	}
	return Decision{RetryAfter: time.Duration(*a.RetryAfterMs) * time.Millisecond}, nil
}
