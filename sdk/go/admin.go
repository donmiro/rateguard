package rateguard

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

// Limit is a count of requests per second, minute or hour, and a burst:
// exactly one of PerSecond, PerMinute and PerHour is set. A Burst of 0 is
// the sidecar's default, a twentieth of the count.
type Limit struct {
	PerSecond uint32 `json:"per_second,omitempty"`
	PerMinute uint32 `json:"per_minute,omitempty"`
	PerHour   uint32 `json:"per_hour,omitempty"`
	Burst     uint32 `json:"burst,omitempty"`
}

// PerSecond is a limit of n requests a second.
func PerSecond(n uint32) Limit { return Limit{PerSecond: n} }

// PerMinute is a limit of n requests a minute.
func PerMinute(n uint32) Limit { return Limit{PerMinute: n} }

// PerHour is a limit of n requests an hour.
func PerHour(n uint32) Limit { return Limit{PerHour: n} }

// WithBurst is the limit with a burst of n.
func (l Limit) WithBurst(n uint32) Limit {
	l.Burst = n
	return l
}

// KeyLimit is one key's exception: its own Limit, or AsClass, the class
// whose limit it follows. Make one with OwnLimit or FollowClass.
type KeyLimit struct {
	// The class the service checks the key in; "" for the default.
	Class   string  `json:"class,omitempty"`
	Key     string  `json:"key"`
	Limit   *Limit  `json:"limit,omitempty"`
	AsClass *string `json:"as_class,omitempty"`
}

// OwnLimit gives key, checked in class, a limit of its own.
func OwnLimit(class, key string, limit Limit) KeyLimit {
	return KeyLimit{Class: class, Key: key, Limit: &limit}
}

// FollowClass gives key, checked in class, the limit of the class asClass,
// tariff changes included.
func FollowClass(class, key, asClass string) KeyLimit {
	return KeyLimit{Class: class, Key: key, AsClass: &asClass}
}

// Stats is what the sidecar's /v1/stats tells; its README says what each
// field means.
type Stats struct {
	Admitted          uint64            `json:"admitted"`
	Denied            uint64            `json:"denied"`
	Overflowed        uint64            `json:"overflowed"`
	UnknownClass      uint64            `json:"unknown_class"`
	ClusterSize       int               `json:"cluster_size"`
	CountedSize       int               `json:"counted_size"`
	Suspects          int               `json:"suspects"`
	TrackedKeys       int               `json:"tracked_keys"`
	HotKeys           int               `json:"hot_keys"`
	Joining           bool              `json:"joining"`
	Learning          bool              `json:"learning"`
	WithoutQuorum     bool              `json:"without_quorum"`
	MembersFull       bool              `json:"members_full"`
	SinceLastRoundMs  *uint64           `json:"since_last_round_ms"`
	Running           bool              `json:"running"`
	DryRun            bool              `json:"dry_run"`
	ShadowDenied      uint64            `json:"shadow_denied"`
	MostDenied        []DeniedKey       `json:"most_denied"`
	MostShadowDenied  []DeniedKey       `json:"most_shadow_denied"`
	GossipAuth        string            `json:"gossip_auth"`
	RejectedDatagrams RejectedDatagrams `json:"rejected_datagrams"`
	IdentityConflicts uint64            `json:"identity_conflicts"`
	MembersRefused    uint64            `json:"members_refused"`
}

// DeniedKey is one of the keys denied most lately, with its rates averaged
// over about ten seconds. Key and Class are there while the sidecar knows
// the key's name.
type DeniedKey struct {
	KeyHash            string  `json:"key_hash"`
	DeniedPerSec       float64 `json:"denied_per_sec"`
	RequestsPerSec     float64 `json:"requests_per_sec"`
	ShadowDeniedPerSec float64 `json:"shadow_denied_per_sec"`
	Key                string  `json:"key,omitempty"`
	Class              string  `json:"class,omitempty"`
	KeyTruncated       bool    `json:"key_truncated,omitempty"`
}

// RejectedDatagrams counts the gossip the sidecar dropped, by reason.
type RejectedDatagrams struct {
	Unsigned          uint64 `json:"unsigned"`
	BadTag            uint64 `json:"bad_tag"`
	Malformed         uint64 `json:"malformed"`
	MismatchedRecords uint64 `json:"mismatched_records"`
}

// KeyStats is one key on this sidecar: its rates, the share it is admitted
// at now, the limit it is held to, and its shadow, nil without one.
type KeyStats struct {
	RequestsPerSec     float64 `json:"requests_per_sec"`
	DeniedPerSec       float64 `json:"denied_per_sec"`
	ShadowDeniedPerSec float64 `json:"shadow_denied_per_sec"`
	Share              float64 `json:"share"`
	Limit              Limit   `json:"limit"`
	Shadow             *Limit  `json:"shadow"`
}

// SidecarError is a call other than a check that the sidecar did not carry
// out. Status is the HTTP status it answered, 0 when it did not answer.
type SidecarError struct {
	Status  int
	Message string
	Err     error
}

func (e *SidecarError) Error() string { return "rateguard: " + e.Message }

func (e *SidecarError) Unwrap() error { return e.Err }

// Stats reads what the sidecar admitted and denied, and why.
func (c *Client) Stats(ctx context.Context) (Stats, error) {
	var s Stats
	_, err := c.call(ctx, http.MethodGet, "/v1/stats", nil, &s, false)
	return s, err
}

// KeyStats reads one key, checked in class ("" for the default); nil, with
// no error, while the sidecar keeps no state for the key, as it does not
// for a key that is not busy there.
func (c *Client) KeyStats(ctx context.Context, class, key string) (*KeyStats, error) {
	query := url.Values{"key": {key}}
	if class != "" {
		query.Set("class", class)
	}
	var s KeyStats
	found, err := c.call(ctx, http.MethodGet, "/v1/keys?"+query.Encode(), nil, &s, true)
	if err != nil || !found {
		return nil, err
	}
	return &s, nil
}

// SetClasses replaces every named class.
func (c *Client) SetClasses(ctx context.Context, classes map[string]Limit) error {
	if classes == nil {
		classes = map[string]Limit{}
	}
	_, err := c.call(ctx, http.MethodPut, "/v1/classes", classes, nil, false)
	return err
}

// SetLimit sets the default class's limit.
func (c *Client) SetLimit(ctx context.Context, limit Limit) error {
	_, err := c.call(ctx, http.MethodPut, "/v1/limit", limit, nil, false)
	return err
}

// SetKeyLimits replaces every key's exception.
func (c *Client) SetKeyLimits(ctx context.Context, keyLimits []KeyLimit) error {
	if keyLimits == nil {
		keyLimits = []KeyLimit{}
	}
	_, err := c.call(ctx, http.MethodPut, "/v1/key_limits", keyLimits, nil, false)
	return err
}

// SetShadows replaces every shadow limit, by class, "" for the default: a
// limit that denies nothing, and counts what it would deny.
func (c *Client) SetShadows(ctx context.Context, shadows map[string]Limit) error {
	if shadows == nil {
		shadows = map[string]Limit{}
	}
	_, err := c.call(ctx, http.MethodPut, "/v1/shadows", shadows, nil, false)
	return err
}

// SetDryRun admits everything while on, and counts what would be denied.
func (c *Client) SetDryRun(ctx context.Context, on bool) error {
	_, err := c.call(ctx, http.MethodPut, "/v1/dry_run", map[string]bool{"on": on}, nil, false)
	return err
}

// call sends body as JSON, if any, and reads the answer into out, if any.
// With missingOK, a 404 is found false rather than an error.
func (c *Client) call(ctx context.Context, method, path string, body, out any, missingOK bool) (found bool, err error) {
	var reader io.Reader
	if body != nil {
		payload, err := json.Marshal(body)
		if err != nil {
			return false, err
		}
		reader = bytes.NewReader(payload)
	}
	ctx, cancel := context.WithTimeout(ctx, c.adminTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, reader)
	if err != nil {
		return false, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	res, err := c.http.Do(req)
	if err != nil {
		return false, &SidecarError{Message: fmt.Sprintf("the sidecar did not answer: %v", err), Err: err}
	}
	defer res.Body.Close()
	data, err := io.ReadAll(res.Body)
	if err != nil {
		return false, &SidecarError{Message: fmt.Sprintf("the sidecar's answer broke off: %v", err), Err: err}
	}
	switch {
	case res.StatusCode == http.StatusNotFound && missingOK:
		return false, nil
	case res.StatusCode == http.StatusNotFound && method == http.MethodPut:
		return false, &SidecarError{Status: 404, Message: "the sidecar has no admin routes: start it with --admin"}
	case res.StatusCode != http.StatusOK && res.StatusCode != http.StatusNoContent:
		return false, &SidecarError{
			Status:  res.StatusCode,
			Message: fmt.Sprintf("the sidecar answered %d: %s", res.StatusCode, reason(data)),
		}
	}
	if out != nil {
		if err := json.Unmarshal(data, out); err != nil {
			return false, &SidecarError{Status: res.StatusCode, Message: "not JSON from the sidecar", Err: err}
		}
	}
	return true, nil
}

// reason is the sidecar's {"error": ...}, or the body as it is.
func reason(data []byte) string {
	var answer struct {
		Error *string `json:"error"`
	}
	if json.Unmarshal(data, &answer) == nil && answer.Error != nil {
		return *answer.Error
	}
	if len(data) == 0 {
		return "no reason given"
	}
	return string(data)
}
