package network

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/larkly/lazystack/internal/testutil"
)

// fixedIPStore is an in-memory fake Neutron port that enforces the
// If-Match revision precondition on updates, like real Neutron does.
type fixedIPStore struct {
	mu        sync.Mutex
	revision  int
	fixedIPs  []map[string]string
	conflicts int
	gets      int
	puts      int
	// barrier, when non-nil, blocks the first total GETs until all have
	// arrived, forcing concurrent readers to observe the same revision.
	barrier *getBarrier
}

type getBarrier struct {
	mu    sync.Mutex
	count int
	total int
	done  chan struct{}
}

func newGetBarrier(total int) *getBarrier {
	return &getBarrier{total: total, done: make(chan struct{})}
}

func (b *getBarrier) wait() {
	b.mu.Lock()
	b.count++
	if b.count == b.total {
		close(b.done)
	}
	ch := b.done
	b.mu.Unlock()
	<-ch
}

func (s *fixedIPStore) handler(t *testing.T) http.Handler {
	t.Helper()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/ports/") {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			if s.barrier != nil {
				s.barrier.wait()
			}
			s.mu.Lock()
			s.gets++
			body := s.portJSONLocked()
			s.mu.Unlock()
			w.WriteHeader(http.StatusOK)
			w.Write(body)
		case http.MethodPut:
			var req struct {
				Port struct {
					FixedIPs []map[string]string `json:"fixed_ips"`
				} `json:"port"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Errorf("decoding PUT body: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			s.mu.Lock()
			defer s.mu.Unlock()
			s.puts++
			want := strings.TrimPrefix(r.Header.Get("If-Match"), "revision_number=")
			if want != fmt.Sprintf("%d", s.revision) {
				s.conflicts++
				w.WriteHeader(http.StatusPreconditionFailed)
				w.Write([]byte(`{"NeutronError":{"message":"Revision number mismatch","type":"RevisionNumberConstraintFailed","detail":""}}`))
				return
			}
			s.fixedIPs = req.Port.FixedIPs
			s.revision++
			w.WriteHeader(http.StatusOK)
			w.Write(s.portJSONLocked())
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
}

func (s *fixedIPStore) portJSONLocked() []byte {
	ips := make([]map[string]string, len(s.fixedIPs))
	copy(ips, s.fixedIPs)
	body, _ := json.Marshal(map[string]any{
		"port": map[string]any{
			"id":              "p1",
			"revision_number": s.revision,
			"fixed_ips":       ips,
		},
	})
	return body
}

func (s *fixedIPStore) subnets() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for _, ip := range s.fixedIPs {
		out = append(out, ip["subnet_id"])
	}
	return out
}

func hasSubnet(subnets []string, want string) bool {
	for _, s := range subnets {
		if s == want {
			return true
		}
	}
	return false
}

func runConcurrent(t *testing.T, fns ...func() error) {
	t.Helper()
	var wg sync.WaitGroup
	errs := make([]error, len(fns))
	for i, fn := range fns {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = fn()
		}()
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("concurrent fixed IP update timed out")
	}
	for i, err := range errs {
		if err != nil {
			t.Fatalf("goroutine %d: %v", i, err)
		}
	}
}

// Two concurrent adds on the same port must both survive: the conflict is
// detected via If-Match and retried, not silently overwritten.
func TestAddFixedIPToPortConcurrentNoLostUpdate(t *testing.T) {
	store := &fixedIPStore{
		revision: 1,
		fixedIPs: []map[string]string{{"subnet_id": "s0", "ip_address": "10.0.0.1"}},
		barrier:  newGetBarrier(2),
	}
	client, cleanup := testutil.FakeServiceClient(store.handler(t))
	defer cleanup()
	ctx := context.Background()

	runConcurrent(t,
		func() error { return AddFixedIPToPort(ctx, client, "p1", "s1", "10.0.1.1") },
		func() error { return AddFixedIPToPort(ctx, client, "p1", "s2", "10.0.2.1") },
	)

	subnets := store.subnets()
	for _, want := range []string{"s0", "s1", "s2"} {
		if !hasSubnet(subnets, want) {
			t.Fatalf("lost update: subnet %s missing from %v", want, subnets)
		}
	}
	if store.conflicts == 0 {
		t.Fatal("expected at least one detected conflict (412), got none")
	}
}

// A concurrent add and remove on the same port must both take effect.
func TestFixedIPConcurrentAddAndRemove(t *testing.T) {
	store := &fixedIPStore{
		revision: 1,
		fixedIPs: []map[string]string{
			{"subnet_id": "keep", "ip_address": "10.0.0.1"},
			{"subnet_id": "drop", "ip_address": "10.0.0.2"},
		},
		barrier: newGetBarrier(2),
	}
	client, cleanup := testutil.FakeServiceClient(store.handler(t))
	defer cleanup()
	ctx := context.Background()

	runConcurrent(t,
		func() error { return AddFixedIPToPort(ctx, client, "p1", "added", "10.0.3.1") },
		func() error { return RemoveFixedIPFromPort(ctx, client, "p1", "drop") },
	)

	subnets := store.subnets()
	if !hasSubnet(subnets, "keep") || !hasSubnet(subnets, "added") {
		t.Fatalf("expected keep+added in %v", subnets)
	}
	if hasSubnet(subnets, "drop") {
		t.Fatalf("expected drop removed from %v", subnets)
	}
	if store.conflicts == 0 {
		t.Fatal("expected at least one detected conflict (412), got none")
	}
}

// Serial, non-conflicting updates keep working with a single GET+PUT each.
func TestFixedIPSerialUpdatesUnchanged(t *testing.T) {
	store := &fixedIPStore{
		revision: 1,
		fixedIPs: []map[string]string{{"subnet_id": "s0", "ip_address": "10.0.0.1"}},
	}
	client, cleanup := testutil.FakeServiceClient(store.handler(t))
	defer cleanup()
	ctx := context.Background()

	if err := AddFixedIPToPort(ctx, client, "p1", "s1", "10.0.1.1"); err != nil {
		t.Fatalf("add: %v", err)
	}
	if err := RemoveFixedIPFromPort(ctx, client, "p1", "s0"); err != nil {
		t.Fatalf("remove: %v", err)
	}

	subnets := store.subnets()
	if len(subnets) != 1 || subnets[0] != "s1" {
		t.Fatalf("unexpected final subnets: %v", subnets)
	}
	if store.conflicts != 0 {
		t.Fatalf("serial updates should not conflict, got %d", store.conflicts)
	}
	if store.gets != 2 || store.puts != 2 {
		t.Fatalf("expected 2 GETs and 2 PUTs, got %d/%d", store.gets, store.puts)
	}
}

// A conflict that never resolves is reported explicitly, not silently dropped.
func TestAddFixedIPToPortPersistentConflict(t *testing.T) {
	client, cleanup := testutil.FakeServiceClient(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodGet {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"port":{"id":"p1","revision_number":1,"fixed_ips":[{"subnet_id":"s0","ip_address":"10.0.0.1"}]}}`))
			return
		}
		w.WriteHeader(http.StatusPreconditionFailed)
		w.Write([]byte(`{"NeutronError":{"message":"Revision number mismatch","type":"RevisionNumberConstraintFailed","detail":""}}`))
	}))
	defer cleanup()

	err := AddFixedIPToPort(context.Background(), client, "p1", "s1", "10.0.1.1")
	if err == nil {
		t.Fatal("expected conflict error, got nil")
	}
	if !errors.Is(err, ErrFixedIPConflict) {
		t.Fatalf("expected ErrFixedIPConflict, got %v", err)
	}
}
