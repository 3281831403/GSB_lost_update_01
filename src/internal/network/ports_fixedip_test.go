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
)

// fakePortServer is a stateful in-memory Neutron port endpoint. When
// enforceRevision is set it honours the If-Match: revision_number=N
// precondition like real Neutron (standard-attr-revisions) and answers
// 412 Precondition Failed on a stale revision.
type fakePortServer struct {
	mu              sync.Mutex
	fixedIPs        []FixedIP
	revision        int
	enforceRevision bool
	alwaysConflict  bool
	conflictOnce    bool
	sawIfMatch      bool
}

func (f *fakePortServer) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/ports/port-1", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			f.mu.Lock()
			defer f.mu.Unlock()
			f.writePort(w)
		case http.MethodPut:
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.enforceRevision {
				if strings.HasPrefix(r.Header.Get("If-Match"), "revision_number=") {
					f.sawIfMatch = true
				}
				var want int
				if _, err := fmt.Sscanf(r.Header.Get("If-Match"), "revision_number=%d", &want); err != nil || want != f.revision || f.alwaysConflict {
					w.WriteHeader(http.StatusPreconditionFailed)
					return
				}
			}
			if f.conflictOnce {
				f.conflictOnce = false
				w.WriteHeader(http.StatusPreconditionFailed)
				return
			}
			var body struct {
				Port struct {
					FixedIPs []struct {
						SubnetID  string `json:"subnet_id"`
						IPAddress string `json:"ip_address"`
					} `json:"fixed_ips"`
				} `json:"port"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			f.fixedIPs = f.fixedIPs[:0]
			for _, ip := range body.Port.FixedIPs {
				f.fixedIPs = append(f.fixedIPs, FixedIP{SubnetID: ip.SubnetID, IPAddress: ip.IPAddress})
			}
			f.revision++
			f.writePort(w)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
	return mux
}

func (f *fakePortServer) writePort(w http.ResponseWriter) {
	ips := make([]map[string]string, 0, len(f.fixedIPs))
	for _, ip := range f.fixedIPs {
		ips = append(ips, map[string]string{"subnet_id": ip.SubnetID, "ip_address": ip.IPAddress})
	}
	json.NewEncoder(w).Encode(map[string]any{
		"port": map[string]any{
			"id":              "port-1",
			"network_id":      "net-1",
			"fixed_ips":       ips,
			"revision_number": f.revision,
		},
	})
}

func (f *fakePortServer) subnets() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, ip := range f.fixedIPs {
		out = append(out, ip.SubnetID)
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

func TestAddFixedIPConcurrentNoLostUpdate(t *testing.T) {
	srv := &fakePortServer{
		fixedIPs:        []FixedIP{{SubnetID: "sub-0", IPAddress: "10.0.0.10"}},
		revision:        1,
		enforceRevision: true,
		conflictOnce:    true, // deterministically force at least one retry
	}
	client := fakeNeutronClient(srv.handler())
	ctx := context.Background()

	const writers = 4
	var wg sync.WaitGroup
	errs := make([]error, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = AddFixedIPToPort(ctx, client, "port-1", fmt.Sprintf("sub-%d", i+1), fmt.Sprintf("10.0.0.%d", i+11))
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("concurrent add %d failed: %v", i, err)
		}
	}
	got := srv.subnets()
	if len(got) != writers+1 {
		t.Fatalf("expected %d fixed IPs after concurrent adds, got %d (%v)", writers+1, len(got), got)
	}
	for i := 0; i <= writers; i++ {
		if !hasSubnet(got, fmt.Sprintf("sub-%d", i)) {
			t.Errorf("lost update: subnet sub-%d missing from %v", i, got)
		}
	}
	if !srv.sawIfMatch {
		t.Error("expected updates to carry an If-Match revision precondition")
	}
}

func TestRemoveFixedIPConcurrentNoLostUpdate(t *testing.T) {
	srv := &fakePortServer{
		fixedIPs: []FixedIP{
			{SubnetID: "sub-a", IPAddress: "10.0.0.1"},
			{SubnetID: "sub-b", IPAddress: "10.0.0.2"},
			{SubnetID: "sub-c", IPAddress: "10.0.0.3"},
		},
		revision:        7,
		enforceRevision: true,
		conflictOnce:    true,
	}
	client := fakeNeutronClient(srv.handler())
	ctx := context.Background()

	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i, sub := range []string{"sub-a", "sub-b"} {
		wg.Add(1)
		go func(i int, sub string) {
			defer wg.Done()
			errs[i] = RemoveFixedIPFromPort(ctx, client, "port-1", sub)
		}(i, sub)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("concurrent remove %d failed: %v", i, err)
		}
	}
	got := srv.subnets()
	if len(got) != 1 || got[0] != "sub-c" {
		t.Fatalf("expected only sub-c to remain, got %v", got)
	}
}

func TestFixedIPConflictReported(t *testing.T) {
	srv := &fakePortServer{
		fixedIPs:        []FixedIP{{SubnetID: "sub-0", IPAddress: "10.0.0.10"}},
		revision:        1,
		enforceRevision: true,
		alwaysConflict:  true,
	}
	client := fakeNeutronClient(srv.handler())
	ctx := context.Background()

	err := AddFixedIPToPort(ctx, client, "port-1", "sub-1", "10.0.0.11")
	if err == nil {
		t.Fatal("expected a conflict error, got nil")
	}
	if !errors.Is(err, ErrFixedIPConflict) {
		t.Errorf("expected ErrFixedIPConflict, got %v", err)
	}

	err = RemoveFixedIPFromPort(ctx, client, "port-1", "sub-0")
	if !errors.Is(err, ErrFixedIPConflict) {
		t.Errorf("expected ErrFixedIPConflict from remove, got %v", err)
	}
	// The conflicting writes must not have been applied.
	if got := srv.subnets(); len(got) != 1 || got[0] != "sub-0" {
		t.Errorf("server state changed despite conflicts: %v", got)
	}
}

func TestFixedIPSerialWithoutRevisionSupport(t *testing.T) {
	// Server without standard-attr-revisions ignores If-Match; serial
	// add/remove must behave exactly as before.
	srv := &fakePortServer{
		fixedIPs: []FixedIP{{SubnetID: "sub-0", IPAddress: "10.0.0.10"}},
		revision: 1,
	}
	client := fakeNeutronClient(srv.handler())
	ctx := context.Background()

	if err := AddFixedIPToPort(ctx, client, "port-1", "sub-1", "10.0.0.11"); err != nil {
		t.Fatalf("serial add failed: %v", err)
	}
	if got := srv.subnets(); !hasSubnet(got, "sub-0") || !hasSubnet(got, "sub-1") {
		t.Fatalf("unexpected subnets after add: %v", got)
	}
	if err := RemoveFixedIPFromPort(ctx, client, "port-1", "sub-0"); err != nil {
		t.Fatalf("serial remove failed: %v", err)
	}
	if got := srv.subnets(); len(got) != 1 || got[0] != "sub-1" {
		t.Fatalf("unexpected subnets after remove: %v", got)
	}
}
