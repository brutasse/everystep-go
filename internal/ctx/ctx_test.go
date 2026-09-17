package ctx

import (
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
)

func strp(s string) *string { return &s }

func newTestRun() *RunState {
	return NewRun(uuid.MustParse("550e8400-e29b-41d4-a716-446655440000"), "wf", nil, NewOutcomeStore(), nil)
}

func TestNextIDUnnamedConsumesPositions(t *testing.T) {
	rs := newTestRun()
	for _, want := range []string{"1", "2", "3"} {
		id, err := rs.NextID(nil)
		if err != nil {
			t.Fatal(err)
		}
		if id != want {
			t.Fatalf("got %q, want %q", id, want)
		}
	}
}

func TestNamedConsumesAPosition(t *testing.T) {
	rs := newTestRun()
	ids := make([]string, 0, 3)
	for _, name := range []*string{nil, strp("mid"), nil} {
		id, err := rs.NextID(name)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if got := strings.Join(ids, ","); got != "1,mid,3" {
		t.Fatalf("ids = %q", got)
	}
}

func TestValidateName(t *testing.T) {
	valid := []string{"one", "a-b", "a_b", "A1", "iter-0"}
	for _, name := range valid {
		if id, err := newTestRun().NextID(strp(name)); err != nil || id != name {
			t.Fatalf("%q: id=%q err=%v", name, id, err)
		}
	}
	invalid := []string{"", "a.b", "a b", "123", "1.5"}
	for _, name := range invalid {
		if _, err := newTestRun().NextID(strp(name)); err == nil {
			t.Fatalf("%q: want error", name)
		}
	}
	// The limit is on the full dotpath: an id of exactly MaxIDLength is
	// allowed, one over is not (empty prefix here).
	if _, err := newTestRun().NextID(strp(strings.Repeat("x", MaxIDLength))); err != nil {
		t.Fatalf("exactly %d chars: %v", MaxIDLength, err)
	}
	if _, err := newTestRun().NextID(strp(strings.Repeat("x", MaxIDLength+1))); err == nil {
		t.Fatalf("%d-char name: want error", MaxIDLength+1)
	}
}

func TestDuplicateNameInScopeFails(t *testing.T) {
	rs := newTestRun()
	if _, err := rs.NextID(strp("x")); err != nil {
		t.Fatal(err)
	}
	_, err := rs.NextID(strp("x"))
	if err == nil || !strings.Contains(err.Error(), "already used in this scope") {
		t.Fatalf("err = %v", err)
	}
	// A fork and a step with the same name in the same scope collide too.
	rs2 := newTestRun()
	if _, err := rs2.NextID(strp("fanout")); err != nil {
		t.Fatal(err)
	}
	if _, err := rs2.NextID(strp("fanout")); err == nil {
		t.Fatal("fork/step name collision: want error")
	}
}

func TestSameNameInDifferentBranchesIsFine(t *testing.T) {
	rs := newTestRun()
	if _, err := rs.NextID(strp("fan")); err != nil {
		t.Fatal(err)
	}
	b0, b1 := rs.Branch("fan", 0), rs.Branch("fan", 1)
	for _, b := range []*RunState{b0, b1} {
		id, err := b.NextID(strp("x"))
		if err != nil {
			t.Fatal(err)
		}
		if id != b.prefix+"x" {
			t.Fatalf("id = %q", id)
		}
	}
	if b0.prefix != "fan.0." || b1.prefix != "fan.1." {
		t.Fatalf("prefixes %q %q", b0.prefix, b1.prefix)
	}
}

func TestBranchCountersArePerScope(t *testing.T) {
	rs := newTestRun()
	// The fork consumes a position in the parent scope before its branches
	// are built (as parallel.Run does).
	forkID, err := rs.NextID(nil)
	if err != nil {
		t.Fatal(err)
	}
	b := rs.Branch(forkID, 0)
	if id, _ := b.NextID(nil); id != "1.0.1" {
		t.Fatalf("id = %q", id)
	}
	// The parent scope is unaffected by branch counters.
	if id, _ := rs.NextID(nil); id != "2" {
		t.Fatalf("parent id = %q", id)
	}
}

func TestOutcomeStoreConcurrentAccess(t *testing.T) {
	s := NewOutcomeStore()
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				id := string(rune('a'+i)) + string(rune('0'+j%10))
				s.Set(id, Outcome{Name: "n", Status: "done"})
				s.Get(id)
			}
		}(i)
	}
	wg.Wait()
}
