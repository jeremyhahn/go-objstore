package schedule

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// fakeClock is a clock that moves only when the loop sleeps on it or a test
// advances it. Each After advances it by the wait at once, plus any sleep
// queued with sleep, the time a machine spends suspended.
type fakeClock struct {
	mu     sync.Mutex
	now    time.Time
	waits  []time.Duration
	asleep time.Duration
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.waits = append(c.waits, d)
	c.now = c.now.Add(d + c.asleep)
	c.asleep = 0
	ch := make(chan time.Time, 1)
	ch <- c.now
	return ch
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func (c *fakeClock) sleep(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.asleep = d
}

var start = time.Date(2026, 3, 10, 9, 15, 0, 0, time.UTC)

// runFor runs l until job has run n times and returns each run's start.
func runFor(t *testing.T, l *Loop, c *fakeClock, n int, during func(i int)) []time.Time {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var starts []time.Time
	err := l.Run(ctx, func(context.Context) {
		starts = append(starts, c.Now())
		if during != nil {
			during(len(starts))
		}
		if len(starts) == n {
			cancel()
		}
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	return starts
}

func mustEqual(t *testing.T, got []time.Time, want ...time.Time) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("runs = %v, want %v", got, want)
	}
	for i := range got {
		if !got[i].Equal(want[i]) {
			t.Fatalf("run %d at %v, want %v", i, got[i], want[i])
		}
	}
}

func TestParse(t *testing.T) {
	if _, err := Every(59 * time.Second); !errors.Is(err, ErrInterval) {
		t.Errorf("Every(59s) = %v, want ErrInterval", err)
	}
	for _, bad := range []string{"", "2:30", "02-30", "24:00", "23:60", "ab:cd", "-1:00", "02:3x"} {
		if _, err := Daily(bad); !errors.Is(err, ErrTimeOfDay) {
			t.Errorf("Daily(%q) = %v, want ErrTimeOfDay", bad, err)
		}
	}
	s, err := Daily("23:59")
	if err != nil || s.String() != "daily at 23:59" {
		t.Fatalf("Daily(23:59) = %v, %v", s, err)
	}
	if _, err := s.WithJitter(24 * time.Hour); !errors.Is(err, ErrJitter) {
		t.Errorf("daily jitter of 24h = %v, want ErrJitter", err)
	}
	e, err := Every(time.Hour)
	if err != nil || e.String() != "every 1h0m0s" {
		t.Fatalf("Every(1h) = %v, %v", e, err)
	}
	for _, j := range []time.Duration{-time.Second, time.Hour} {
		if _, err := e.WithJitter(j); !errors.Is(err, ErrJitter) {
			t.Errorf("jitter %v = %v, want ErrJitter", j, err)
		}
	}
	if e, err = e.WithJitter(time.Minute); err != nil || e.Jitter() != time.Minute {
		t.Fatalf("WithJitter(1m) = %v, %v", e.Jitter(), err)
	}
}

func TestNext(t *testing.T) {
	e, _ := Every(time.Hour)
	if got := e.First(start); !got.Equal(start) {
		t.Errorf("interval First = %v, want the start", got)
	}
	for _, tc := range []struct{ now, want time.Duration }{
		{30 * time.Minute, time.Hour},
		{time.Hour, 2 * time.Hour},
		{150 * time.Minute, 3 * time.Hour},
		{-time.Minute, time.Hour},
	} {
		if got := e.Next(start, start.Add(tc.now)); !got.Equal(start.Add(tc.want)) {
			t.Errorf("Next at +%v = %v, want +%v", tc.now, got, tc.want)
		}
	}
	d, _ := Daily("02:30")
	at := func(day, h, m int) time.Time { return time.Date(2026, 3, day, h, m, 0, 0, time.UTC) }
	if got := d.First(at(10, 1, 0)); !got.Equal(at(10, 2, 30)) {
		t.Errorf("daily First before the time = %v", got)
	}
	if got := d.First(at(10, 2, 30)); !got.Equal(at(11, 2, 30)) {
		t.Errorf("daily First at the time = %v", got)
	}
	if got := d.Next(at(10, 2, 30), at(31, 23, 0)); !got.Equal(time.Date(2026, 4, 1, 2, 30, 0, 0, time.UTC)) {
		t.Errorf("daily Next across a month = %v", got)
	}
}

func TestDailyAcrossDST(t *testing.T) {
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skip("no time zone database")
	}
	d, _ := Daily("12:00")
	// The clocks go forward on 2026-03-08; a day then lasts 23 hours.
	now := time.Date(2026, 3, 7, 13, 0, 0, 0, loc)
	if got := d.Next(now, now); !got.Equal(time.Date(2026, 3, 8, 12, 0, 0, 0, loc)) || got.Sub(now) != 22*time.Hour {
		t.Errorf("Next across DST = %v (%v later)", got, got.Sub(now))
	}
}

func TestIntervalFires(t *testing.T) {
	s, _ := Every(time.Hour)
	c := &fakeClock{now: start}
	starts := runFor(t, &Loop{Schedule: s, Clock: c}, c, 3, func(int) { c.advance(10 * time.Minute) })
	mustEqual(t, starts, start, start.Add(time.Hour), start.Add(2*time.Hour))
	for _, w := range c.waits {
		if w > MaxWait {
			t.Fatalf("waited %v at once, more than MaxWait", w)
		}
	}
}

func TestDailyFires(t *testing.T) {
	s, _ := Daily("02:30")
	c := &fakeClock{now: start}
	starts := runFor(t, &Loop{Schedule: s, Clock: c}, c, 2, nil)
	first := time.Date(2026, 3, 11, 2, 30, 0, 0, time.UTC)
	mustEqual(t, starts, first, first.AddDate(0, 0, 1))
}

func TestNoOverlap(t *testing.T) {
	s, _ := Every(time.Hour)
	c := &fakeClock{now: start}
	running := false
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var starts []time.Time
	_ = (&Loop{Schedule: s, Clock: c}).Run(ctx, func(context.Context) {
		if running {
			t.Fatal("a run started while one was going")
		}
		running = true
		starts = append(starts, c.Now())
		if len(starts) == 1 {
			c.advance(150 * time.Minute) // outlasts two due times
		}
		running = false
		if len(starts) == 2 {
			cancel()
		}
	})
	// The due times passed during the first run are skipped, not queued.
	mustEqual(t, starts, start, start.Add(3*time.Hour))
}

func TestMissedRunFiresOnce(t *testing.T) {
	s, _ := Every(time.Hour)
	c := &fakeClock{now: start}
	starts := runFor(t, &Loop{Schedule: s, Clock: c}, c, 3, func(i int) {
		if i == 1 {
			c.sleep(5*time.Hour + 20*time.Minute) // the machine sleeps through five due times
		}
	})
	woke := start.Add(5*time.Hour + 20*time.Minute + MaxWait)
	mustEqual(t, starts, start, woke, start.Add(6*time.Hour))
}

func TestJitter(t *testing.T) {
	s, _ := Every(time.Hour)
	s, _ = s.WithJitter(10 * time.Minute)
	c := &fakeClock{now: start}
	var dues []time.Time
	l := &Loop{
		Schedule: s, Clock: c,
		Rand: func(n time.Duration) time.Duration { return n / 2 },
		Due:  func(at time.Time) { dues = append(dues, at) },
	}
	starts := runFor(t, l, c, 2, nil)
	mustEqual(t, starts, start.Add(5*time.Minute), start.Add(65*time.Minute))
	mustEqual(t, dues, starts...)

	// The default random source stays below the jitter.
	c = &fakeClock{now: start}
	starts = runFor(t, &Loop{Schedule: s, Clock: c}, c, 1, nil)
	if d := starts[0].Sub(start); d < 0 || d >= 10*time.Minute+MaxWait {
		t.Fatalf("jittered start %v after the due time", d)
	}
}

func TestCancel(t *testing.T) {
	s, _ := Daily("02:30")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ran := false
	if err := (&Loop{Schedule: s}).Run(ctx, func(context.Context) { ran = true }); !errors.Is(err, context.Canceled) || ran {
		t.Fatalf("Run on a canceled context = %v, ran %v", err, ran)
	}
	// Due at once: the loop still checks ctx before it starts the job.
	e, _ := Every(time.Hour)
	if err := (&Loop{Schedule: e}).Run(ctx, func(context.Context) { ran = true }); !errors.Is(err, context.Canceled) || ran {
		t.Fatalf("Run due at once on a canceled context = %v, ran %v", err, ran)
	}

	// A wait on the system clock ends when ctx is canceled.
	ctx, cancel = context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := (&Loop{Schedule: s}).Run(ctx, func(context.Context) { ran = true }); !errors.Is(err, context.DeadlineExceeded) || ran {
		t.Fatalf("Run = %v, ran %v", err, ran)
	}
	if now := (System{}).Now(); now.Round(0) != now {
		t.Error("System.Now carries a monotonic reading")
	}
}
