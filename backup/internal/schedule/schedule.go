// Package schedule runs a job on a fixed interval or at a time of day, one
// run at a time, by the wall clock.
//
// The loop never sleeps longer than MaxWait before it reads the wall clock
// again, so a run that fell due while the machine slept starts within
// MaxWait of waking. Due times that pass while a run is still going, or
// while the machine sleeps, are not made up: the loop runs once and then
// waits for the next due time after that run.
package schedule

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"
	"time"
)

// MinInterval is the shortest interval Every accepts.
const MinInterval = time.Minute

// MaxWait is the longest the loop sleeps before it reads the wall clock
// again.
const MaxWait = time.Minute

var (
	// ErrInterval is returned for an interval shorter than MinInterval.
	ErrInterval = errors.New("schedule: the interval must be at least 1m")
	// ErrTimeOfDay is returned for a daily time that is not HH:MM.
	ErrTimeOfDay = errors.New("schedule: the daily time must be HH:MM, from 00:00 to 23:59")
	// ErrJitter is returned for a jitter that is negative or not shorter
	// than the schedule's period.
	ErrJitter = errors.New("schedule: the jitter must be at least 0 and shorter than the period")
)

// Schedule is when a job is due: every fixed interval, or daily at a local
// time of day. The zero Schedule is not valid; use Every or Daily.
type Schedule struct {
	every  time.Duration
	daily  bool
	hour   int
	minute int
	jitter time.Duration
}

// Every returns a schedule due when the loop starts and then every d.
func Every(d time.Duration) (Schedule, error) {
	if d < MinInterval {
		return Schedule{}, ErrInterval
	}
	return Schedule{every: d}, nil
}

// Daily returns a schedule due each day at hhmm ("02:30"), local time.
func Daily(hhmm string) (Schedule, error) {
	h, m, ok := strings.Cut(hhmm, ":")
	if !ok || len(h) != 2 || len(m) != 2 {
		return Schedule{}, ErrTimeOfDay
	}
	hour, err1 := strconv.Atoi(h)
	minute, err2 := strconv.Atoi(m)
	if err1 != nil || err2 != nil || hour < 0 || hour > 23 || minute < 0 || minute > 59 {
		return Schedule{}, ErrTimeOfDay
	}
	return Schedule{daily: true, hour: hour, minute: minute}, nil
}

// WithJitter returns s with each run delayed by a random duration from 0 up
// to j, so many agents on one schedule do not start together.
func (s Schedule) WithJitter(j time.Duration) (Schedule, error) {
	if j < 0 || j >= s.period() {
		return Schedule{}, ErrJitter
	}
	s.jitter = j
	return s, nil
}

// Jitter is the most a run is delayed past its due time.
func (s Schedule) Jitter() time.Duration { return s.jitter }

func (s Schedule) period() time.Duration {
	if s.daily {
		return 24 * time.Hour
	}
	return s.every
}

// String describes the schedule, such as "every 6h0m0s" or "daily at 02:30".
func (s Schedule) String() string {
	if s.daily {
		return fmt.Sprintf("daily at %02d:%02d", s.hour, s.minute)
	}
	return "every " + s.every.String()
}

// First is the first due time of a loop started at now.
func (s Schedule) First(now time.Time) time.Time {
	if s.daily {
		return s.Next(now, now)
	}
	return now
}

// Next is the first due time after now, given that the last run was due at
// prev. An interval schedule keeps its due times on prev's grid; a daily
// schedule's due times are the time of day in now's location.
func (s Schedule) Next(prev, now time.Time) time.Time {
	if s.daily {
		y, mo, d := now.Date()
		t := time.Date(y, mo, d, s.hour, s.minute, 0, 0, now.Location())
		for !t.After(now) {
			d++
			t = time.Date(y, mo, d, s.hour, s.minute, 0, 0, now.Location())
		}
		return t
	}
	if !now.Before(prev) {
		return prev.Add((now.Sub(prev)/s.every + 1) * s.every)
	}
	return prev.Add(s.every)
}

// Clock is the time source of a loop.
type Clock interface {
	// Now is the current wall-clock time.
	Now() time.Time
	// After delivers on the returned channel once d has passed.
	After(d time.Duration) <-chan time.Time
}

// System is the system clock. Its Now carries no monotonic reading, so
// comparisons follow the wall clock across a sleep.
type System struct{}

// Now returns the wall-clock time.
func (System) Now() time.Time { return time.Now().Round(0) }

// After is time.After.
func (System) After(d time.Duration) <-chan time.Time { return time.After(d) }

// Loop runs a job on a schedule.
type Loop struct {
	// Schedule is when the job is due.
	Schedule Schedule
	// Clock is the time source. Nil means System.
	Clock Clock
	// Rand returns a random duration from 0 up to but not including n, for
	// the jitter. Nil means math/rand/v2's N.
	Rand func(n time.Duration) time.Duration
	// Due, if set, is called with each run's start time before the loop
	// waits for it.
	Due func(at time.Time)
}

// Run calls job each time it falls due until ctx is done, and then returns
// ctx's error. A job runs to its end before the next is scheduled, so runs
// never overlap; a job that should stop when ctx is done watches ctx.
func (l *Loop) Run(ctx context.Context, job func(ctx context.Context)) error {
	clock := l.Clock
	if clock == nil {
		clock = System{}
	}
	random := l.Rand
	if random == nil {
		random = rand.N[time.Duration]
	}
	due := l.Schedule.First(clock.Now())
	for {
		at := due
		if j := l.Schedule.jitter; j > 0 {
			at = at.Add(random(j))
		}
		if l.Due != nil {
			l.Due(at)
		}
		for {
			now := clock.Now()
			if !now.Before(at) {
				break
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-clock.After(min(at.Sub(now), MaxWait)):
			}
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		job(ctx)
		if err := ctx.Err(); err != nil {
			return err
		}
		due = l.Schedule.Next(due, clock.Now())
	}
}
