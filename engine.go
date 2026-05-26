package main

import (
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	secondSpan      = 300 //time in seconds of top searches feed refresh
	userReqCooldown = 60  //cooldown between users search counts (antifraud policy)
)

type TopSearch struct {
	Query string `json:"query"`
	Count int    `json:"count"`
}

type TrendEngine struct {
	mu         sync.Mutex
	buckets    [secondSpan]map[string]int //1 bucket per 5 mins span of searches
	totals     map[string]int
	currentSec int

	stopWords    map[string]struct{}
	stopWordsRwm sync.RWMutex

	userLocks   map[string]int64
	userLocksMu sync.Mutex

	cachedTop atomic.Pointer[[]TopSearch]
}

func NewTrendEngine() *TrendEngine {
	e := &TrendEngine{
		totals:    make(map[string]int),
		stopWords: make(map[string]struct{}),
		userLocks: make(map[string]int64),
	}
	for i := range e.buckets {
		e.buckets[i] = make(map[string]int)
	}
	e.cachedTop.Store(new(make([]TopSearch, 0)))

	go e.slidingWindow()
	go e.userLockCleaner()

	return e
}

func (e *TrendEngine) AddEvent(query, userID string, timestamp int64) {
	query = strings.ToLower(strings.ReplaceAll(strings.TrimSpace(query), " ", ""))

	//stop word filtering
	e.stopWordsRwm.RLock()
	if _, ok := e.stopWords[query]; ok {
		e.stopWordsRwm.RUnlock()
		return
	}
	e.stopWordsRwm.RUnlock()

	//antifraud filtering
	lockKey := userID + ":" + query
	e.userLocksMu.Lock()
	if _, ok := e.userLocks[lockKey]; ok {
		e.userLocksMu.Unlock()
		return
	}
	e.userLocks[lockKey] = timestamp + userReqCooldown
	e.userLocksMu.Unlock()

	e.mu.Lock()
	e.buckets[e.currentSec][query]++
	e.totals[query]++
	e.mu.Unlock()

}

func (e *TrendEngine) slidingWindow() {
	ticker := time.NewTicker(1 * time.Second)
	for range ticker.C {
		e.mu.Lock()
		nextSec := (e.currentSec + 1) % secondSpan

		//removing old data from past 5 mins
		for q, count := range e.buckets[nextSec] {
			e.totals[q] -= count
			if e.totals[q] == 0 {
				delete(e.totals, q)
			}
		}

		e.buckets[nextSec] = make(map[string]int)
		e.currentSec = nextSec

		topList := make([]TopSearch, 0, len(e.totals))
		for q, count := range e.totals {
			topList = append(topList, TopSearch{q, count})
		}
		e.mu.Unlock()

		//descending order sorting
		slices.SortFunc(topList, func(a, b TopSearch) int {
			return b.Count - a.Count
		})
		e.cachedTop.Store(&topList)
	}
}

func (e *TrendEngine) userLockCleaner() {
	ticker := time.NewTicker(userReqCooldown * time.Second)
	for range ticker.C {
		now := time.Now().Unix()
		e.userLocksMu.Lock()
		for k, exp := range e.userLocks {
			if now > exp {
				delete(e.userLocks, k)
			}
		}
		e.userLocksMu.Unlock()
	}
}

func (e *TrendEngine) AddStopWord(word string) {
	e.stopWordsRwm.Lock()
	e.stopWords[strings.ToLower(word)] = struct{}{}
	e.stopWordsRwm.Unlock()

	//delete existing stop word records
	e.mu.Lock()
	if _, ok := e.totals[word]; ok {
		delete(e.totals, word)
	}
	for i := 0; i < e.currentSec; i++ {
		if _, ok := e.buckets[i][word]; ok {
			delete(e.buckets[i], word)
		}
	}
	e.mu.Unlock()
}

func (e *TrendEngine) RemoveStopWord(word string) {
	e.stopWordsRwm.Lock()
	delete(e.stopWords, strings.ToLower(word))
	e.stopWordsRwm.Unlock()
}

func (e *TrendEngine) Top(limit int) []TopSearch {
	top := *e.cachedTop.Load()
	if limit > len(top) {
		return top
	}
	return top[:limit]
}
