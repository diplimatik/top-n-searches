package main

import (
	"testing"
	"time"
)

func TestTrendEngine_StopWords(t *testing.T) {
	e := NewTrendEngine()
	e.AddStopWord("slur")
	e.AddEvent("slur", "user1", time.Now().Unix())
	e.AddEvent("Good word", "user2", time.Now().Unix())
	e.AddEvent("\t\nSL u    R\n", "user3", time.Now().Unix())

	time.Sleep(1300 * time.Millisecond) // waition for slidingWindow goroutine to update the cache

	top := e.Top(10)
	if len(top) != 1 {
		t.Fatalf("Expected 1 top word, got %d", len(top))
	}
	if top[0].Query != "goodword" {
		t.Errorf("Expected top query to be 'goodword', got %s", top[0].Query)
	}

	e.RemoveStopWord("slur")
	if len(e.stopWords) != 0 {
		t.Fatalf("Expected 0 stop words in dictionary, got %d", len(e.stopWords))
	}

}

func TestTrendEngine_AntiFraud(t *testing.T) {
	e := NewTrendEngine()
	e.AddEvent("iphone", "spammer1", time.Now().Unix())
	e.AddEvent("iphone", "spammer1", time.Now().Unix())
	e.AddEvent("iphone", "spammer1", time.Now().Unix())

	e.AddEvent("iphone", "user1", time.Now().Unix())
	time.Sleep(1300 * time.Millisecond)

	top := e.Top(10)
	if len(top) == 0 {
		t.Fatal("Expected top words, got none")
	}

	if top[0].Count != 2 {
		t.Fatalf("Expected count to be 2, got %d", top[0].Count)
	}
}

func TestTrendEngine_TopSortLimit(t *testing.T) {
	e := NewTrendEngine()

	e.AddEvent("iPhone", "user1", time.Now().Unix())
	e.AddEvent("iPhone", "user2", time.Now().Unix())
	e.AddEvent("iPhone", "user3", time.Now().Unix())

	e.AddEvent("panties", "user4", time.Now().Unix())
	e.AddEvent("panties", "user5", time.Now().Unix())

	e.AddEvent("toy", "user6", time.Now().Unix())

	time.Sleep(1300 * time.Millisecond)

	top := e.Top(2)

	if len(top) != 2 {
		t.Fatalf("Expected 2 limited top words, got %d", len(top))
	}

	if top[0].Query != "iphone" || top[1].Query != "panties" {
		t.Error("Wrong sorting order: ", top)
	}
}
