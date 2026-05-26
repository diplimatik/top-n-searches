package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/segmentio/kafka-go"
)

type SearchEvent struct {
	Query     string `json:"query"`
	UserID    string `json:"user_id"`
	Timestamp int64  `json:"timestamp"`
}

var engine = NewTrendEngine()

func main() {
	kafkaURL := os.Getenv("KAFKA_URL")
	if kafkaURL == "" {
		kafkaURL = "localhost:9092"
	}

	go startKafkaConsumer(kafkaURL, "search-logs", "trends-group")

	http.HandleFunc("/api/trends", trendsHandler)     //GET
	http.HandleFunc("/api/stoplist", stopListHandler) //POST DELETE
	http.Handle("/metrics", promhttp.Handler())

	port := ":8080"
	log.Println("Server running on port:", port)
	if err := http.ListenAndServe(port, nil); err != nil {
		log.Fatal("Error starting the serer:", err)
	}
}

func startKafkaConsumer(brokers, topic, groupID string) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{brokers},
		GroupID:        groupID,
		Topic:          topic,
		CommitInterval: time.Second, // flushes commits to Kafka every second
	})

	defer func(r *kafka.Reader) {
		err := r.Close()
		if err != nil {
			log.Fatal("Failed to close reader:", err)
		}
	}(r)

	log.Println("Waiting for the Kafka message...")

	for {
		log.Println("Reading message...")
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			log.Println("error reading from Kafka:", err)
			time.Sleep(time.Second)
			continue
		}

		var event SearchEvent
		if err := json.Unmarshal(m.Value, &event); err != nil {
			log.Println("JSON parsing error:", err)
			continue
		}

		engine.AddEvent(event.Query, event.UserID, event.Timestamp)
	}
}

func trendsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	limitStr := r.URL.Query().Get("limit")
	//default query limit
	limit := 10
	if l, err := strconv.Atoi(limitStr); err == nil && l > 0 && l <= 100 {
		limit = l
	}

	top := engine.Top(limit)

	w.Header().Set("Content-Type", "application/json")
	err := json.NewEncoder(w).Encode(top)
	if err != nil {
		log.Println("JSON parsing error:", err)
	}
}

func stopListHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Word string `json:"word"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Word == "" {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodPost:
		engine.AddStopWord(req.Word)
		w.WriteHeader(http.StatusCreated)
		_, err := fmt.Fprintf(w, `{"status": "added"}`)
		if err != nil {
			return
		}
	case http.MethodDelete:
		engine.RemoveStopWord(req.Word)
		w.WriteHeader(http.StatusOK)
		_, err := fmt.Fprintf(w, `{"status": "removed"}`)
		if err != nil {
			return
		}
	default:
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
	}
}
