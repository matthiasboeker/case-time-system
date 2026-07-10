package main

import (
    "context"
    "encoding/json"
    "fmt"
    "log"
    "os"
    "os/signal"
    "syscall"
	"github.com/jackc/pgx/v5"
    "github.com/segmentio/kafka-go"
)

// Order mirrors exactly what the Python producer sends
type Event struct {
	EventID     string `json:"event_id"`
	EventType   string `json:"event_type"`
	UserID      string `json:"user_id"`
	TimeCreated string `json:"time_created"`
}

func main() {
	db, err := pgx.Connect(context.Background(), "postgres://user:password@localhost:5432/orders_db")
	if err != nil {
		fmt.Println("could not to postgres: ", err)
	}
	defer db.Close(context.Background())
	fmt.Println("Connected to Postgres!")
    
    // ── 1. Connect to Kafka ──────────────────────────────────────
    reader := kafka.NewReader(kafka.ReaderConfig{
        Brokers:  []string{"localhost:9092"},
        Topic:    "events",
        GroupID:  "go-events-group",
        MinBytes: 1,
        MaxBytes: 10e6, // 10MB
    })
    defer reader.Close()

    fmt.Println("Consumer started. Waiting for messages...")

    // ── 2. Graceful shutdown ─────────────────────────────────────
    ctx, cancel := context.WithCancel(context.Background())

    sigCh := make(chan os.Signal, 1)
    signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
    go func() {
        <-sigCh
        fmt.Println("\nShutting down...")
        cancel()
    }()

    // ── 3. Read loop ─────────────────────────────────────────────
    for {
        msg, err := reader.ReadMessage(ctx)
        if err != nil {
            if ctx.Err() != nil {
                break // context was cancelled — clean shutdown
            }
            log.Printf("read error: %v", err)
            continue
        }

        // ── 4. Decode JSON ───────────────────────────────────────
        var event Event
        if err := json.Unmarshal(msg.Value, &event); err != nil {
            fmt.Printf("bad message at offset %d: %v", msg.Offset, err)
            continue
        }

		_, err = db.Exec(context.Background(),
		 `INSERT INTO events (event_id, user_id, event_type, time_created) VALUES ($1, $2, $3, $4)`, 
		 event.EventID,
		 event.UserID,
		 event.EventType,
		 event.TimeCreated,)
		if err != nil {
		 	fmt.Println("db error", err)
		 	continue
		 }


        // ── 5. Process it ────────────────────────────────────────
		fmt.Printf("event=%s | user=%s | state=%s | time=%s\n",
			event.EventID,
			event.UserID,
			event.EventType,
			event.TimeCreated,
		)
        fmt.Printf("  partition=%d offset=%d\n", msg.Partition, msg.Offset)
    }

    fmt.Println("Consumer closed")
}