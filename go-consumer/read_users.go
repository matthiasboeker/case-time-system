package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
    "os/signal"
    "syscall"
    "github.com/segmentio/kafka-go"
	"github.com/jackc/pgx/v5"

)

type User struct {
	UserID  string `json:"user_id"`
	Name    string `json:"name"`
	Age     int    `json:"age"`
	Kommune string `json:"kommune"`
}

func main(){
	fmt.Println(os.Getenv("DATABASE_URL"))
	conn, err := pgx.Connect(context.Background(), "postgres://user:password@localhost:5432/orders_db")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close(context.Background())


	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic: "users",
		GroupID:  "go-users-group",
		Partition: 0,
		MaxBytes: 10e6,
		StartOffset: kafka.FirstOffset,
	})
	defer func() {
		if err := r.Close(); err != nil {
			log.Println("failed to close reader:", err)
		}
	}()

	
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	for {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			log.Fatal("failed:", err)
		}
		var user User
		if err := json.Unmarshal(m.Value, &user); err != nil {
			fmt.Println("error:", err)
		}
		fmt.Printf("%+v\n", user)
		_, err = conn.Exec(context.Background(),
		 `INSERT INTO users (user_id, name, age, kommune) VALUES ($1, $2, $3, $4)`, 
		 user.UserID,
		 user.Name,
		 user.Age,
		user.Kommune)
		if err != nil {
		 	fmt.Println("db error", err)
		 	continue
		 }

	}
	if err := r.Close(); err != nil {
		log.Fatal("failed to close reader:", err)
	}

}