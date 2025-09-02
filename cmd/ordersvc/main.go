package main

import (
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"

	_ "github.com/lib/pq"

	"github.com/zaycv/go-order-service/internal/cache"
	"github.com/zaycv/go-order-service/internal/kafka"
	"github.com/zaycv/go-order-service/internal/storage"
)

func main() {
	// --- 1. Подключение к Postgres ---
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		dsn = "postgres://app:app@localhost:5433/orders?sslmode=disable"
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Fatalf("error opening database: %v", err)
	}
	if err = db.Ping(); err != nil {
		log.Fatalf("error connecting to database: %v", err)
	}
	log.Println("connected to Postgres")

	store := &storage.Storage{DB: db}
	c := cache.NewCache()

	// --- 2. Заполняем кеш из БД ---
	if err := c.LoadFromDB(store); err != nil {
		log.Printf("failed to load cache: %v", err)
	}

	// --- 3. Запускаем Kafka consumer (с обновлением кеша) ---
	go kafka.ConsumeKafka(store, c)

	// --- 4. HTTP-сервер ---
	mux := http.NewServeMux()

	// --- 5. Отдача веб-интерфейса ---
	fs := http.FileServer(http.Dir("./web"))
	mux.Handle("/", fs)

	// healthcheck
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

	// получить заказ по ID
	mux.HandleFunc("/order/", func(w http.ResponseWriter, r *http.Request) {
		orderUID := r.URL.Path[len("/order/"):]
		if orderUID == "" {
			http.Error(w, "missing order_uid", http.StatusBadRequest)
			return
		}

		// Сначала кеш
		if order, ok := c.Get(orderUID); ok {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(order)
			return
		}

		// Потом БД
		order, err := store.GetOrder(orderUID)
		if err != nil {
			http.Error(w, "order not found", http.StatusNotFound)
			return
		}
		c.Set(order)

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(order)
	})

	srv := &http.Server{
		Addr:              ":8081",
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Println("order-service listening on :8081")
	log.Fatal(srv.ListenAndServe())
}
