package config

import "os"

type Config struct {
	HTTPAddr     string
	PostgresDSN  string
	KafkaBrokers string
	KafkaTopic   string
}

func FromEnv() Config {
	return Config{
		HTTPAddr:     getenv("HTTP_ADDR", ":8081"),
		PostgresDSN:  os.Getenv("POSTGRES_DSN"),
		KafkaBrokers: getenv("KAFKA_BROKERS", "localhost:9092"),
		KafkaTopic:   getenv("KAFKA_TOPIC", "orders"),
	}
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
