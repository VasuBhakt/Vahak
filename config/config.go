package config

import (
	"log"
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

type Config struct {
	AckMode           string // "durable" (default) or "fast"
	DBEngine          string
	DBPath            string
	DBUrl             string
	DBPoolUrl         string
	Port              string
	APIKey            string
	AllowLocalTargets bool
	AllowInsecureKey  bool
	RetentionDays     int
}

func Load() *Config {
	err := godotenv.Load()
	if err != nil {
		log.Println("no .env file found, reading from environment")
	}

	ackMode := os.Getenv("ACK_MODE")
	if ackMode != "fast" {
		ackMode = "durable"
	}

	allowLocal := os.Getenv("ALLOW_LOCAL_TARGETS") == "true"
	allowInsecureKey := os.Getenv("ALLOW_INSECURE_DEFAULT_KEY") == "true"
	dbUrl := os.Getenv("DB_URL")
	dbEngine := os.Getenv("DB_ENGINE")
	if dbEngine != "postgres" && dbEngine != "sqlite" {
		if len(dbUrl) >= 8 && (dbUrl[:8] == "postgres" || (len(dbUrl) >= 10 && dbUrl[:10] == "postgresql")) {
			dbEngine = "postgres"
		} else {
			dbEngine = "sqlite"
		}
	}

	dbPath := os.Getenv("DB_PATH")
	if dbPath == "" {
		dbPath = "vahak.db"
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	retentionDays := 7
	if rStr := os.Getenv("RETENTION_DAYS"); rStr != "" {
		if rVal, err := strconv.Atoi(rStr); err == nil {
			retentionDays = rVal
		}
	}

	dbPoolUrl := os.Getenv("DB_POOL_URL")
	if dbPoolUrl == "" {
		dbPoolUrl = dbUrl
	}

	return &Config{
		AckMode:           ackMode,
		DBEngine:          dbEngine,
		DBPath:            dbPath,
		DBUrl:             dbUrl,
		DBPoolUrl:         dbPoolUrl,
		Port:              port,
		APIKey:            os.Getenv("API_KEY"),
		AllowLocalTargets: allowLocal,
		AllowInsecureKey:  allowInsecureKey,
		RetentionDays:     retentionDays,
	}
}
