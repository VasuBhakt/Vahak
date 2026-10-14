package config

import (
	"log"
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

type Config struct {
	DBEngine          string
	DBPath            string
	DBUrl             string
	DBPoolUrl         string
	Port              string
	APIKey            string
	AllowLocalTargets bool
	RetentionDays     int
}

func Load() *Config {
	err := godotenv.Load()
	if err != nil {
		log.Println("no .env file found, reading from environment")
	}

	allowLocal := os.Getenv("ALLOW_LOCAL_TARGETS") == "true"
	dbUrl := os.Getenv("DB_URL")
	dbEngine := os.Getenv("DB_ENGINE")
	if dbEngine == "" {
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

	return &Config{
		DBEngine:          dbEngine,
		DBPath:            dbPath,
		DBUrl:             dbUrl,
		DBPoolUrl:         os.Getenv("DB_POOL_URL"),
		Port:              port,
		APIKey:            os.Getenv("API_KEY"),
		AllowLocalTargets: allowLocal,
		RetentionDays:     retentionDays,
	}
}
