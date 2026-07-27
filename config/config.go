package config

import (
	"log"
	"os"

	"github.com/joho/godotenv"
)

type Config struct {
	DBUrl             string
	DBPoolUrl         string
	Port              string
	APIKey            string
	AllowLocalTargets bool
}

func Load() *Config {
	err := godotenv.Load()
	if err != nil {
		log.Println("no .env file found, reading from environment")
	}

	allowLocal := os.Getenv("ALLOW_LOCAL_TARGETS") == "true"

	return &Config{
		DBUrl:             os.Getenv("DB_URL"),
		DBPoolUrl:         os.Getenv("DB_POOL_URL"),
		Port:              os.Getenv("PORT"),
		APIKey:            os.Getenv("API_KEY"),
		AllowLocalTargets: allowLocal,
	}
}
