package config

import (
	"log"
	"os"

	"github.com/joho/godotenv"
)

type Config struct {
	DBUrl     string
	DBPoolUrl string
	Port      string
	APIKey    string
}

func Load() *Config {
	err := godotenv.Load()
	if err != nil {
		log.Println("no .env file found, reading from environment")
	}

	return &Config{
		DBUrl:     os.Getenv("DB_URL"),
		DBPoolUrl: os.Getenv("DB_POOL_URL"),
		Port:      os.Getenv("PORT"),
		APIKey:    os.Getenv("API_KEY"),
	}
}
