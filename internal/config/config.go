package config

import (
	"time"

	"github.com/caarlos0/env/v11"
)

type Config struct {
	ListenAddr      string        `env:"LISTEN_ADDR" envDefault:":8080"`
	ShutdownTimeout time.Duration `env:"SHUTDOWN_TIMEOUT" envDefault:"3s"`
}

func Load() (Config, error) {
	return env.ParseAs[Config]()
}