package logger

import (
	"os"

	"github.com/sirupsen/logrus"
)

var Log = logrus.New()

func InitLogger() {
	// Set format to JSON so it's easy for Docker/Kubernetes to parse
	Log.SetFormatter(&logrus.JSONFormatter{
		TimestampFormat: "2006-01-02 15:04:05",
	})

	// Output to stdout (standard for Docker)
	Log.SetOutput(os.Stdout)

	// Set level (you could also load this from config)
	Log.SetLevel(logrus.InfoLevel)
}
