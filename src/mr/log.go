package mr

import (
	"os"

	"github.com/sirupsen/logrus"
)

var logru = logrus.New()

func InitLogger() {
	// Customize the logger settings as needed
	logru.SetFormatter(&logrus.TextFormatter{
		FullTimestamp:    true,
		DisableTimestamp: false,
	})

	logru.SetOutput(os.Stdout)
	logru.SetLevel(logrus.DebugLevel)
}

// GetLogger returns the package-level logger for use in other parts of your program.
func GetLogger() *logrus.Logger {
	return logru
}
