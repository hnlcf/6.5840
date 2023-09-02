package mr

import (
	"github.com/sirupsen/logrus"
)

var logru = logrus.New()

func InitLogger() {
	// Customize the logger settings as needed
	logru.SetFormatter(&logrus.TextFormatter{
		FullTimestamp:    true,
		DisableTimestamp: false,
	})
}

// GetLogger returns the package-level logger for use in other parts of your program.
func GetLogger() *logrus.Logger {
	return logru
}
