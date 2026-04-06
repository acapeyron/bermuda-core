package logger

import "log"

func Init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
}

func Info(msg string, args ...interface{}) {
	log.Printf("[INFO] "+msg, args...)
}

func Debug(msg string, args ...interface{}) {
	log.Printf("[DEBUG] "+msg, args...)
}

func Warn(msg string, args ...interface{}) {
	log.Printf("[WARN] "+msg, args...)
}

func Error(msg string, args ...interface{}) {
	log.Printf("[ERROR] "+msg, args...)
}
