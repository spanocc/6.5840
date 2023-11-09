package kvraft

import (
	"fmt"
	"log"
	"os"
	"time"
)

// Debugging
const Debug = true

type LogLevel int

type ConnectionRole int

const (
	ClerkRole ConnectionRole = iota
	serverRole
)

const (
	DEBUG LogLevel = iota
	INFO
	WARN
	ERROR
)

func Level2String(level LogLevel) string {
	if level == DEBUG {
		return "DEBUG"
	} else if level == INFO {
		return "INFO "
	} else if level == WARN {
		return "WARN "
	} else {
		return "ERROR"
	}
}

func Role2String(rr ConnectionRole) string {
	if rr == ClerkRole {
		return string("Clek")
	} else {
		return string("Serv")
	}
}

func currentLogLevel() LogLevel {
	v := os.Getenv("LOGLEVEL")
	level := INFO
	if v != "" {
		if v == "DEBUG" {
			level = DEBUG
		} else if v == "INFO" {
			level = INFO
		} else if v == "WARN" {
			level = WARN
		} else if v == "ERROR" {
			level = ERROR
		} else {
			log.Fatal("Invalid LogLevel: v", v)
		}
	}
	return level
}

func DPrintf(role ConnectionRole, me int, level LogLevel, format string, a ...interface{}) (n int, err error) {
	if level < currentLogLevel() {
		return
	}
	now := time.Now()
	formattedTime := now.Format("15:04:05.000000")
	prefix := fmt.Sprintf("%s %s S%-2d %s ", formattedTime, Role2String(role), me, Level2String(level))
	format = prefix + format
	fmt.Printf(format, a...)

	if level == ERROR {
		panic(format)
	}

	return
}
