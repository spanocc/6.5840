package raft

import (
	"fmt"
	"log"
	"os"
	"time"
)

// Debugging
const Debug = true

type LogLevel int

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

func Role2String(rr RaftRole) string {
	if rr == Leader {
		return string("Lead")
	} else if rr == Candidate {
		return string("Cand")
	} else {
		return string("Foll")
	}
}

func currentLogLevel() LogLevel {
	v := os.Getenv("LOGLEVEL")
	level := DEBUG
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

func DPrintf(role RaftRole, serverId int, term int, level LogLevel, format string, a ...interface{}) (n int, err error) {
	if level < currentLogLevel() {
		return
	}
	now := time.Now()
	formattedTime := now.Format("15:04:05.000000")
	prefix := fmt.Sprintf("%s %s S%-2d term:%-2d %s ", formattedTime, Role2String(role), serverId, term, Level2String(level))
	format = prefix + format
	fmt.Printf(format, a...)

	if level == ERROR {
		panic(format)
	}

	return
}
