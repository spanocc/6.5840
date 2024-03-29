package shardkv

import (
	"fmt"
	"log"
	"os"
	"time"
)

// Debugging
var Debug = false

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

type SharedCtrRole int

const (
	ClerkRole SharedCtrRole = iota
	ServerRole
)

func Role2String(scr SharedCtrRole) string {
	if scr == ClerkRole {
		return string("kvClerk ")
	} else if scr == ServerRole {
		return string("kvServer")
	}
	return "ERROR Role"
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

func DPrintf(role SharedCtrRole, gid int, serverId int, level LogLevel, format string, a ...interface{}) (n int, err error) {
	if level < currentLogLevel() || Debug == false {
		return
	}
	now := time.Now()
	formattedTime := now.Format("15:04:05.000000")
	prefix := fmt.Sprintf("%s %s G%-5d - S%-2d %s ", formattedTime, Role2String(role), gid, serverId, Level2String(level))
	format = prefix + format
	fmt.Printf(format, a...)

	if level == ERROR {
		panic(format)
	}

	return
}
