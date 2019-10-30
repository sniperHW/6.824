package raft

import "log"
import "math/rand"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func randBetween(min int, max int) int {
	return rand.Int()%max + min
}
