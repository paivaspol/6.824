package paxos

import "fmt"

var debug = false

func output_debug(debug_message string) {
  if debug {
    fmt.Println(debug_message)
  }
}