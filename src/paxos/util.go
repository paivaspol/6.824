package paxos

import "fmt"

var debug = true

func output_debug(debug_message string) {
  if debug {
    fmt.Println(debug_message)
  }
}