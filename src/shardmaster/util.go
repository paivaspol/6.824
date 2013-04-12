package shardmaster

import (
	"fmt"
	"strconv"
	"math/rand"
)

var debug = true

func output_debug(debug_message string) {
  if debug {
    fmt.Println(debug_message)
  }
}

/*
TODO: Does not actually return a UUID according to standards. Just a random
in which suffices for now.
*/
func generate_uuid() string {
  return strconv.Itoa(rand.Int())
}