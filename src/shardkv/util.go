package shardkv

import (
	"fmt"
	"strconv"
	"math/rand"
)

var verbose = true

func debug(debug_message string) {
  if verbose {
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