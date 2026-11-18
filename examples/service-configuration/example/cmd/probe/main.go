package main

import (
	"log"
	"net"
	"os"
	"time"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatal("usage: probe host:port")
	}
	connection, err := net.DialTimeout("tcp", os.Args[1], time.Second)
	if err != nil {
		log.Fatal(err)
	}
	connection.Close()
}
