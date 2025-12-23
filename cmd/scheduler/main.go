package main

import (
	"log"
	"github.com/nats-io/nats.go"
)

func main() {
	// Connect to Docker NATS
	nc, err := nats.Connect("nats://localhost:4222")
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Scheduler connected to NATS!")
    select {} // Block forever
}