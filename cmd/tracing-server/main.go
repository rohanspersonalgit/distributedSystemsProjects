 package main

import (
	"log"
	"github.com/DistributedClocks/tracing"
)

func main() {
	tracingServer := tracing.NewTracingServerFromFile("config/tracing_server_config.json")

	err := tracingServer.Open()
	if err != nil {
		log.Fatal(err)
	}

	tracingServer.Accept() // serve requests forever
}
