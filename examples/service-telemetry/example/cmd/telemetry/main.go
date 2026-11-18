package main

import (
	"flag"
	"log"
	"net/http"
	"time"

	demo "hexlet.local/telemetry"
)

func main() {
	scenario := flag.String("scenario", "slow-checks", demo.ScenarioNames())
	listen := flag.String("listen", "127.0.0.1:8155", "local HTTP address")
	flag.Parse()
	handler, err := demo.NewServer(*scenario)
	if err != nil {
		log.Fatal(err)
	}
	server := &http.Server{Addr: *listen, Handler: handler, ReadHeaderTimeout: 2 * time.Second}
	log.Printf("teaching fixture %s on %s", *scenario, *listen)
	log.Fatal(server.ListenAndServe())
}
