package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	audit "hexlet.local/checksrpc"
	pb "hexlet.local/checksrpc/checks/v1"
)

func main() {
	serverMode := flag.Bool("server", false, "serve prepared results")
	id := flag.String("audit", "a-41", "audit to read")
	flag.Parse()
	if *serverMode {
		listener, err := net.Listen("tcp", "127.0.0.1:9090")
		if err != nil {
			log.Fatal(err)
		}
		server := grpc.NewServer()
		pb.RegisterChecksServiceServer(server, &audit.RPCServer{Reader: audit.FixtureReader{}})
		log.Print("checks RPC ready on 127.0.0.1:9090")
		log.Fatal(server.Serve(listener))
	}
	connection, err := grpc.NewClient("127.0.0.1:9090", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	defer connection.Close()
	reader := &audit.RPCReader{Client: pb.NewChecksServiceClient(connection)}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	result, err := reader.Result(ctx, *id)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		log.Fatal(err)
	}
}
