package main

import (
	"chat-system/pb"
	"chat-system/service"
	"flag"
	"log"
	"net"
	"strconv"
	"sync"

	"github.com/soheilhy/cmux"
	"google.golang.org/grpc"
)

func main() {
	// Process commandline arguments
	// address := flag.String("Address", "0.0.0.0", "server address")
	// portArg := flag.Int("port", 12000, "the server port")
	id := flag.Int("id", 1, "Server_ID")
	address := flag.String("Address", "0.0.0.0", "server address")
	flag.Parse()
	port := 12000 + *id
	serverAddress := *address + ":" + strconv.Itoa(int(port))
	//port := *portArg
	Listener, err := net.Listen("tcp", serverAddress)
	var wg sync.WaitGroup
	mux := cmux.New(Listener)
	trpcL := mux.Match(cmux.Any())
	//register the server
	grpcserver := grpc.NewServer()
	groupstore := service.NewInMemoryGroupStore()
	clients := service.NewInMemoryConnStore()
	userstore := service.NewInMemoryUserStore()
	raftServer := service.CreateServer(uint64(*id), trpcL)
	chatserver := service.NewChatServiceServer(groupstore, userstore, clients, raftServer)
	pb.RegisterChatServiceServer(grpcserver, chatserver)
	pb.RegisterAuthServiceServer(grpcserver, chatserver)
	if err != nil {
		log.Fatal("cannot start server: %w", err)
	}
	log.Printf("Start GRPC server at %s", Listener.Addr())
	go grpcserver.Serve(Listener)
	raftServer.Serve()
	mux.Serve()
	wg.Wait()
	if err != nil {
		return
	}
}
