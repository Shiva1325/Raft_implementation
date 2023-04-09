package client_main

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"os"

	"chat-system/pb"
	"chat-system/service"
	"strings"

	"google.golang.org/grpc"

	"google.golang.org/grpc/credentials/insecure"
)

// client
func CallClient(server_address string, port string) error {
	log.Printf("Dialing to server %s:%v", server_address, port)

	// Connect to RPC server
	transportOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	conn, err := grpc.Dial(server_address+":"+port, transportOption)
	if err != nil {
		log.Fatal("cannot dial the server", err)
		return errors.New("Please provide correct address")
	}

	log.Printf("Dialing to server %s:%v", server_address, port)
	log.Printf("Target : %v :%v ", conn.GetState(), conn.Target())
	clientstore := service.NewInMemoryClientStore()
	chatclient := service.NewChatServiceClient(pb.NewChatServiceClient(conn), pb.NewAuthServiceClient(conn), clientstore)

	for {
		//read input
		log.Printf("Enter the message:")
		msg, err := bufio.NewReader(os.Stdin).ReadString('\n')
		if err != nil {
			log.Fatalf("Cannot read the message, please enter again\n")
		}
		//parsing input
		msg = strings.Trim(msg, "\r\n")
		args := strings.Split(msg, " ")
		cmd := strings.TrimSpace(args[0])
		arg := strings.Join(args[1:], " ")

		switch cmd {
		case "u":
			username := strings.TrimSpace(arg)
			if username == "" {
				log.Println("Please provide a valid user name")
			} else {
				err := service.UserLogin(username, chatclient)
				if err != nil {
					fmt.Println(err)
					return nil
				}
			}

		case "j":
			if clientstore.GetUser().GetName() == "" {
				log.Println("Please login to join a group.")
			} else {
				//join the group
				groupname := strings.TrimSpace(arg)
				if groupname == "" {
					log.Println("Please provide a valid group name")
				} else {
					service.JoinGroup(groupname, chatclient)
					//start streaming
					service.GroupChat(chatclient)
				}

			}
		case "a", "l", "r":
			log.Println("please Enter the chat group to perform chat operations")

		case "q":
			if clientstore.GetUser().Name != "" {
				resp := service.UserLogout(chatclient)
				if resp {
					conn.Close()

					log.Println("closed localhost:12000 connection. Please enter address again to connect")
					return nil
				} else {
					log.Println("Failed to exit program. Please try again.")
				}
			} else {
				conn.Close()
				log.Println("closed localhost:12000 connection.  Please enter address again to connect")
				return nil
			}

		default:
			log.Printf("incorrect command, please enter again\n")
		}
	}

}
