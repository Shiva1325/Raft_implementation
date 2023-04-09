package service

import (
	"bufio"
	"chat-system/pb"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/google/uuid"
	"golang.org/x/exp/maps"
	"google.golang.org/grpc/metadata"
)

// client service
type ChatServiceClient struct {
	chatservice pb.ChatServiceClient
	authservice pb.AuthServiceClient
	clientstore ClientStore
}

func NewChatServiceClient(chatservice pb.ChatServiceClient, authservice pb.AuthServiceClient, store ClientStore) *ChatServiceClient {
	return &ChatServiceClient{
		chatservice: chatservice,
		authservice: authservice,
		clientstore: store,
	}
}

// rpc to send the join request
func JoinGroup(groupname string, client *ChatServiceClient) error {
	//adding grouname and userdata to metadata to subscribe to broadcasts.
	md := metadata.Pairs("groupname", client.clientstore.GetGroup().Groupname, "userid", strconv.Itoa(int(client.clientstore.GetUser().Id)))
	ctx := metadata.NewOutgoingContext(context.Background(), md)
	user := client.clientstore.GetUser()
	group := client.clientstore.GetGroup()
	joinchat := &pb.JoinChat{
		Newgroup:  groupname,
		User:      user,
		Currgroup: group.Groupname,
	}
	req := &pb.JoinRequest{
		Joinchat: joinchat,
	}
	res, err := client.chatservice.JoinGroup(ctx, req)
	if err != nil {
		return err
	}
	err = client.clientstore.SetGroup(res.GetGroup())
	fmt.Printf("joined :%s\n", client.clientstore.GetGroup().Groupname)
	return nil
}

func ClientActiveStatus(user_details *pb.LoginRequest, client *ChatServiceClient) {
	log.Println("Heart Beat started")
	stream, _ := client.authservice.ClientStatus(context.Background())
	for {
		stream.Send(user_details)
	}
}

// login rpc
func UserLogin(user_name string, client *ChatServiceClient) error {
	user := &pb.User{
		Id:   uuid.New().ID(),
		Name: user_name,
	}
	user_details := &pb.LoginRequest{
		User: user,
	}
	go ClientActiveStatus(user_details, client)
	_, err := client.authservice.Login(context.Background(), user_details)
	if err != nil {
		return fmt.Errorf("Failed to create user: %v", err)
	}
	client.clientstore.SetUser(user)
	log.Printf("User %v Logged in succesfully.", client.clientstore.GetUser().Name)

	return nil
}

// logoutrpc to remove the user from the server
func UserLogout(client *ChatServiceClient) bool {
	user := client.clientstore.GetUser()
	req := &pb.LogoutRequest{
		User: &pb.Logout{
			User: user,
		},
	}
	resp, err := client.authservice.Logout(context.Background(), req)
	if err != nil {
		log.Println("cannot Logout.Please Try again")
	}
	client.clientstore.SetUser(
		&pb.User{
			Name: "",
			Id:   0,
		},
	)
	log.Printf("User %v Logged out succesfully.", user.Name)
	return resp.Status
}

var wg = new(sync.WaitGroup)

// Bi-directional streaming rpc to send and receive the group messages
func GroupChat(client *ChatServiceClient) error {
	md := metadata.Pairs("groupname", client.clientstore.GetGroup().Groupname, "userid", strconv.Itoa(int(client.clientstore.GetUser().Id)))
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	stream, err := client.chatservice.GroupChat(ctx)
	if err != nil {
		return err
	}
	waitResponse := make(chan error)
	wg.Add(2)
	go send(stream, client)
	//receive stream
	go func() error {
		defer log.Println("Receive stream ended")
		defer wg.Done()
		for {
			err := contextError(stream.Context())
			if err != nil {
				return err
			}
			res, err := stream.Recv()
			if err == io.EOF {
				log.Print("no more responses")

				waitResponse <- nil
				return err
			}
			if err != nil {
				waitResponse <- fmt.Errorf("cannot receive stream response: %v", err)
				return err
			}
			command := res.Command
			if command == "p" {
				PrintAll(res.Group)
			} else {
				PrintRecent(res.Group)
			}
		}
	}()
	wg.Wait()
	err = <-waitResponse
	return err
}

func send(stream pb.ChatService_GroupChatClient, client *ChatServiceClient) error {
	defer wg.Done()
	for {
		log.Printf("Enter the message in the group:")
		msg, err := bufio.NewReader(os.Stdin).ReadString('\n')
		if err != nil {
			log.Fatalf("Cannot read the message, please enter again\n")
		}

		msg = strings.Trim(msg, "\r\n")
		args := strings.Split(msg, " ")
		cmd := strings.TrimSpace(args[0])
		msg = strings.Join(args[1:], " ")
		switch cmd {

		case "a":
			if client.clientstore.GetUser().GetName() == "" {
				log.Println("Please login to join a group.")
			} else if client.clientstore.GetGroup().Groupname == "" {
				log.Println("Please join a group to send a message")
			} else {
				//appending a message
				appendchat := &pb.GroupChatRequest_Append{
					Append: &pb.AppendChat{
						Group: client.clientstore.GetGroup(),
						Chatmessage: &pb.ChatMessage{
							MessagedBy: client.clientstore.GetUser(),
							Message:    msg,
							LikedBy:    make(map[uint32]string, 0),
						},
					},
				}
				req := &pb.GroupChatRequest{
					Action: appendchat,
				}
				stream.Send(req)
				log.Printf("appended a message in the group")
			}

		//like message
		case "l":
			if client.clientstore.GetUser().GetName() == "" {
				log.Println("Please login to join a group.")
			} else if client.clientstore.GetGroup().Groupname == "" {
				log.Println("Please join a group to send a message")
			} else {
				messagenumber, err := strconv.ParseUint(msg, 10, 32)
				if err != nil {
					log.Printf("please provide a valid number to like")
					continue
				}
				likemessage := &pb.GroupChatRequest_Like{
					Like: &pb.LikeMessage{
						User:      client.clientstore.GetUser(),
						Messageid: uint32(messagenumber),
						Group:     client.clientstore.GetGroup(),
					},
				}
				req := &pb.GroupChatRequest{
					Action: likemessage,
				}
				stream.Send(req)
			}

		case "r":
			messagenumber, err := strconv.ParseUint(msg, 10, 32)
			if err != nil {
				log.Printf("please provide a valid number to unlike")
			}
			unlikemessage := &pb.GroupChatRequest_Unlike{
				Unlike: &pb.UnLikeMessage{
					User:      client.clientstore.GetUser(),
					Messageid: uint32(messagenumber),
					Group:     client.clientstore.GetGroup(),
				},
			}
			req := &pb.GroupChatRequest{
				Action: unlikemessage,
			}
			stream.Send(req)

		case "p":
			{
				print := &pb.GroupChatRequest_Print{
					Print: &pb.PrintChat{
						User:      client.clientstore.GetUser(),
						Groupname: client.clientstore.GetGroup().Groupname,
					},
				}
				req := &pb.GroupChatRequest{
					Action: print,
				}
				stream.Send(req)
			}

		case "j":
			//join the group
			groupname := strings.TrimSpace(args[1])
			if groupname == "" {
				log.Println("Please provide a valid group name")
			} else {
				err = JoinGroup(groupname, client)
				if err != nil {
					log.Printf("Failed to join a group: %v", err)
					continue
				}
				GroupChat(client)
			}

		//quit the program
		case "q":
			logout := &pb.GroupChatRequest_Logout{
				Logout: &pb.Logout{
					User: client.clientstore.GetUser(),
				},
			}
			req := &pb.GroupChatRequest{
				Action: logout,
			}
			stream.Send(req)
			stream.CloseSend()
			return nil

		default:
			log.Printf("Cannot read the message, please enter again")
			continue
		}
	}

}

// print only 10 recent messages
func PrintRecent(group *pb.Group) {
	groupname := group.GetGroupname()
	participants := maps.Values(group.GetParticipants())
	chatmessages := group.GetMessages()
	chatlength := len(chatmessages)
	print_recent := 10
	fmt.Printf("Group: %v\n", groupname)
	fmt.Printf("Participants: %v\n", participants)
	for print_recent > 0 {
		i := uint32(chatlength - print_recent)

		chatmessage, found := chatmessages[i]
		if found {
			fmt.Printf("%v. %v: %v                     likes: %v\n", i, chatmessage.MessagedBy.Name, chatmessage.Message, len(chatmessage.LikedBy))
		}
		print_recent--
	}

}

// printall
func PrintAll(group *pb.Group) {
	groupname := group.GetGroupname()
	participants := maps.Values(group.GetParticipants())
	chatmessages := group.GetMessages()
	chatlength := len(chatmessages)
	fmt.Printf("Group: %v\n", groupname)
	fmt.Printf("Participants: %v\n", participants)
	count := 0
	for count < chatlength {
		i := uint32(count)

		chatmessage, found := chatmessages[i]
		if found {
			fmt.Printf("%v. %v: %v                     likes: %v\n", i, chatmessage.MessagedBy.Name, chatmessage.Message, len(chatmessage.LikedBy))
		}
		count++
	}

}
