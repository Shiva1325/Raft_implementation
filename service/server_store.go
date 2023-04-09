package service

import (
	"chat-system/pb"
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/google/uuid"
	"github.com/jinzhu/copier"
)

// userstore stores the user information on server
type UserStore interface {
	SaveUser(user *pb.User) error
	DeleteUser(User *pb.User)
}

// groupstore stores the group information on server
type GroupStore interface {
	GetGroup(groupname string) *pb.Group
	JoinGroup(groupname string, user *pb.User) (*pb.Group, error)
	AppendMessage(appendchat *pb.AppendChat) error
	LikeMessage(like *pb.LikeMessage) error
	UnLikeMessage(unlike *pb.UnLikeMessage) error
	RemoveUser(user *pb.User, groupname string)
}

//stores the incoming connection so as to braodcast later
type ConnStore interface {
	BroadCast(groupname string, resp *pb.GroupChatResponse) error
	AddConn(stream pb.ChatService_GroupChatServer, client [2]string)
	RemoveConn(client [2]string)
}

type InMemoryConnStore struct {
	mutex   sync.RWMutex
	clients map[pb.ChatService_GroupChatServer][2]string
}

type InMemoryUserStore struct {
	mutex sync.RWMutex
	User  map[uint32]*pb.User
}

type InMemoryGroupStore struct {
	mutex sync.RWMutex
	Group map[string]*pb.Group
}

func NewInMemoryConnStore() *InMemoryConnStore {
	return &InMemoryConnStore{
		clients: make(map[pb.ChatService_GroupChatServer][2]string),
	}
}

func NewInMemoryUserStore() *InMemoryUserStore {
	return &InMemoryUserStore{
		User: make(map[uint32]*pb.User),
	}
}

func NewInMemoryGroupStore() *InMemoryGroupStore {

	return &InMemoryGroupStore{
		Group: make(map[string]*pb.Group, 0),
	}
}

// Broadcasts the respecitve group infos to the connected clients
func (conn *InMemoryConnStore) BroadCast(groupname string, res *pb.GroupChatResponse) error {
	for stream, name := range conn.clients {
		if name[0] == groupname {
			if stream.Context().Err() == context.Canceled || stream.Context().Err() == context.DeadlineExceeded {
				delete(conn.clients, stream)
				continue
			}
			stream.Send(res)

		}
	}

	log.Printf("Broadcasted succesfully")
	return nil
}

// adds an incoming conn in the server connstore
func (conn *InMemoryConnStore) AddConn(stream pb.ChatService_GroupChatServer, client [2]string) {
	conn.mutex.Lock()
	defer conn.mutex.Unlock()
	if conn.clients == nil {
		conn.clients = make(map[pb.ChatService_GroupChatServer][2]string)
	}
	currclient, found := conn.clients[stream]
	if found && currclient == client {
		log.Printf("Client already present")
		return
	}
	conn.clients[stream] = client
	log.Printf("Client added")
}

// removes an incoming conn in the server connstore
func (conn *InMemoryConnStore) RemoveConn(removeclient [2]string) {
	conn.mutex.Lock()
	defer conn.mutex.Unlock()
	if conn.clients == nil {
		log.Printf("No connection present")
	}
	for stream, client := range conn.clients {
		if removeclient == client {
			delete(conn.clients, stream)
			return
		}
	}
	log.Println("No record found in connection store")
}

func (userstore *InMemoryUserStore) SaveUser(user *pb.User) error {
	userstore.mutex.Lock()
	defer userstore.mutex.Unlock()

	usercopy := &pb.User{}
	err := copier.Copy(usercopy, user)
	if err != nil {
		return fmt.Errorf("error while deepcopy user: %w", err)
	}
	Id := usercopy.GetId()
	userstore.User[Id] = usercopy

	log.Printf("user %v logged in the server", user.GetName())

	return nil
}

// deletes a user in the userstore
func (userstore *InMemoryUserStore) DeleteUser(user *pb.User) {
	delete(userstore.User, user.Id)
}

// retreives a group info
func (group_master *InMemoryGroupStore) GetGroup(groupname string) *pb.Group {
	return group_master.Group[groupname]
}

// joins a client to the requested group
func (group_master *InMemoryGroupStore) JoinGroup(groupname string, user *pb.User) (*pb.Group, error) {
	group_master.mutex.Lock()
	defer group_master.mutex.Unlock()
	//try finding the group in the groupstore

	group, found := group_master.Group[groupname]
	if found {
		group.Participants[user.GetId()] = user.GetName()
		return group, nil
	}
	//if not found create one
	new_group := &pb.Group{
		GroupID:      uuid.New().ID(),
		Groupname:    groupname,
		Participants: make(map[uint32]string),
		Messages:     make(map[uint32]*pb.ChatMessage),
	}
	group_master.Group[groupname] = new_group
	new_group.Participants[user.GetId()] = user.GetName()

	log.Printf("user %v joined %v group", user.GetName(), groupname)
	return new_group, nil
}

// appends a message in the group with the user and message information
func (group_master *InMemoryGroupStore) AppendMessage(appendchat *pb.AppendChat) error {
	group_master.mutex.Lock()
	defer group_master.mutex.Unlock()
	//get groupname and message.
	groupname := appendchat.Group.GetGroupname()
	chatmessage := &pb.ChatMessage{
		MessagedBy: appendchat.Chatmessage.MessagedBy,
		Message:    appendchat.Chatmessage.Message,
		LikedBy:    make(map[uint32]string),
	}
	log.Printf("chatmessage arrived is %v", chatmessage)
	//get group and messagenumber
	group := group_master.GetGroup(groupname)
	chatmessagenumber := len(group.Messages)
	//append in the group
	group.Messages[uint32(chatmessagenumber)] = chatmessage
	log.Printf("group messages are %v", group.Messages)
	log.Printf("group %v has a new message appended", groupname)
	return nil
}

// likes a message in the group
func (group_master *InMemoryGroupStore) LikeMessage(likemessage *pb.LikeMessage) error {
	group_master.mutex.Lock()
	defer group_master.mutex.Unlock()
	groupname := likemessage.Group.GetGroupname()
	likedmsgnumber := likemessage.GetMessageid()
	likeduser := likemessage.User

	//get the group
	group := group_master.GetGroup(groupname)
	//validate and get the message to be liked
	message, found := group.Messages[likedmsgnumber]
	if !found {
		return fmt.Errorf("please enter valid message")
	}
	log.Printf("getting the message : %v", message)
	//like it only if he is not the sender of the message
	if message.MessagedBy.Id == likeduser.Id {
		return fmt.Errorf("cannot like you own message")
	}
	//check if the like is already present
	user, found := message.LikedBy[likeduser.GetId()]
	if found {
		return fmt.Errorf("message already liked")
	}
	//like
	message.LikedBy[likeduser.GetId()] = user

	log.Printf("message liked")
	return nil
}

// unlikes a message in the group
func (group_master *InMemoryGroupStore) UnLikeMessage(unlikemessage *pb.UnLikeMessage) error {
	group_master.mutex.Lock()
	defer group_master.mutex.Unlock()
	groupname := unlikemessage.Group.GetGroupname()
	unlikedmsgnumber := unlikemessage.GetMessageid()
	unlikeduser := unlikemessage.User

	//get the group
	group := group_master.GetGroup(groupname)
	//validate and get the message to be liked
	message, found := group.Messages[unlikedmsgnumber]
	if !found {
		return fmt.Errorf("please enter valid message")
	}
	//like it only if he is not the sender of the message
	if message.MessagedBy.Id == unlikeduser.Id {
		return fmt.Errorf("cannot unlike you own message")
	}
	//check if the like is present
	username, found := message.LikedBy[unlikeduser.GetId()]
	if !found {
		return fmt.Errorf("message never liked")
	}
	//unlike
	delete(message.LikedBy, unlikeduser.GetId())

	log.Printf("user %s unliked a message", username)
	return nil
}

// when the user left the group, we remove the user from participants of the group
func (group_master *InMemoryGroupStore) RemoveUser(user *pb.User, groupname string) {
	groupmap := group_master.Group[groupname]
	if groupmap == nil {
		return
	} else if groupmap.Participants == nil {
		return
	} else if groupmap.Participants[user.Id] == "" {

		return
	}

	delete(group_master.Group[groupname].Participants, user.Id)
}
