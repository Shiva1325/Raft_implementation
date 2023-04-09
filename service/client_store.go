package service

import (
	"chat-system/pb"

)

// stores the active user and group info
type ClientStore interface {
	SetUser(*pb.User) error
	SetGroup(*pb.Group) error
	GetUser() *pb.User
	GetGroup() *pb.Group
}

type InMemoryClientStore struct {
	active_user *pb.User
	group       *pb.Group
}

func NewInMemoryClientStore() *InMemoryClientStore {
	group := &pb.Group{
		GroupID:      0,
		Groupname:    "",
		Participants: make(map[uint32]string),
		Messages:     make(map[uint32]*pb.ChatMessage),
	}
	active_user := &pb.User{
		Name: "",
		Id:   0,
	}

	return &InMemoryClientStore{
		active_user: active_user,
		group:       group,
	}
}

// setting a client user
func (clientstore *InMemoryClientStore) SetUser(user *pb.User) error {
	clientstore.active_user = user
	return nil
}

// setting a client group
func (clientstore *InMemoryClientStore) SetGroup(group *pb.Group) error {
	clientstore.group = group
	return nil
}

// getting a client user
func (clientstore *InMemoryClientStore) GetUser() *pb.User {
	return clientstore.active_user
}

// getting a client group
func (clientstore *InMemoryClientStore) GetGroup() *pb.Group {
	return clientstore.group
}
