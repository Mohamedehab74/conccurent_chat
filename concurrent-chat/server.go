package main

import (
	"fmt"
	"sort"
	"sync"
)

type EventType int

const (
	JoinEvent EventType = iota
	MessageEvent
	LeaveEvent
	ShutdownEvent
)

type Event struct {
	Type    EventType
	Client  *Client
	Message string
}

type Server struct {
	clients map[string]*Client
	events  chan Event
	quit    chan struct{}

	mu sync.Mutex
	wg sync.WaitGroup
}

func NewServer() *Server {
	return &Server{
		clients: make(map[string]*Client),
		events:  make(chan Event),
		quit:    make(chan struct{}),
	}
}

// Run starts the server's event-processing goroutine.
func (s *Server) Run() {
	s.wg.Add(1)

	go func() {
		defer s.wg.Done()

		for {
			select {
			case event := <-s.events:
				switch event.Type {
				case JoinEvent:
					s.handleJoin(event.Client)

				case MessageEvent:
					s.handleMessage(event.Client, event.Message)

				case LeaveEvent:
					s.handleLeave(event.Client)

				case ShutdownEvent:
					s.shutdownClients()
					return
				}

			case <-s.quit:
				s.shutdownClients()
				return
			}
		}
	}()
}

// handleJoin adds a new client to the server.
func (s *Server) handleJoin(client *Client) {
	s.mu.Lock()

	if _, exists := s.clients[client.Username]; exists {
		s.mu.Unlock()

		client.Incoming <- fmt.Sprintf(
			"Username '%s' is already taken.",
			client.Username,
		)
		return
	}

	s.clients[client.Username] = client

	s.mu.Unlock()

	s.broadcast(
		client.Username,
		fmt.Sprintf("User %s joined the chat.", client.Username),
	)
}

// handleMessage broadcasts a message to all other clients.
func (s *Server) handleMessage(client *Client, message string) {
	s.broadcast(
		client.Username,
		fmt.Sprintf("%s: %s", client.Username, message),
	)
}

// handleLeave removes a client from the server.
func (s *Server) handleLeave(client *Client) {
	s.mu.Lock()

	if _, exists := s.clients[client.Username]; !exists {
		s.mu.Unlock()
		return
	}

	delete(s.clients, client.Username)

	close(client.Done)

	s.mu.Unlock()

	s.broadcast(
		client.Username,
		fmt.Sprintf("User %s left the chat.", client.Username),
	)
}

// broadcast sends a message to everyone except sender.
func (s *Server) broadcast(sender string, message string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for username, client := range s.clients {
		if username == sender {
			continue
		}

		client.Incoming <- message
	}
}

// ListUsers returns a sorted list of connected users.
func (s *Server) ListUsers() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	users := make([]string, 0, len(s.clients))

	for username := range s.clients {
		users = append(users, username)
	}

	sort.Strings(users)

	return users
}

// GetClient returns a client by username.
func (s *Server) GetClient(username string) *Client {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.clients[username]
}

// IsConnected checks whether a username is currently connected.
func (s *Server) IsConnected(username string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, exists := s.clients[username]
	return exists
}

// shutdownClients disconnects all clients.
func (s *Server) shutdownClients() {
	s.mu.Lock()

	for username, client := range s.clients {
		close(client.Done)
		delete(s.clients, username)
	}

	s.mu.Unlock()
}

// Shutdown cleanly stops the server.
func (s *Server) Shutdown() {
	select {
	case <-s.quit:
		// Already closed.
	default:
		close(s.quit)
	}

	s.wg.Wait()
}