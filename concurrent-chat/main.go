package main

import (
	"bufio"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
)

var terminalMu sync.Mutex

var clientWG sync.WaitGroup

func main() {
	server := NewServer()
	server.Run()

	// Handle Ctrl+C.
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)

	shutdownChan := make(chan struct{})
	var shutdownOnce sync.Once

	shutdown := func() {
		shutdownOnce.Do(func() {
			close(shutdownChan)
		})
	}

	go func() {
		<-signalChan

		terminalMu.Lock()
		fmt.Println("\n\nCtrl+C detected. Shutting down...")
		terminalMu.Unlock()

		shutdown()
	}()

	reader := bufio.NewReader(os.Stdin)

	var activeUser *Client

	for {
		select {
		case <-shutdownChan:
			cleanShutdown(server, activeUser)
			return

		default:
			// Continue with terminal menu.
		}

		printMenu(activeUser)

		choice, err := readLine(reader)

		if err != nil {
			fmt.Println("\nInput closed. Shutting down...")
			cleanShutdown(server, activeUser)
			return
		}

		switch choice {
		case "1":
			client := createUser(reader, server)

			if client != nil {
				activeUser = client
			}

		case "2":
			listUsers(server)

		case "3":
			activeUser = selectUser(reader, server, activeUser)

		case "4":
			sendMessage(reader, server, activeUser)

		case "5":
			removed := removeUser(reader, server, activeUser)

			if removed {
				activeUser = nil
			}

		case "6":
			terminalMu.Lock()
			fmt.Println("\nShutting down...")
			terminalMu.Unlock()

			cleanShutdown(server, activeUser)
			return

		default:
			terminalMu.Lock()
			fmt.Println("\nInvalid option. Please choose 1-6.")
			terminalMu.Unlock()
		}

		fmt.Println()
	}
}

// printMenu displays the main menu.
func printMenu(activeUser *Client) {
	terminalMu.Lock()
	defer terminalMu.Unlock()

	fmt.Println("\n================================")
	fmt.Println("       CONCURRENT CHAT")
	fmt.Println("================================")

	if activeUser != nil {
		fmt.Printf("Current user: %s\n", activeUser.Username)
	} else {
		fmt.Println("Current user: None")
	}

	fmt.Println("--------------------------------")
	fmt.Println("1. Create user")
	fmt.Println("2. List connected users")
	fmt.Println("3. Select user")
	fmt.Println("4. Send message")
	fmt.Println("5. Remove user")
	fmt.Println("6. Exit")
	fmt.Println("--------------------------------")
	fmt.Print("Choose an option: ")
}

// readLine reads a line from the terminal.
func readLine(reader *bufio.Reader) (string, error) {
	input, err := reader.ReadString('\n')

	if err != nil {
		return "", err
	}

	return strings.TrimSpace(input), nil
}

// createUser creates a new chat client.
func createUser(reader *bufio.Reader, server *Server) *Client {
	terminalMu.Lock()
	fmt.Print("\nEnter username: ")
	terminalMu.Unlock()

	username, err := readLine(reader)

	if err != nil {
		return nil
	}

	if username == "" {
		terminalMu.Lock()
		fmt.Println("Username cannot be empty.")
		terminalMu.Unlock()
		return nil
	}

	if server.IsConnected(username) {
		terminalMu.Lock()
		fmt.Printf("Username '%s' is already taken.\n", username)
		terminalMu.Unlock()
		return nil
	}

	client := NewClient(username)

	// Start the client's listener goroutine.
	clientWG.Add(1)
	go client.Listen(&clientWG, &terminalMu)

	// Send join event to the server.
	server.events <- Event{
		Type:   JoinEvent,
		Client: client,
	}

	terminalMu.Lock()
	fmt.Printf("User '%s' joined the chat.\n", username)
	terminalMu.Unlock()

	return client
}

// listUsers displays all connected users.
func listUsers(server *Server) {
	users := server.ListUsers()

	terminalMu.Lock()
	defer terminalMu.Unlock()

	fmt.Println("\nConnected users:")

	if len(users) == 0 {
		fmt.Println("No users are currently connected.")
		return
	}

	for i, username := range users {
		fmt.Printf("%d. %s\n", i+1, username)
	}
}

// selectUser lets the user choose who is currently active.
func selectUser(
	reader *bufio.Reader,
	server *Server,
	current *Client,
) *Client {

	users := server.ListUsers()

	if len(users) == 0 {
		terminalMu.Lock()
		fmt.Println("\nNo users are connected.")
		terminalMu.Unlock()
		return current
	}

	terminalMu.Lock()
	fmt.Println("\nSelect a user:")

	for i, username := range users {
		fmt.Printf("%d. %s\n", i+1, username)
	}

	fmt.Print("Enter number: ")
	terminalMu.Unlock()

	input, err := readLine(reader)

	if err != nil {
		return current
	}

	number, err := strconv.Atoi(input)

	if err != nil || number < 1 || number > len(users) {
		terminalMu.Lock()
		fmt.Println("Invalid user selection.")
		terminalMu.Unlock()
		return current
	}

	username := users[number-1]
	client := server.GetClient(username)

	if client == nil {
		terminalMu.Lock()
		fmt.Println("That user is no longer connected.")
		terminalMu.Unlock()
		return current
	}

	terminalMu.Lock()
	fmt.Printf("Current user is now '%s'.\n", username)
	terminalMu.Unlock()

	return client
}

// sendMessage sends a message as the selected user.
func sendMessage(
	reader *bufio.Reader,
	server *Server,
	activeUser *Client,
) {
	if activeUser == nil {
		terminalMu.Lock()
		fmt.Println("\nNo user selected.")
		terminalMu.Unlock()
		return
	}

	// Make sure the selected user still exists.
	if !server.IsConnected(activeUser.Username) {
		terminalMu.Lock()
		fmt.Printf(
			"\nUser '%s' is no longer connected.\n",
			activeUser.Username,
		)
		terminalMu.Unlock()
		return
	}

	terminalMu.Lock()
	fmt.Printf("\nMessage as %s: ", activeUser.Username)
	terminalMu.Unlock()

	message, err := readLine(reader)

	if err != nil {
		return
	}

	if message == "" {
		terminalMu.Lock()
		fmt.Println("Message cannot be empty.")
		terminalMu.Unlock()
		return
	}

	// Send message event to server.
	server.events <- Event{
		Type:    MessageEvent,
		Client:  activeUser,
		Message: message,
	}
}

// removeUser removes the selected user.
func removeUser(
	reader *bufio.Reader,
	server *Server,
	activeUser *Client,
) bool {
	users := server.ListUsers()

	if len(users) == 0 {
		terminalMu.Lock()
		fmt.Println("\nNo users are connected.")
		terminalMu.Unlock()
		return false
	}

	terminalMu.Lock()
	fmt.Println("\nSelect a user to remove:")

	for i, username := range users {
		fmt.Printf("%d. %s\n", i+1, username)
	}

	fmt.Print("Enter number: ")
	terminalMu.Unlock()

	input, err := readLine(reader)

	if err != nil {
		return false
	}

	number, err := strconv.Atoi(input)

	if err != nil || number < 1 || number > len(users) {
		terminalMu.Lock()
		fmt.Println("Invalid user selection.")
		terminalMu.Unlock()
		return false
	}

	username := users[number-1]
	client := server.GetClient(username)

	if client == nil {
		terminalMu.Lock()
		fmt.Println("That user is no longer connected.")
		terminalMu.Unlock()
		return false
	}

	// Send leave event to server.
	server.events <- Event{
		Type:   LeaveEvent,
		Client: client,
	}

	terminalMu.Lock()
	fmt.Printf("User '%s' was removed.\n", username)
	terminalMu.Unlock()

	return activeUser != nil && activeUser.Username == username
}

// cleanShutdown shuts down the server and waits for client goroutines.
func cleanShutdown(server *Server, activeUser *Client) {
	// Tell the server to shut down.
	select {
	case server.events <- Event{Type: ShutdownEvent}:
	default:
	}

	// Shut down the server.
	server.Shutdown()

	// Wait for all client listener goroutines.
	clientWG.Wait()

	terminalMu.Lock()
	fmt.Println("All clients disconnected.")
	fmt.Println("Server stopped.")
	fmt.Println("Goodbye!")
	terminalMu.Unlock()
}
