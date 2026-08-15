package main

import (
	"fmt"
	"sync"
)

type Client struct {
	Username string
	Incoming chan string
	Done     chan struct{}
}

func NewClient(username string) *Client {
	return &Client{
		Username: username,
		Incoming: make(chan string),
		Done:     make(chan struct{}),
	}
}

// Listen waits for messages sent by the server.
// Every client runs this function in its own goroutine.
func (c *Client) Listen(wg *sync.WaitGroup, terminalMu *sync.Mutex) {
	defer wg.Done()

	for {
		select {
		case message := <-c.Incoming:
			terminalMu.Lock()
			fmt.Printf("\n[%s] %s\n", c.Username, message)
			terminalMu.Unlock()

		case <-c.Done:
			return
		}
	}
}