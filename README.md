# Concurrent Chat

A simple **concurrent chat application written in Go** that demonstrates how to use goroutines, channels, mutexes, and wait groups to manage multiple chat clients.

## Features

* Create and manage multiple users.
* List connected users.
* Select an active user.
* Send messages between users.
* Remove users from the chat.
* Concurrent message listening using goroutines.
* Event-based server communication.
* Graceful shutdown with `Ctrl+C`.

## Project Structure

```text
.
├── main.go
├── server.go
├── client.go
└── go.mod
```

### `main.go`

Handles the terminal menu, user input, creating/removing users, sending messages, and graceful shutdown.

### `server.go`

Manages connected clients and processes `JoinEvent`, `MessageEvent`, `LeaveEvent`, and `ShutdownEvent`.

### `client.go`

Defines the `Client` and listens for incoming messages using a goroutine.

## How to Run

Make sure Go is installed, then run:

```bash
go run .
```

The application will display a menu:

```text
1. Create user
2. List connected users
3. Select user
4. Send message
5. Remove user
6. Exit
```

## Concurrency

The project demonstrates several Go concurrency concepts:

* **Goroutines** — each client can run its `Listen` function concurrently.
* **Channels** — used for communication between the server and clients.
* **`select`** — waits for incoming messages or shutdown signals.
* **`sync.Mutex`** — protects shared client data and terminal output.
* **`sync.WaitGroup`** — waits for goroutines to finish during shutdown.

The server processes events through its `events` channel and dispatches them according to their type.

## Example

Create two users:

```text
Ahmed
Mohamed
```

Select `Ahmed` and send:

```text
Hello Mohamed
```

The server broadcasts the message to the other connected clients through their `Incoming` channels.

## Technologies

* **Go**
* Goroutines
* Channels
* `sync.Mutex`
* `sync.WaitGroup`
* Event-driven communication
