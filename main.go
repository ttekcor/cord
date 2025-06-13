package main

import (
	"fmt"
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

type Client struct {
	conn *websocket.Conn
	send chan map[string]interface{}
}

var (
	waitingClient *Client
	pairMutex     sync.Mutex
)

func signalHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("Upgrade error:", err)
		return
	}
	client := &Client{conn: conn, send: make(chan map[string]interface{}, 10)}

	var peer *Client
	var role string

	pairMutex.Lock()
	if waitingClient == nil {
		waitingClient = client
		role = "initiator"
		pairMutex.Unlock()
		// Ждём второго клиента
		for {
			_, _, err := conn.NextReader()
			if err != nil {
				pairMutex.Lock()
				if waitingClient == client {
					waitingClient = nil
				}
				pairMutex.Unlock()
				conn.Close()
				return
			}
		}
	} else {
		peer = waitingClient
		waitingClient = nil
		role = "receiver"
		pairMutex.Unlock()
	}

	// Сообщаем клиенту его роль
	conn.WriteJSON(map[string]interface{}{ "type": "role", "role": role })

	// Канал для пересылки сообщений между клиентами
	go func() {
		for msg := range client.send {
			conn.WriteJSON(msg)
		}
	}()

	// Чтение сообщений и пересылка другому клиенту
	for {
		var msg map[string]interface{}
		err := conn.ReadJSON(&msg)
		if err != nil {
			conn.Close()
			if peer != nil {
				peer.conn.Close()
			}
			return
		}
		if peer != nil {
			peer.send <- msg
		}
	}
}

func main() {
	http.HandleFunc("/signal", signalHandler)
	http.Handle("/", http.FileServer(http.Dir("static")))
	fmt.Println("Сервер запущен на :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
} 