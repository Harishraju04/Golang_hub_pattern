package main

import (
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func wsHandler(hub *Hub, w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("upgrading request: %s", err)
		return
	}
	client := hub.Register(conn)
	go handleConnection(hub, client)
}

func handleConnection(h *Hub, client *Client) {
	conn := client.conn
	defer h.UnRegister(conn)
	if err := conn.SetReadDeadline(time.Now().Add(pongTimeout)); err != nil {
		log.Printf("Read deadline exceed: %s", err)
		return
	}
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(pongTimeout))
		return nil
	})

	go pingLoop(h, client)

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			log.Printf("reading message: %s", err)
			return
		}
		client.updateLastMessage()
		conn.SetReadDeadline(time.Now().Add(pongTimeout))
		h.Broadcast(msg)
	}
}

func pingLoop(h *Hub, client *Client) {
	ticker := time.NewTicker(checkInterval)
	conn := client.conn
	defer ticker.Stop()
	for range ticker.C {
		if !client.isIdle(idelTreshold) {
			continue
		}
		err := conn.SetWriteDeadline(time.Now().Add(writeTimeout))
		if err != nil {
			log.Printf("ping write timeout: ;%s", err)
			h.UnRegister(conn)
			return
		}
		if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
			log.Printf("writeing ping: %s ", err)
			h.UnRegister(conn)
			return
		}
	}
}

func main() {
	hub := NewHub()
	go hub.Run()

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		wsHandler(hub, w, r)
	})

	if err := http.ListenAndServe(":8080", nil); err != nil {
		fmt.Println("error starting server:", err)
	}
}
