package main

import (
	"fmt"
	"log"
	"net/http"

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
	hub.Register(conn)
	go handleConnection(hub, conn)
}

func handleConnection(h *Hub, conn *websocket.Conn) {
	defer h.UnRegister(conn)
	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			log.Printf("reading message: %s", err)
			return
		}
		h.Broadcast(msg)
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
