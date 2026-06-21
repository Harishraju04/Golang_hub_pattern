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
	hub.Register(conn)
	go handleConnection(hub, conn)
}

func handleConnection(h *Hub, conn *websocket.Conn) {
	defer h.UnRegister(conn)
	err := conn.SetReadDeadline(time.Now().Add(pongTimeout))
	if err != nil {
		log.Printf("Readlind exceed: %s", err)
		return
	}
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(pongTimeout))
		return nil
	})

	go pingLoop(h, conn)

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			log.Printf("reading message: %s", err)
			return
		}
		h.Broadcast(msg)
	}
}

func pingLoop(h *Hub, conn *websocket.Conn) {
	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()
	for range ticker.C {
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
