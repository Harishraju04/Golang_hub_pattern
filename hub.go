package main

import (
	"log"
	"sync"

	"github.com/gorilla/websocket"
)

type Hub struct {
	clients   map[*websocket.Conn]bool
	mutex     sync.RWMutex
	broadcast chan []byte
}

func NewHub() *Hub {
	return &Hub{
		clients:   make(map[*websocket.Conn]bool),
		broadcast: make(chan []byte),
	}
}

func (h *Hub) Register(conn *websocket.Conn) {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	h.clients[conn] = true
}

func (h *Hub) UnRegister(conn *websocket.Conn) {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	if _, ok := h.clients[conn]; ok {
		delete(h.clients, conn)
		conn.Close()
	}
}

func (h *Hub) Broadcast(message []byte) {
	h.broadcast <- message
}

func (h *Hub) getSnapshot() []*websocket.Conn {
	h.mutex.RLock()
	snapshot := make([]*websocket.Conn, 0, len(h.clients))
	defer h.mutex.RUnlock()
	for client, _ := range h.clients {
		snapshot = append(snapshot, client)
	}
	return snapshot
}

func (h *Hub) Run() {
	for msg := range h.broadcast {
		clients := h.getSnapshot()
		for _, client := range clients {
			if err := client.WriteMessage(websocket.TextMessage, msg); err != nil {
				log.Printf("writing message: %s", err)
				h.UnRegister(client)
			}
		}
	}
}
