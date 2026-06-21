package main

import (
	"log"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	writeTimeout  = 10 * time.Second
	pongTimeout   = 60 * time.Second
	idelTreshold  = 30 * time.Second
	checkInterval = 10 * time.Second
)

type Hub struct {
	clients   map[*websocket.Conn]*Client
	mutex     sync.RWMutex
	broadcast chan []byte
}

func NewHub() *Hub {
	return &Hub{
		clients:   make(map[*websocket.Conn]*Client),
		broadcast: make(chan []byte),
	}
}

func (h *Hub) Register(conn *websocket.Conn) *Client {
	client := &Client{conn: conn}
	h.mutex.Lock()
	defer h.mutex.Unlock()
	h.clients[conn] = client
	return client
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
			err := client.SetWriteDeadline(time.Now().Add(writeTimeout))
			if err != nil {
				log.Printf("write deadline exceed: %s", err)
				h.UnRegister(client)
			}
			if err := client.WriteMessage(websocket.TextMessage, msg); err != nil {
				log.Printf("writing message: %s", err)
				h.UnRegister(client)
			}
		}
	}
}
