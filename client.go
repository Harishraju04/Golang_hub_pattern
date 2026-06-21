package main

import (
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type Client struct {
	conn        *websocket.Conn
	lastMessage time.Time
	mutex       sync.Mutex
}

func NewClient(conn *websocket.Conn) *Client {
	return &Client{
		conn:        conn,
		lastMessage: time.Now(),
	}
}

func (c *Client) updateLastMessage() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.lastMessage = time.Now()
}

func (c *Client) isIdle(threshold time.Duration) bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return time.Since(c.lastMessage) > threshold
}
