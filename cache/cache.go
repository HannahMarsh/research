package cache

import (
	"encoding/gob"
	"fmt"
	"github.com/bradfitz/gomemcache/memcache"
	_ "github.com/bradfitz/gomemcache/memcache"
	"io"
	"log"
	"net"
	"runtime/debug"
	"shared"
	"time"
)

type Cache struct {
	mc    *memcache.Client
	ip    string
	port  string
	start time.Time
	die   []shared.DieInfo
}

func (c *Cache) init(ip string, port string, die []shared.DieInfo) {
	c.mc = memcache.New("localhost:11211") // todo, change this to the address of the memcache server
	c.ip = ip
	c.port = port
	c.die = die
	gob.Register(shared.Request{})
}

func (c *Cache) startListening() {
	listener, err := net.Listen("tcp", ":"+c.port)
	if err != nil {
		log.Fatalf("Error setting up listener: %v: %s\n", err, debug.Stack())
	}
	defer func(listener net.Listener) {
		err := listener.Close()
		if err != nil {
			fmt.Println("Error closing connection:", err)
		}
	}(listener)

	sem := make(chan struct{}, 100000)
	c.start = time.Now()

	for {
		if !c.isDead() {
			conn, err := listener.Accept()
			if err != nil {
				log.Printf("Error accepting connection: %v", err)
				continue
			}

			sem <- struct{}{}
			go c.handleRequest(conn)
		}
	}

}

func (c *Cache) isDead() bool {
	currentTime := time.Since(c.start)

	for _, dieInfo := range c.die {
		// Convert dieInfo.Time and dieInfo.Duration to time.Duration
		dieTime := time.Duration(dieInfo.Time) * time.Second
		duration := time.Duration(dieInfo.Duration) * time.Second

		// Check if currentTime is within the 'dead' interval
		if currentTime >= dieTime && currentTime < dieTime+duration {
			return true // Cache is currently 'dead'
		}
	}

	return false // Cache is not 'dead' at the current time
}

func (c *Cache) handleRequest(conn net.Conn) {

	defer func(conn net.Conn) {
		err := conn.Close()
		if err != nil {
			fmt.Println("Error closing connection:", err)
		}
	}(conn)

	decoder := gob.NewDecoder(conn)
	encoder := gob.NewEncoder(conn)

	for {
		// Define a variable to store the decoded message
		var rq shared.Request
		if err := decoder.Decode(&rq); err != nil {
			if err == io.EOF {
				fmt.Println("Client closed the connection")
				break // Exit the loop and end the goroutine
			} else {
				fmt.Printf("Error decoding message: %s: \n %s\n", err, debug.Stack())
				break
			}
		} else {
			item := c.handleCacheRequest(rq)
			if item != nil {
				if err := encoder.Encode(&item); err != nil {
					fmt.Printf("Error encoding item: %s\n%s\n", err, debug.Stack())
				}
			} else {
				if err := encoder.Encode(""); err != nil {
					fmt.Printf("Error encoding response: %s\n%s\n", err, debug.Stack())
				}
			}
		}
	}
}

func (c *Cache) handleCacheRequest(rq shared.Request) *memcache.Item {
	if rq.Method == "GET" {
		item, err := c.mc.Get(rq.Key)
		if err != nil {
			log.Println("Error getting item:", err)
		}
		return item
		//if item != nil {
		//	fmt.Println("Value:", string(item.Value))
		//}
	} else if rq.Method == "SET" {
		// Set a value
		err := c.mc.Set(&memcache.Item{Key: rq.Key, Value: []byte(rq.Value)})
		if err != nil {
			log.Println("Error setting item:", err)
		}
	}
	return nil
}
