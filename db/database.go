package db

import (
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"runtime/debug"
	"shared"
)

/**
 * Handles incoming TCP connections, reads messages, interprets the operation type (PUT, GET, DELETE),
 * calls the appropriate operation on the KV store, and writes back the result.
 */

type DataBase struct {
	kv   *KVStore
	ip   string
	port string
}

func (db *DataBase) init(ip string, port string, keyspace string, cassandraHosts ...string) {
	db.kv = NewKVStore(keyspace, cassandraHosts...)
	gob.Register(shared.DbRequest{})
	db.ip = ip
	db.port = port
}

func (db *DataBase) StartListening() {
	listener, err := net.Listen("tcp", ":"+db.port)
	if err != nil {
		log.Fatalf("Error setting up listener: %v: %s\n", err, debug.Stack())
	}
	defer func(listener net.Listener) {
		err := listener.Close()
		if err != nil {
			log.Printf("Error closing listener: %v: %s\n", err, debug.Stack())
		}
	}(listener)

	sem := make(chan struct{}, 100000)

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Printf("Error accepting connection: %v\n%s\n", err, debug.Stack())
			log.Printf("Error accepting connection: %v", err)
			continue
		}

		sem <- struct{}{}
		go db.handleConnection(conn, sem)
	}

}

func (db *DataBase) handleConnection(conn net.Conn, sem chan struct{}) {
	defer func() {
		<-sem
		err := conn.Close()
		if err != nil {
			return
		}
	}()

	db.HandleConnection(conn)
}

func (db *DataBase) HandleConnection(conn net.Conn) {
	defer func(conn net.Conn) {
		err := conn.Close()
		if err != nil {

		}
	}(conn)

	// Create a decoder that reads from the connection
	decoder := gob.NewDecoder(conn)

	// Create an encoder that writes to the connection
	encoder := gob.NewEncoder(conn)

	for {
		// Define a variable to store the decoded message
		var msg shared.DbRequest
		// Decode a message from the connection
		// The decoded data will be stored in 'msg'
		if err := decoder.Decode(&msg); err != nil {
			fmt.Printf("Error decoding message: %s: \n %s\n", err, debug.Stack())
		} else {
			// At this point, 'msg' contains the received message
			// Now, handle this message and prepare a response
			response := db.handlebarDbRequest(&msg)

			// Then, you'd send the response back to the client
			if err := encoder.Encode(&response); err != nil {
				fmt.Printf("Error encoding response: %s\n%s\n", err, debug.Stack())
			}
		}
	}
}

func (db *DataBase) handlebarDbRequest(msg *shared.DbRequest) *shared.DbRequest {
	var response shared.DbRequest
	switch msg.Command {
	case "PUT":
		db.kv.Put(msg.Key, msg.Value)
		response = shared.DbRequest{Command: "RESPONSE", Value: "OK"}
	case "GET":
		value, exists := db.kv.Get(msg.Key)
		if !exists {
			response = shared.DbRequest{Command: "RESPONSE", Value: ""}
		} else {
			response = shared.DbRequest{Command: "RESPONSE", Value: value}
		}
	case "DELETE":
		db.kv.Delete(msg.Key)
		response = shared.DbRequest{Command: "RESPONSE", Value: "OK"}
	default:
		log.Printf("Unknown command: %s", msg.Command)
		response = shared.DbRequest{Command: "RESPONSE", Value: "ERROR"}
	}
	return &response
}
