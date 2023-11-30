package db

import (
	"shared"
)

func main() {
	config := shared.LoadConfig()
	node := config.Database
	ip := node.IP
	port := node.Port
	var db DataBase
	db.init(ip, port, "keyspace", "localhost") // jdbc:cassandra://localhost:9042
	db.StartListening()
}
