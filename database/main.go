package database

import (
	"shared"
)

func main() {
	config := shared.LoadConfig()
	node := config.Database
	ip := node.IP
	port := node.Port
	var db DataBase
	db.init(ip, port)
	db.StartListening()
}
