package cache

import (
	"flag"
	"fmt"
	_ "github.com/bradfitz/gomemcache/memcache"
	"os"
	"shared"
)

func main() {
	var cacheId string
	var help bool
	flag.BoolVar(&help, "help", false, "Display usage")
	flag.StringVar(&cacheId, "id", "", "Cache ID (an integer 1 to 5)")

	flag.Parse()

	if cacheId == "" || help == true {
		fmt.Println("Usage: <program> [-help] -id <cache_id>")
		flag.PrintDefaults()
		os.Exit(1)
	}

	config := shared.LoadConfig()

	node := config.Cache[cacheId]
	ip := node.IP
	port := node.Port
	die := node.Die

	fmt.Printf("Cache node %s listening on %s:%s\n", cacheId, ip, port)

	// create cache node instance
	var cache Cache
	cache.init(ip, port, die)
	cache.startListening()

}
