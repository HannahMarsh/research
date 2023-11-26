package benchmark

import (
	"./shared"
	"fmt"
)

func main() {
	config := shared.LoadConfig()
	// Use the config...
	fmt.Println(config)
}
