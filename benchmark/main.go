package benchmark

import (
	"fmt"
	"shared"
)

func main() {
	config := shared.LoadConfig()
	// Use the config...
	fmt.Println(config)
}
