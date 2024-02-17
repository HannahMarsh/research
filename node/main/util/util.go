package util

import (
	"fmt"
	"strings"
)

func MapToString(m map[string]int) string {
	var b strings.Builder
	for key, value := range m {
		b.WriteString(fmt.Sprintf("%s: %d, ", key, value))
	}
	// Remove the last comma and space if the map is not empty
	if b.Len() > 0 {
		return b.String()[:b.Len()-2]
	}
	return b.String()
}
