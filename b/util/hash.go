// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package util

import (
	"encoding/binary"
	"github.com/spaolacci/murmur3"
	"hash/fnv"
)

// Hash64 returns a fnv Hash of the integer.
func Hash64(n int64) int64 {
	var b [8]byte
	binary.BigEndian.PutUint64(b[0:8], uint64(n))
	hash := fnv.New64a()
	hash.Write(b[0:8])
	result := int64(hash.Sum64())
	if result < 0 {
		return -result
	}
	return result
}

func StringHash(key string) int {
	// Using MurmurHash to compute the hash
	hash := murmur3.New32() // create a new 32-bit MurmurHash3 hash
	if _, err := hash.Write([]byte(key)); err != nil {
		panic(err)
	}
	return int(hash.Sum32())
}

// BytesHash64 returns the fnv hash of a bytes
func BytesHash64(b []byte) int64 {
	hash := fnv.New64a()
	hash.Write(b)
	return int64(hash.Sum64())
}

// StringHash64 returns the fnv hash of a string
func StringHash64(s string) int64 {
	hash := fnv.New64a()
	hash.Write(Slice(s))
	return int64(hash.Sum64())
}

// CreateArray creates an array of n ints from 0 to n-1.
func CreateArray(n int) []int {
	arr := make([]int, n)
	for i := 0; i < n; i++ {
		arr[i] = i
	}
	return arr
}
