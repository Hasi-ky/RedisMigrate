package main

import (
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"github.com/go-redis/redis/v7"
)

func main() {
	client := redis.NewClient(&redis.Options{
		//Addr:     "redis-svc:6379",
		Addr:     "redis-cluster-svc-np:6379",
		Password: "",
		DB:       0,
	})
	pong, err := client.Ping().Result()
	fmt.Println(pong, err)
	defer client.Close()
	file, err := os.Create("output.txt")
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer file.Close()
	var cursor uint64 = 0
	for {
		keys, cur, err := client.Scan(cursor, "*lora*", 10).Result()
		if err != nil {
			fmt.Println("Error during SCAN:", err)
			break
		}
		cursor = cur
		for _, key := range keys {
			keyType, err := client.Type(key).Result()
			if err != nil {
				fmt.Printf("Error getting type for key %s: %v\n", key, err)
				continue
			}
			var val interface{}
			switch keyType {
			case "string":
				val, err = client.Get(key).Result()
			case "list":
				val, err = client.LRange(key, 0, -1).Result()
			case "set":
				val, err = client.SMembers(key).Result()
			case "hash":
				val, err = client.HGetAll(key).Result()
			case "zset":
				val, err = client.ZRangeWithScores(key, 0, -1).Result()
			}
			if err != nil {
				fmt.Printf("Error getting value for key %s: %v\n", key, err)
				continue
			}
			if v, ok := val.(string); ok {
				hexString := hex.EncodeToString([]byte(v))
				str := key + "|#|" + hexString + "|#|" + keyType + "\n"
				file.WriteString(str)
			} else if v, ok := val.([]string); ok {
				s := strings.Join(v, "+-")
				hexString := hex.EncodeToString([]byte(s))
				file.WriteString(key + "|#|" + hexString + "|#|" + keyType + "\n")
			}
		}
		if cursor == 0 {
			break
		}
	}
}
