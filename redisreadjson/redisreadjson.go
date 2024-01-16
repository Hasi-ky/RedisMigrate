package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/go-redis/redis/v7"
)

type KV struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type KVSet struct {
	Key   string   `json:"key"`
	Value []string `json:"value"`
}

func main() {
	client := redis.NewClient(&redis.Options{
		//Addr:     "redis-svc:6379",
		Addr:     "redis-cluster-svc-np:6379",
		Password: "",
		DB:       0,
	})
	 
	_, err := client.Ping().Result()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("redis连接成功")
	defer client.Close()
	file1, err3 := os.Create("output1.json")
	file2, err4 := os.Create("output2.json")
	if err3 != nil || err4 != nil {
		fmt.Println("Error:", err)
		return
	}
	defer file1.Close()
	defer file2.Close()
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
				tempKv := KV{
					Key:   key,
					Value: v,
				}
				jsonData, _ := json.Marshal(tempKv)
				file1.Write(jsonData)
			} else if v, ok := val.([]string); ok {
				// tempValue := make([]string, 0)
				// for _, record := range v {
				// 	str := hex.EncodeToString([]byte(record))
				// 	tempValue = append(tempValue, str)
				// }
				tempKv := KVSet{
					Key:   key,
					Value: v,
				}
				jsonData, _ := json.Marshal(tempKv)
				file2.Write(jsonData)
			}
		}
		if cursor == 0 {
			break
		}
	}
}
