package main

import (
	"os"

	"github.com/go-redis/redis/v7"
)

func main() {
	rdb := redis.NewClient(&redis.Options{
		//Addr: "redis-svc:6379",
		Addr: "localhost:6379",
		//清除存在的
	})
	rdb.Ping()
	filePath := "serialized.json"
	file1, _ := os.Create(filePath)
	s, _ := rdb.Dump("lora:ns:devaddr:00000001").Result()
	file1.WriteString(s)
}
