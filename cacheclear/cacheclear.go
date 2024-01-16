package main

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
)

func main() {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr: "redis-cluster-svc-np:6379",
		//Addr: "redis-svc:6379",
		//Addr: "33.33.33.244:6381",
	})
	//清除存在的
	keys, err := rdb.Keys(ctx, "lora:*").Result()
	if err != nil {
		fmt.Println("Error getting keys:", err)
		return
	}
	for _, key := range keys {
		err := rdb.Del(ctx, key).Err()
		if err != nil {
			fmt.Println("Error deleting key", key, ":", err)
		}
	}
}
