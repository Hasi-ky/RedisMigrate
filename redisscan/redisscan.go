package main

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

type MyScanStruct struct {
	Key       string
	Cursor    uint64
	Match     string
	ResultCap int
	Count     int64
}

func main() {
	tt := MyScanStruct{
		Cursor: uint64(0),
		Match:  "devhis:%s*",
		Count:  10000,
	}
	ctx := context.Background()
	rdb := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: []string{"redis-svc:6379"},
		//Addr: "33.33.33.244:6381",
	})

	//清除存在的
	for {
		keys, cur := rdb.Scan(ctx, tt.Cursor, tt.Match, tt.Count).Val()
		for _, key := range keys {
			fmt.Println(key)
			m := rdb.HGetAll(context.TODO(), key).Val()
			for k, v := range m {
				fmt.Println("key值为:", k, "value值为:", v)
			}
		}
		tt.Cursor = cur
		if tt.Cursor == 0 {
			fmt.Println("扫描结束")
			break
		}
	}
}
