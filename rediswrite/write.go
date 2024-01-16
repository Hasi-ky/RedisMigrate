package main

import (
	"bufio"
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
)

func main() {
	var (
		deviceSessionTTL  = time.Hour * 24 * 31
		deviceSessionTTL2 = time.Hour * 24 * 7
		ctx               = context.Background()
	)

	rdb := redis.NewClient(&redis.Options{
		Addr: "redis-svc:6379",
		//Addr: "33.33.33.244:6381",
		//清除存在的
	})
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
	
	file, err := os.Open("output.txt")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := strings.Split(scanner.Text(), "|#|")
		switch line[2] {
		case "string":
			fmt.Println(line[0], "值为：", line[1])
			tempValue, _ := hex.DecodeString(line[1])
			err2 := rdb.Set(context.Background(), line[0], string(tempValue), deviceSessionTTL).Err()
			if err2 != nil {
				fmt.Println(err2)
			}
		case "set":
			pipe := rdb.Pipeline()
			var err1 error
			fmt.Println(line[0], "值为：", line[1])
			tempValue, _ := hex.DecodeString(line[1])
			strGroup := strings.Split(string(tempValue), "+-")
			for _, v := range strGroup {
				pipe.SAdd(ctx, line[0], v)
			}
			if strings.HasPrefix(line[0], "lora:topo:gw") || strings.HasPrefix(line[0], "lora:topo:dev") {
				err1 = pipe.PExpire(ctx, line[0], deviceSessionTTL2).Err()
			} else if strings.HasPrefix(line[0], "lora:ns:devaddr") || strings.HasPrefix(line[0], "lora:ns:topo") {
				err1 = pipe.PExpire(ctx, line[0], deviceSessionTTL).Err()
			}
			if err1 != nil {
				fmt.Println(err1)
				continue
			}
			pipe.Exec(ctx)
		default:
		}
		if err != nil {
			panic(err)
		}
	}
	if err := scanner.Err(); err != nil {
		panic(err)
	}
	fmt.Println("文件内容已经成功逐行写入到 Redis 中")
}
