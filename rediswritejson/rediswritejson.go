package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/go-redis/redis/v7"
)

type KV struct {
	Key   string `json:"key"`
	Value string `json:"vaule"`
}

type KVSet struct {
	Key   string   `json:"key"`
	Value []string `json:"value"`
}

var (
	deviceSessionTTL  = time.Hour * 24 * 31
	deviceSessionTTL2 = time.Hour * 24 * 7
)

func main() {

	rdb := redis.NewClient(&redis.Options{
		Addr: "redis-svc:6379",
		DB:   0,
		//Addr: "33.33.33.244:6381",
		//Addr: "localhost:6379",
	})
	err2 := rdb.Ping().Err()
	if err2 != nil {
		log.Fatal(err2)
		fmt.Println("redis连接错误")
	}
	insertValue(rdb, "output1.json", true)
	insertValue(rdb, "output2.json", false)
	fmt.Println("文件内容已经成功逐行写入到 Redis 中")
}

func insertValue(rdb *redis.Client, url string, flag bool) {
	file, err := os.Open(url)
	if err != nil {
		fmt.Println(url, "文件打开错误")
		log.Fatal(err)
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	if flag {
		for {
			var record KV
			if err := decoder.Decode(&record); err != nil {
				break
			}
			err = rdb.Set(record.Key, record.Value, deviceSessionTTL).Err()
			if err != nil {
				fmt.Println("缓存设置出错")
				log.Fatal(err)
			}
		}
	} else {
		pipe := rdb.Pipeline()
		for {
			var record KVSet
			if err := decoder.Decode(&record); err != nil {
				break
			}
			if strings.Contains(record.Key, "devaddr") {
				for _, devE := range record.Value {
					temp, _ := hex.DecodeString(strings.ToLower(devE))
					devByte := ByteToEUI(temp)
					pipe.SAdd(record.Key, devByte[:])
				}
			} else {
				err = pipe.SAdd(record.Key, record.Value).Err()
			}
			if err != nil {
				fmt.Println("数值set插入错误")
				log.Fatal(err)
			}
			if strings.Contains(record.Key, "lora:topo:gw") || strings.Contains(record.Key, "lora:topo:dev") {
				pipe.Expire(record.Key, deviceSessionTTL)
			} else {
				pipe.Expire(record.Key, deviceSessionTTL2)
			}
		}
		_, err = pipe.Exec()
		if err != nil {
			fmt.Println("redis出现错误")
			log.Fatal(err)
		}
	}
	fmt.Println(url, "文件处理完成")
}

type EUI64 [8]byte

func ByteToEUI(args []byte) EUI64 {
	return [8]byte{args[0], args[1], args[2], args[3], args[4], args[5], args[6], args[7]}
}
