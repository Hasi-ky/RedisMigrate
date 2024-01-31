package main

import (
	"batch/common"
	"batch/global"
	"fmt"
)

func main() {
	common.RedisHost = "localhost:6379"
	global.GetRedisClient()
	_, err := global.Rdb.Del("jianlai")
	fmt.Println(err)
}
