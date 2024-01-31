package main

import (
	"batch/common"
	"batch/global"
	"flag"
	"strings"

	log "github.com/sirupsen/logrus"
)

func main() {
	flag.StringVar(&common.RedisHost, "rH", "redis-svc:6379", "-rH=redis-svc:6379")
	flag.StringVar(&common.RedisDBName, "rDB", "0", "-rDB=0")
	flag.StringVar(&common.RedisPwd, "rPD", "", "-rPD=")
	flag.StringVar(&common.REDIS_VERSION, "rV", "", "-rV=4")
	flag.BoolVar(&common.RedisCluster, "rC", false, "-rC=false")
	help := flag.Bool("help", false, "Display help infomation")
	flag.Parse()
	if *help {
		printHelp()
		return
	}
	global.GetRedisClient()
	keys, err := global.Rdb.Scan(0, "lora:*", 1000)
	if err != nil {
		log.Fatalln("Error getting keys:", err)
	}
	for _, key := range keys {
		if strings.Contains(key, common.GwDeviceKey) || strings.Contains(key, common.GwDeviceRouteKey) {
			continue
		}
		_, err := global.Rdb.Del(key)
		if err != nil {
			log.Fatalln("Error deleting key", key, ":", err)
		}
	}
	log.Infoln("Lora终端数据清空完成")
}

func printHelp() {
	log.Infof(`
Usage:
	RedisClear [-rH=127.0.0.1:6379] [-rDB=0] [-password=Auth] [-rC=false] [-rV=4] ....

Options:
	-rH=redisHost                 The redis instance (host:port).
	-rDB=redisDBName              The redis DBName use (0-15)
	-rPD=redisPassword            The redis Password
	-rV=redisVersion              The redis version
	-rC=redisCluster              Is Redis a cluster

Examples:
	$ RedisClear -rH=127.0.0.1
	$ RedisClear -rH=127.0.0.1 -rPD=123445
	$ RedisClear -rH=127.0.0.1 -rPD=123445 -rV=7
	`)
}
