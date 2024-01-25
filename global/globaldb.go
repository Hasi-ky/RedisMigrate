package global

import (
	"batch/common"
	"batch/db/redis"
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"
	log "github.com/sirupsen/logrus"
)

//全局导向
var Rdb redis.DBClient

var Sdb *sqlx.DB

func GetRedisClient() {
	hostAndPort := strings.Split(common.RedisHost, ":")
	var err error
	if common.RedisCluster {
		servers := []string{common.RedisHost}
		Rdb, err = redis.NewClusterClient(servers, common.RedisPwd)
	} else {
		Rdb, err = redis.NewClient(hostAndPort[0], hostAndPort[1], common.RedisPwd)
	}
	if err != nil {
		log.Fatal("redis client 创建失败")
	}
	pingRes := Rdb.Ping()
	if !pingRes {
		log.Fatal("redis连接失败")
	}
	log.Infoln("redis连接成功")
}


func GetPsqlClient() {
	var err error
	psqlInfo := fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable",
		common.PsqlUser, common.PsqlPwd, common.PsqlHost, common.PsqlDBName)
	Sdb, err = sqlx.Open("postgres", psqlInfo)
	if err != nil {
		log.Fatal("获取数据库连接失败!", err)
	}
	log.Infoln("获取数据库连接成功!")
}