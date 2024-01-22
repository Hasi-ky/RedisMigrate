package global

import (
	"batch/db/redis"
	"github.com/jmoiron/sqlx"
)

//全局导向
var Rdb redis.DBClient


var Sdb *sqlx.DB