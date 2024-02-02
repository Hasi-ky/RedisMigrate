package redis

import "time"

type DBClient interface {
	CloseSession() error
	Ping() bool
	Rpush(key string, data []byte) (int, error)
	Lpop(key string) (string, error)
	Lpush(key string, data []byte) (int, error)
	Del(key string) (int, error)
	Lrem(key string, count int, value string) (int, error)
	Lindex(key string, start int) (string, error)
	Llen(key string) (int, error)
	Lrange(key string, start int, stop int) ([]string, error)
	Lset(key string, index int, value []byte) (string, error)
	Sadd(key string, member interface{}) (int, error)
	Srem(key string, member string) (int, error)
	Smembers(key string) ([]string, error)
	Set(key string, value interface{}, expiration time.Duration) (string, error)
	Get(key string) (string, error)
	Incr(transferKey string) (int, error)
	Scan(cursor uint64, match string, count int64) ([]string, error)
	HGet(key, field string) ([]byte, error)
	HSet(key, field string, value []byte) error
	HGetAll(key string) (map[string]string, error)
	TTL(key string) (time.Duration, error)
	Expire(key string, expiration time.Duration) error
	TxPipeline() interface{}
	HExists(key, field string) (bool, error)
	Dump(key string) (string, error)
	RestoreReplace(key string, ttl time.Duration, field string) (string, error)
}
