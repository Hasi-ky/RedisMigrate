package redis

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
	Sadd(key string, member string) (int, error)
	Srem(key string, member string) (int, error)
	Smembers(key string) ([]string, error)
	Set(key string, value string) (string, error)
	Get(key string) (string, error)
	Incr(transferKey string) (int, error)
	Scan(cursor uint64, match string, count int64) ([]string, uint64, error)
	HGet(key, field string) ([]byte, error)
	HSet(key, field string) error
}
