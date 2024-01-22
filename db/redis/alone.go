package redis

import (
	"context"
	"errors"

	redis7 "github.com/go-redis/redis/v7"
	redis9 "github.com/redis/go-redis/v9"
)

//CloseSession CloseSession
func (rc *Client) CloseSession() error {
	if rc != nil && rc.Client != nil {
		if r, ok := rc.Client.(*redis7.Client); ok {
			return r.Close()
		}
	}
	return rc.Client.(*redis9.Client).Close()
}

//Ping Ping
func (rc *Client) Ping() bool {
	if rc == nil || rc.Client == nil {
		return false
	}
	var err error
	if r, ok := rc.Client.(*redis7.Client); ok {
		err = r.Ping().Err()
	} else {
		err = rc.Client.(*redis9.Client).Ping(context.TODO()).Err()
	}
	return err == nil
}

//Rpush Rpush
func (rc *Client) Rpush(key string, data []byte) (int, error) {
	if rc == nil || rc.Client == nil {
		return 0, errors.New("redis client is disconnect")
	}
	var (
		reply int64
		err   error
	)
	if r, ok := rc.Client.(*redis7.Client); ok {
		reply, err = r.RPush(key, data).Result()
	} else {
		reply, err = rc.Client.(*redis9.Client).RPush(context.TODO(), key, data).Result()
	}
	if err == redis9.Nil || err == redis7.Nil {
		err = nil
	}
	return int(reply), err
}

//Lpop Lpop
func (rc *Client) Lpop(key string) (string, error) {
	if rc == nil || rc.Client == nil {
		return "", errors.New("redis client is disconnect")
	}
	var (
		reply string
		err   error
	)
	if r, ok := rc.Client.(*redis7.Client); ok {
		reply, err = r.LPop(key).Result()
	} else {
		reply, err = rc.Client.(*redis9.Client).LPop(context.TODO(), key).Result()
	}
	if err == redis9.Nil || err == redis7.Nil {
		err = nil
	}
	return reply, err
}

//Lpush Lpush
func (rc *Client) Lpush(key string, data []byte) (int, error) {
	if rc == nil || rc.Client == nil {
		return 0, errors.New("redis client is disconnect")
	}
	var (
		reply int64
		err   error
	)
	if r, ok := rc.Client.(*redis7.Client); ok {
		reply, err = r.LPush(key, data).Result()
	} else {
		reply, err = rc.Client.(*redis9.Client).LPush(context.TODO(), key, data).Result()
	}
	if err == redis9.Nil || err == redis7.Nil {
		err = nil
	}
	return int(reply), err
}

//Del Del
func (rc *Client) Del(key string) (int, error) {
	if rc == nil || rc.Client == nil {
		return 0, errors.New("redis client is disconnect")
	}
	var (
		reply int64
		err   error
	)
	if r, ok := rc.Client.(*redis7.Client); ok {
		reply, err = r.Del(key).Result()
	} else {
		reply, err = rc.Client.(*redis9.Client).Del(context.TODO(), key).Result()
	}
	if err == redis9.Nil || err == redis7.Nil {
		err = nil
	}
	return int(reply), err
}

//Lrem Lrem
func (rc *Client) Lrem(key string, count int, value string) (int, error) {
	if rc == nil || rc.Client == nil {
		return 0, errors.New("redis client is disconnect")
	}
	var (
		reply int64
		err   error
	)
	if r, ok := rc.Client.(*redis7.Client); ok {
		reply, err = r.LRem(key, int64(count), value).Result()
	} else {
		reply, err = rc.Client.(*redis9.Client).LRem(context.TODO(), key, int64(count), value).Result()
	}
	if err == redis9.Nil || err == redis7.Nil {
		err = nil
	}
	return int(reply), err
}

//Lindex Lindex
func (rc *Client) Lindex(key string, start int) (string, error) {
	if rc == nil || rc.Client == nil {
		return "", errors.New("redis client is disconnect")
	}
	var (
		reply string
		err   error
	)
	if r, ok := rc.Client.(*redis7.Client); ok {
		reply, err = r.LIndex(key, int64(start)).Result()
	} else {
		reply, err = rc.Client.(*redis9.Client).LIndex(context.TODO(), key, int64(start)).Result()
	}
	if err == redis9.Nil || err == redis7.Nil {
		err = nil
	}
	return reply, err
}

//Llen Llen
func (rc *Client) Llen(key string) (int, error) {
	if rc == nil || rc.Client == nil {
		return 0, errors.New("redis client is disconnect")
	}
	var (
		reply int64
		err   error
	)
	if r, ok := rc.Client.(*redis7.Client); ok {
		reply, err = r.LLen(key).Result()
	} else {
		reply, err = rc.Client.(*redis9.Client).LLen(context.TODO(), key).Result()
	}

	if err == redis9.Nil || err == redis7.Nil {
		err = nil
	}
	return int(reply), err
}

//Lrange Lrange
func (rc *Client) Lrange(key string, start int, stop int) ([]string, error) {
	if rc == nil || rc.Client == nil {
		return []string{}, errors.New("redis client is disconnect")
	}
	var (
		reply []string
		err   error
	)
	if r, ok := rc.Client.(*redis7.Client); ok {
		reply, err = r.LRange(key, int64(start), int64(stop)).Result()
	} else {
		reply, err = rc.Client.(*redis9.Client).LRange(context.TODO(), key, int64(start), int64(stop)).Result()
	}
	if err == redis9.Nil || err == redis7.Nil {
		err = nil
	}
	return reply, err
}

//Lset Lset
func (rc *Client) Lset(key string, index int, value []byte) (string, error) {
	if rc == nil || rc.Client == nil {
		return "", errors.New("redis client is disconnect")
	}
	var (
		reply string
		err   error
	)
	if r, ok := rc.Client.(*redis7.Client); ok {
		reply, err = r.LSet(key, int64(index), value).Result()
	} else {
		reply, err = rc.Client.(*redis9.Client).LSet(context.TODO(), key, int64(index), value).Result()
	}
	if err == redis9.Nil || err == redis7.Nil {
		err = nil
	}
	return reply, err
}

//Sadd Sadd
func (rc *Client) Sadd(key string, member string) (int, error) {
	if rc == nil || rc.Client == nil {
		return 0, errors.New("redis client is disconnect")
	}
	var (
		reply int64
		err   error
	)
	if r, ok := rc.Client.(*redis7.Client); ok {
		reply, err = r.SAdd(key, member).Result()
	} else {
		reply, err = rc.Client.(*redis9.Client).SAdd(context.TODO(), key, member).Result()
	}
	if err == redis9.Nil || err == redis7.Nil {
		err = nil
	}
	return int(reply), err
}

//Srem Srem
func (rc *Client) Srem(key string, member string) (int, error) {
	if rc == nil || rc.Client == nil {
		return 0, errors.New("redis client is disconnect")
	}
	var (
		reply int64
		err   error
	)
	if r, ok := rc.Client.(*redis7.Client); ok {
		reply, err = r.SRem(key, member).Result()
	} else {
		reply, err = rc.Client.(*redis9.Client).SRem(context.TODO(), key, member).Result()
	}
	if err == redis9.Nil || err == redis7.Nil {
		err = nil
	}
	return int(reply), err
}

//Smembers Smembers
func (rc *Client) Smembers(key string) ([]string, error) {
	if rc == nil || rc.Client == nil {
		return []string{}, errors.New("redis client is disconnect")
	}
	var (
		reply []string
		err   error
	)
	if r, ok := rc.Client.(*redis7.Client); ok {
		reply, err = r.SMembers(key).Result()
	} else {
		reply, err = rc.Client.(*redis9.Client).SMembers(context.TODO(), key).Result()
	}
	if err == redis9.Nil || err == redis7.Nil {
		err = nil
	}
	return reply, err
}

//Set Set
func (rc *Client) Set(key string, value string) (string, error) {
	if rc == nil || rc.Client == nil {
		return "", errors.New("redis client is disconnect")
	}
	var (
		reply string
		err   error
	)
	if r, ok := rc.Client.(*redis7.Client); ok {
		reply, err = r.Set(key, value, 0).Result()
	} else {
		reply, err = rc.Client.(*redis9.Client).Set(context.TODO(), key, value, 0).Result()
	}
	if err == redis9.Nil || err == redis7.Nil {
		err = nil
	}
	return reply, err
}

//Get Get
func (rc *Client) Get(key string) (string, error) {
	if rc == nil || rc.Client == nil {
		return "", errors.New("redis client is disconnect")
	}
	var (
		reply string
		err   error
	)
	if r, ok := rc.Client.(*redis7.Client); ok {
		reply, err = r.Get(key).Result()
	} else {
		reply, err = rc.Client.(*redis9.Client).Get(context.TODO(), key).Result()
	}
	if err == redis9.Nil || err == redis7.Nil {
		err = nil
	}
	return reply, err
}

//Incr Incr
func (rc *Client) Incr(transferKey string) (int, error) {
	if rc == nil || rc.Client == nil {
		return 0, errors.New("redis client is disconnect")
	}
	var (
		reply int64
		err   error
	)
	if r, ok := rc.Client.(*redis7.Client); ok {
		reply, err = r.Incr(transferKey).Result()
	} else {
		reply, err = rc.Client.(*redis9.Client).Incr(context.TODO(), transferKey).Result()
	}
	if err == redis9.Nil || err == redis7.Nil {
		err = nil
	}
	return int(reply), err
}

//scan
func (rc *Client) Scan(cursor uint64, match string, count int64) ([]string, uint64, error) {
	if rc == nil || rc.Client == nil {
		return nil, 0, errors.New("redis client is disconnect")
	}
	var (
		keys       []string
		err        error
		nextCursor uint64
	)
	if r, ok := rc.Client.(*redis7.Client); ok {
		keys, nextCursor, err = r.Scan(cursor, match, count).Result()
	} else {
		keys, nextCursor, err = rc.Client.(*redis9.Client).Scan(context.TODO(), cursor, match, count).Result()
	}
	return keys, nextCursor, err
}

//HGet
func (rc *Client) HGet(key, field string) ([]byte, error) {
	if rc == nil || rc.Client == nil {
		return nil, errors.New("redis client is disconnect")
	}
	var (
		result string
		err    error
	)
	if r, ok := rc.Client.(*redis7.Client); ok {
		result, err = r.HGet(key, field).Result()
	} else {
		result, err = rc.Client.(*redis9.Client).HGet(context.TODO(), key, field).Result()
	}
	return []byte(result), err
}

//HSet
func (rc *Client) HSet(key, field string, value []byte) error {
	var err error
	if rc == nil || rc.Client == nil {
		err = errors.New("redis client is disconnect")
		return err
	}
	if r, ok := rc.Client.(*redis7.Client); ok {
		r.HSet(key, field, value)
	} else {
		rc.Client.(*redis9.Client).HSet(context.TODO(), key, field, value)
	}
	return err
}
