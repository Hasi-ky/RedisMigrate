package redis

import (
	"batch/common"
	"context"
	"errors"
	"strings"

	redis7 "github.com/go-redis/redis/v7"
	redis9 "github.com/redis/go-redis/v9"
)

var clusterNodes9 []redis9.ClusterNode
var clusterNodes7 []redis7.ClusterNode

//CloseSession CloseSession
func (rc *ClusterClient) CloseSession() error {
	if rc != nil && rc.ClusterClient != nil {
		if r, ok := rc.ClusterClient.(*redis7.ClusterClient); ok {
			return r.Close()
		}
	}
	return rc.ClusterClient.(*redis9.ClusterClient).Close()
}

//Ping Ping
func (rc *ClusterClient) Ping() bool {
	if rc != nil && rc.ClusterClient != nil {
		var err error
		if common.REDIS_VERSION == "7" {
			_, err = rc.ClusterClient.(*redis9.ClusterClient).Ping(context.TODO()).Result()
		} else {
			_, err = rc.ClusterClient.(*redis7.ClusterClient).Ping().Result()
		}
		if err != nil {
			return false
		} else {
			//var serverClusterNodes []redisv9.ClusterNode
			if common.REDIS_VERSION != "7" {
				var serverClusterNodes []redis7.ClusterNode
				for index, value := range strings.Split(rc.ClusterClient.(*redis7.ClusterClient).ClusterNodes().String(), "\n") {
					if len(value) > 0 && strings.Contains(value, ":6380@") {
						if index == 0 {
							serverClusterNodes = append(serverClusterNodes, redis7.ClusterNode{
								ID:   value[15:55],
								Addr: value[56:strings.Index(value, ":6380@")],
							})
						} else {
							serverClusterNodes = append(serverClusterNodes, redis7.ClusterNode{
								ID:   value[:40],
								Addr: value[41:strings.Index(value, ":6380@")],
							})
						}
					}
				}
				if len(clusterNodes7) == 0 {
					clusterNodes7 = make([]redis7.ClusterNode, len(serverClusterNodes))
					copy(clusterNodes7, serverClusterNodes)
				} else {
					// globallogger.Log.Warnf("redis local clusterNodes: %+v, server clusterNodes: %+v", clusterNodes, serverClusterNodes)
					if len(clusterNodes7) != len(serverClusterNodes) {
						// globallogger.Log.Warnln("redis clusterNodes len not match")
						return false
					} else {
						for _, v2 := range serverClusterNodes {
							for k3, v3 := range clusterNodes7 {
								if v2.Addr == v3.Addr {
									// globallogger.Log.Warnln("redis clusterNodes Addr match")
									break
								}
								if k3 == (len(clusterNodes7) - 1) {
									// globallogger.Log.Warnln("redis clusterNodes Addr not match")
									clusterNodes7 = make([]redis7.ClusterNode, len(serverClusterNodes))
									copy(clusterNodes7, serverClusterNodes)
									return false
								}
							}
						}
					}
				}
			} else {
				var serverClusterNodes []redis9.ClusterNode
				for index, value := range strings.Split(rc.ClusterClient.(*redis9.ClusterClient).ClusterNodes(context.TODO()).String(), "\n") {
					if len(value) > 0 && strings.Contains(value, ":6380@") {
						if index == 0 {
							serverClusterNodes = append(serverClusterNodes, redis9.ClusterNode{
								ID:   value[15:55],
								Addr: value[56:strings.Index(value, ":6380@")],
							})
						} else {
							serverClusterNodes = append(serverClusterNodes, redis9.ClusterNode{
								ID:   value[:40],
								Addr: value[41:strings.Index(value, ":6380@")],
							})
						}
					}
				}
				if len(clusterNodes9) == 0 {
					clusterNodes9 = make([]redis9.ClusterNode, len(serverClusterNodes))
					copy(clusterNodes9, serverClusterNodes)
				} else {
					// globallogger.Log.Warnf("redis local clusterNodes: %+v, server clusterNodes: %+v", clusterNodes, serverClusterNodes)
					if len(clusterNodes9) != len(serverClusterNodes) {
						// globallogger.Log.Warnln("redis clusterNodes len not match")
						return false
					} else {
						for _, v2 := range serverClusterNodes {
							for k3, v3 := range clusterNodes9 {
								if v2.Addr == v3.Addr {
									// globallogger.Log.Warnln("redis clusterNodes Addr match")
									break
								}
								if k3 == (len(clusterNodes9) - 1) {
									// globallogger.Log.Warnln("redis clusterNodes Addr not match")
									clusterNodes9 = make([]redis9.ClusterNode, len(serverClusterNodes))
									copy(clusterNodes9, serverClusterNodes)
									return false
								}
							}
						}
					}
				}
			}
			return true
		}
	}
	return false
}

//Rpush Rpush
func (rc *ClusterClient) Rpush(key string, data []byte) (int, error) {
	if rc == nil || rc.ClusterClient == nil {
		return 0, errors.New("redis client is disconnect")
	}
	var (
		reply int64
		err   error
	)
	if r, ok := rc.ClusterClient.(*redis7.ClusterClient); ok {
		reply, err = r.RPush(key, data).Result()
	} else {
		reply, err = rc.ClusterClient.(*redis9.ClusterClient).RPush(context.TODO(), key, data).Result()
	}
	if err == redis9.Nil || err == redis7.Nil {
		err = nil
	}
	return int(reply), err
}

//Lpop Lpop
func (rc *ClusterClient) Lpop(key string) (string, error) {
	if rc == nil || rc.ClusterClient == nil {
		return "", errors.New("redis client is disconnect")
	}
	var (
		reply string
		err   error
	)
	if r, ok := rc.ClusterClient.(*redis7.ClusterClient); ok {
		reply, err = r.LPop(key).Result()
	} else {
		reply, err = rc.ClusterClient.(*redis9.ClusterClient).LPop(context.TODO(), key).Result()
	}
	if err == redis9.Nil || err == redis7.Nil {
		err = nil
	}
	return reply, err
}

//Lpush Lpush
func (rc *ClusterClient) Lpush(key string, data []byte) (int, error) {
	if rc == nil || rc.ClusterClient == nil {
		return 0, errors.New("redis client is disconnect")
	}
	var (
		reply int64
		err   error
	)
	if r, ok := rc.ClusterClient.(*redis7.ClusterClient); ok {
		reply, err = r.LPush(key, data).Result()
	} else {
		reply, err = rc.ClusterClient.(*redis9.ClusterClient).LPush(context.TODO(), key, data).Result()
	}
	if err == redis9.Nil || err == redis7.Nil {
		err = nil
	}
	return int(reply), err
}

//Del Del
func (rc *ClusterClient) Del(key string) (int, error) {
	if rc == nil || rc.ClusterClient == nil {
		return 0, errors.New("redis client is disconnect")
	}
	var (
		reply int64
		err   error
	)
	if r, ok := rc.ClusterClient.(*redis7.ClusterClient); ok {
		reply, err = r.Del(key).Result()
	} else {
		reply, err = rc.ClusterClient.(*redis9.ClusterClient).Del(context.TODO(), key).Result()
	}
	if err == redis9.Nil || err == redis7.Nil {
		err = nil
	}
	return int(reply), err
}

//Lrem Lrem
func (rc *ClusterClient) Lrem(key string, count int, value string) (int, error) {
	if rc == nil || rc.ClusterClient == nil {
		return 0, errors.New("redis client is disconnect")
	}
	var (
		reply int64
		err   error
	)
	if r, ok := rc.ClusterClient.(*redis7.ClusterClient); ok {
		reply, err = r.LRem(key, int64(count), value).Result()
	} else {
		reply, err = rc.ClusterClient.(*redis9.ClusterClient).LRem(context.TODO(), key, int64(count), value).Result()
	}
	if err == redis9.Nil || err == redis7.Nil {
		err = nil
	}
	return int(reply), err
}

//Lindex Lindex
func (rc *ClusterClient) Lindex(key string, start int) (string, error) {
	if rc == nil || rc.ClusterClient == nil {
		return "", errors.New("redis client is disconnect")
	}
	var (
		reply string
		err   error
	)
	if r, ok := rc.ClusterClient.(*redis7.ClusterClient); ok {
		reply, err = r.LIndex(key, int64(start)).Result()
	} else {
		reply, err = rc.ClusterClient.(*redis9.ClusterClient).LIndex(context.TODO(), key, int64(start)).Result()
	}
	if err == redis9.Nil || err == redis7.Nil {
		err = nil
	}
	return reply, err
}

//Llen Llen
func (rc *ClusterClient) Llen(key string) (int, error) {
	if rc == nil || rc.ClusterClient == nil {
		return 0, errors.New("redis client is disconnect")
	}
	var (
		reply int64
		err   error
	)
	if r, ok := rc.ClusterClient.(*redis7.ClusterClient); ok {
		reply, err = r.LLen(key).Result()
	} else {
		reply, err = rc.ClusterClient.(*redis9.ClusterClient).LLen(context.TODO(), key).Result()
	}
	if err == redis9.Nil || err == redis7.Nil {
		err = nil
	}
	return int(reply), err
}

//Lrange Lrange
func (rc *ClusterClient) Lrange(key string, start int, stop int) ([]string, error) {
	if rc == nil || rc.ClusterClient == nil {
		return []string{}, errors.New("redis client is disconnect")
	}
	var (
		reply []string
		err   error
	)
	if r, ok := rc.ClusterClient.(*redis7.ClusterClient); ok {
		reply, err = r.LRange(key, int64(start), int64(stop)).Result()
	} else {
		reply, err = rc.ClusterClient.(*redis9.ClusterClient).LRange(context.TODO(), key, int64(start), int64(stop)).Result()
	}
	if err == redis9.Nil || err == redis7.Nil {
		err = nil
	}
	return reply, err
}

//Lset Lset
func (rc *ClusterClient) Lset(key string, index int, value []byte) (string, error) {
	if rc == nil || rc.ClusterClient == nil {
		return "", errors.New("redis client is disconnect")
	}
	var (
		reply string
		err   error
	)
	if r, ok := rc.ClusterClient.(*redis7.ClusterClient); ok {
		reply, err = r.LSet(key, int64(index), value).Result()
	} else {
		reply, err = rc.ClusterClient.(*redis9.ClusterClient).LSet(context.TODO(), key, int64(index), value).Result()
	}
	if err == redis9.Nil || err == redis7.Nil {
		err = nil
	}
	return reply, err
}

//Sadd Sadd
func (rc *ClusterClient) Sadd(key string, member string) (int, error) {
	if rc == nil || rc.ClusterClient == nil {
		return 0, errors.New("redis client is disconnect")
	}
	var (
		reply int64
		err   error
	)
	if r, ok := rc.ClusterClient.(*redis7.ClusterClient); ok {
		reply, err = r.SAdd(key, member).Result()
	} else {
		reply, err = rc.ClusterClient.(*redis9.ClusterClient).SAdd(context.TODO(), key, member).Result()
	}
	if err == redis9.Nil || err == redis7.Nil {
		err = nil
	}
	return int(reply), err
}

//Srem Srem
func (rc *ClusterClient) Srem(key string, member string) (int, error) {
	if rc == nil || rc.ClusterClient == nil {
		return 0, errors.New("redis client is disconnect")
	}
	var (
		reply int64
		err   error
	)
	if r, ok := rc.ClusterClient.(*redis7.ClusterClient); ok {
		reply, err = r.SRem(key, member).Result()
	} else {
		reply, err = rc.ClusterClient.(*redis9.ClusterClient).SRem(context.TODO(), key, member).Result()
	}
	if err == redis9.Nil || err == redis7.Nil {
		err = nil
	}
	return int(reply), err
}

//Smembers Smembers
func (rc *ClusterClient) Smembers(key string) ([]string, error) {
	if rc == nil || rc.ClusterClient == nil {
		return []string{}, errors.New("redis client is disconnect")
	}
	var (
		reply []string
		err   error
	)
	if r, ok := rc.ClusterClient.(*redis7.ClusterClient); ok {
		reply, err = r.SMembers(key).Result()
	} else {
		reply, err = rc.ClusterClient.(*redis9.ClusterClient).SMembers(context.TODO(), key).Result()
	}
	if err == redis9.Nil || err == redis7.Nil {
		err = nil
	}
	return reply, err
}

//Set Set
func (rc *ClusterClient) Set(key string, value string) (string, error) {
	if rc == nil || rc.ClusterClient == nil {
		return "", errors.New("redis client is disconnect")
	}
	var (
		reply string
		err   error
	)
	if r, ok := rc.ClusterClient.(*redis7.ClusterClient); ok {
		reply, err = r.Set(key, value, 0).Result()
	} else {
		reply, err = rc.ClusterClient.(*redis9.ClusterClient).Set(context.TODO(), key, value, 0).Result()
	}
	if err == redis9.Nil || err == redis7.Nil {
		err = nil
	}
	return reply, err
}

//Get Get
func (rc *ClusterClient) Get(key string) (string, error) {
	if rc == nil || rc.ClusterClient == nil {
		return "", errors.New("redis client is disconnect")
	}
	var (
		reply string
		err   error
	)
	if r, ok := rc.ClusterClient.(*redis7.ClusterClient); ok {
		reply, err = r.Get(key).Result()
	} else {
		reply, err = rc.ClusterClient.(*redis9.ClusterClient).Get(context.TODO(), key).Result()
	}
	if err == redis9.Nil || err == redis7.Nil {
		err = nil
	}
	return reply, err
}

//Incr Incr
func (rc *ClusterClient) Incr(transferKey string) (int, error) {
	if rc == nil || rc.ClusterClient == nil {
		return 0, errors.New("redis client is disconnect")
	}
	var (
		reply int64
		err   error
	)
	if r, ok := rc.ClusterClient.(*redis7.ClusterClient); ok {
		reply, err = r.Incr(transferKey).Result()
	} else {
		reply, err = rc.ClusterClient.(*redis9.ClusterClient).Incr(context.TODO(), transferKey).Result()
	}
	if err == redis9.Nil || err == redis7.Nil {
		err = nil
	}
	return int(reply), err
}

//scan
func (rc *ClusterClient) Scan(cursor uint64, match string, count int64) ([]string, uint64, error) {
	if rc == nil || rc.ClusterClient == nil {
		return nil, 0, errors.New("redis client is disconnect")
	}
	var (
		keys       []string
		err        error
		nextCursor uint64
	)
	if r, ok := rc.ClusterClient.(*redis7.Client); ok {
		keys, nextCursor, err = r.Scan(cursor, match, count).Result()
	} else {
		keys, nextCursor, err = rc.ClusterClient.(*redis9.Client).Scan(context.TODO(), cursor, match, count).Result()
	}
	return keys, nextCursor, err
}

//Hget
func (rc *ClusterClient) HGet(key, field string) ([]byte, error) {
	if rc == nil || rc.ClusterClient == nil {
		return nil, errors.New("redis client is disconnect")
	}
	var (
		result string
		err    error
	)
	if r, ok := rc.ClusterClient.(*redis7.Client); ok {
		result, err = r.HGet(key, field).Result()
	} else {
		result, err = rc.ClusterClient.(*redis9.Client).HGet(context.TODO(), key, field).Result()
	}
	return []byte(result), err
}

//HSet
func (rc *ClusterClient) HSet(key, field string, value []byte) error {
	var err error
	if rc == nil || rc.ClusterClient == nil {
		err = errors.New("redis client is disconnect")
		return err
	}
	if r, ok := rc.ClusterClient.(*redis7.Client); ok {
		r.HSet(key, field, value)
	} else {
		rc.ClusterClient.(*redis9.Client).HSet(context.TODO(), key, field, value)
	}
	return err
}