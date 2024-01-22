package redis

import (
	"batch/common"
	"context"
	"fmt"
	"time"

	redis7 "github.com/go-redis/redis/v7"
	redis9 "github.com/redis/go-redis/v9"
	log "github.com/sirupsen/logrus"
)

//ClusterClient ClusterClient
type ClusterClient struct {
	ClusterClient interface{}
}

//NewClusterClient NewClusterClient
func NewClusterClient(servers []string, password string) (*ClusterClient, error) {
	var r *ClusterClient = new(ClusterClient)
	switch common.REDIS_VERSION {
	case "7":
		r.ClusterClient = redis9.NewClusterClient(&redis9.ClusterOptions{
			Addrs:        servers,
			DialTimeout: 5 * time.Second,
			Password:    password,
		})
	default:
		r.ClusterClient = redis7.NewClusterClient(&redis7.ClusterOptions{
			Addrs:        servers,
			DialTimeout: 5 * time.Second,
			Password:    password,
		})
	}
	if r.ClusterClient == nil {
		return nil, fmt.Errorf("create cluster client nil")
	}
	if common.REDIS_VERSION == "7" && r.ClusterClient.(*redis9.ClusterClient).ClientGetName(context.TODO()).Name() == "" {
		return nil, fmt.Errorf("create cluster client failed")
	} else if common.REDIS_VERSION != "7" && r.ClusterClient.(*redis7.ClusterClient).ClientGetName().Name() == "" {
		return nil, fmt.Errorf("create cluster client failed")
	}
	var err error
	if common.REDIS_VERSION == "7" {
		_, err = r.ClusterClient.(*redis9.ClusterClient).Ping(context.TODO()).Result()
	} else {
		_, err = r.ClusterClient.(*redis7.ClusterClient).Ping().Result()
	}
	if err != nil {
		log.Errorln(err)
		return nil, err
	}
	log.Errorf("current cluster redis version is: %v\n", common.REDIS_VERSION)
	return r, nil
}
