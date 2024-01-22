package redis

import (
	"batch/common"
	"context"
	"fmt"
	"time"
	log "github.com/sirupsen/logrus"	
	redis7 "github.com/go-redis/redis/v7"
	redis9 "github.com/redis/go-redis/v9"
)

// Client Client
type Client struct {
	Client interface{}
}

// NewClient NewClient
func NewClient(host string, port string, password string) (*Client, error) {
	var r *Client = new(Client)
	switch common.REDIS_VERSION {
	case "7":
		r.Client = redis9.NewClient(&redis9.Options{
			Addr:        host + ":" + port,
			DialTimeout: 5 * time.Second,
			Password:    password,
		})
	default:
		r.Client = redis7.NewClient(&redis7.Options{
			Addr:        host + ":" + port,
			DialTimeout: 5 * time.Second,
			Password:    password,
		})
	}
	if r.Client == nil {
		return nil, fmt.Errorf("create client nil")
	}
	if (common.REDIS_VERSION == "7" && r.Client.(*redis9.Client).ClientGetName(context.TODO()).Name() == "") || (common.REDIS_VERSION != "7" && r.Client.(*redis7.Client).ClientGetName().Name() == "") {
		return nil, fmt.Errorf("create client failed")
	}
	var err error
	if common.REDIS_VERSION == "7" {
		_, err = r.Client.(*redis9.Client).Ping(context.TODO()).Result()
	} else {
		_, err = r.Client.(*redis7.Client).Ping().Result()
	}
	if err != nil {
		log.Errorln(err)
		return nil, err
	}
	log.Errorf("current single redis version is: %v", common.REDIS_VERSION)
	return r, nil
}
