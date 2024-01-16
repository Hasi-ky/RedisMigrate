package main

import (
	"github.com/gofrs/uuid"
)

//运维新增：网关与终端拓扑整体数据结构
type TopologyRedisData struct {
	UserId      uuid.UUID `db:"user_id"`
	DevMode     string    `db:"dev_mode" json:"devMode"`
	GatewayName string    `db:"gwname" json:"gwname"`
	Rssi        string    `db:"rssi" json:"rssi"` // 信号质量/强度
	Time        int64     `db:"time" json:"time"`
}
