package common

import (
	"time"

	"github.com/gofrs/uuid"
	"github.com/lib/pq"
)

var (
	DevAddrKey          = "lora:ns:devaddr:" //该键值配合地址信息 value=set集合形式(devEui)  === lora_moteCfg中可以取到
	DevDeviceKey        = "lora:ns:device:"
	DevDeviceSuffixKey  = ":gwrx"
	DevDeviceGwTopoKey  = "lora:topo:gw:"
	DevDeviceDevTopoKey = "lora:topo:dev:"
	DevDeviceTopoKey    = "lora:topo:"
	DevDeviceHiskey     = "lora:his"
	DevSeparator        = ":"
)

type TopologyRedisData struct {
	UserId      uuid.UUID `db:"user_id"`
	DevMode     string    `db:"dev_mode" json:"devMode"`
	GatewayName string    `db:"gwname" json:"gwname"`
	Rssi        string    `db:"rssi" json:"rssi"` // 信号质量/强度
	Time        int64     `db:"time" json:"time"`
}

// 历史报文整体数据结构  //在redis当中存储的结构为map key deveui 数组报文
type DeviceHistory struct {
	//DeviceHistoryShow
	Seq        int            `db:"seq" json:"seq"`               // 报文序列/lora报文 FCnt字段值
	Chan       string         `db:"chan" json:"chan"`             // 信道
	Freq       float64        `db:"freq" json:"freq"`             // 中心频率(MHz)
	Sf         int            `db:"sf" json:"sf"`                 // 扩频因子
	Rfch       string         `db:"rfch" json:"rfch"`             // 接收报文的radio(射频id)
	Rssi       string         `db:"rssi" json:"rssi"`             // 信号质量/强度
	Lsnr       string         `db:"lsnr" json:"lsnr"`             // 信噪比
	Modulation string         `db:"modulation" json:"modulation"` // 模式LORA/FSK
	Adr        string         `db:"adr" json:"adr"`               // ADR是否置位(速率自适应)
	Type       string         `db:"type" json:"type"`             // 报文类型
	Port       string         `db:"port" json:"port"`             // Fport
	Time       time.Time      `db:"time" json:"time"`
	Content    pq.StringArray `db:"content" json:"content"`     //payload
	Direction  string         `db:"direction" json:"direction"` // up/down
	GatewayMac EUI64          `db:"gwmac" json:"gwmac"`
	Id         string         `db:"id" json:"id"` // id,用于记录获取到多少条报文
	UserId     uuid.UUID      `db:"user_id"`      //运维新增
	DevEui     EUI64          `db:"deveui" json:"devEui"`
	Bw         int            `db:"bw" json:"bw"`             // 带宽
	CodeRate   string         `db:"coderate" json:"codeRate"` // 编码率
	Needopt    bool           `db:"needopt" json:"needopt"`   //运维新增：待优化字段
}
