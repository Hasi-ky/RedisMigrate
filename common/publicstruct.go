package common

import (
	"os"
	"time"

	"github.com/gofrs/uuid"
	"github.com/lib/pq"
)

// RXWindow defines the RX window option.
type RXWindow int8
type Array []int

type Channel struct {
	Frequency int
	MinDR     int
	MaxDR     int
	enabled   bool
	custom    bool
}

type DeviceMode string

type CID byte
type DevAddr [4]byte
type EUI64 [8]byte
type AES128Key [16]byte
type DevNonce uint16

// JoinType defines the join-request type.
type JoinType uint8

// Join-request types.
const (
	JoinRequestType    JoinType = 0xff
	RejoinRequestType0 JoinType = 0x00
	RejoinRequestType1 JoinType = 0x01
	RejoinRequestType2 JoinType = 0x02
	//总体按照数据库的形式来分类

	DeviceSessionTTL      = time.Hour * 24 * 31
	DeviceSessionSevenTTL = time.Hour * 24 * 7
)

var (
	GwDeviceKey         = "lora:ns:gw"
	GwDeviceRouteKey    = "lora:ns:gwroute"
	DevAddrKey          = "lora:ns:devaddr:" //该键值配合地址信息 value=set集合形式(devEui)  === lora_moteCfg中可以取到
	DevDeviceKey        = "lora:ns:device:"
	DevDeviceSuffixKey  = ":gwrx"
	DevDeviceGwTopoKey  = "lora:topo:gw:"
	DevDeviceDevTopoKey = "lora:topo:dev:"
	DevDeviceTopoKey    = "lora:topo:"
	DevSeparator        = ":"
	DevActivationKey    = "lora:activation:"
	DevAddrKeyAll       = "lora:ns:devaddr*"
	REDIS_VERSION       = "4"
	RedisHost           string
	RedisDBName         string
	RedisPwd            string
	RedisCluster        bool
	MongoHost           string
	PsqlHost            string
	PsqlUser            string
	PsqlPwd             string
	PsqlDBName          string
	PsqlPort            string
	Mode                string
	Output              string
	Input               string
	DatabaseCountString string
	NeedHistory         bool
	FileHistory         *os.File
)

type TopologyRedisData struct {
	UserId      uuid.UUID `db:"user_id"`
	DevMode     string    `db:"dev_mode" json:"devMode,omitempty"`
	GatewayName string    `db:"gwname" json:"gwname,omitempty"`
	Rssi        string    `db:"rssi" json:"rssi,omitempty"` // 信号质量/强度
	Time        int64     `db:"time" json:"time,omitempty"`
}

// 历史报文整体数据结构  //在redis当中存储的结构为map key deveui 数组报文
type DeviceHistory struct {
	//DeviceHistoryShow
	Seq        int            `db:"seq" json:"seq,omitempty"`               // 报文序列/lora报文 FCnt字段值
	Chan       string         `db:"chan" json:"chan,omitempty"`             // 信道
	Freq       float64        `db:"freq" json:"freq,omitempty"`             // 中心频率(MHz)
	Sf         int            `db:"sf" json:"sf,omitempty"`                 // 扩频因子
	Rfch       string         `db:"rfch" json:"rfch,omitempty"`             // 接收报文的radio(射频id)
	Rssi       string         `db:"rssi" json:"rssi,omitempty"`             // 信号质量/强度
	Lsnr       string         `db:"lsnr" json:"lsnr,omitempty"`             // 信噪比
	Modulation string         `db:"modulation" json:"modulation,omitempty"` // 模式LORA/FSK
	Adr        string         `db:"adr" json:"adr,omitempty"`               // ADR是否置位(速率自适应)
	Type       string         `db:"type" json:"type,omitempty"`             // 报文类型
	Port       string         `db:"port" json:"port,omitempty"`             // Fport
	Time       time.Time      `db:"time" json:"time,omitempty"`
	Content    pq.StringArray `db:"content" json:"content,omitempty"`     //payload
	Direction  string         `db:"direction" json:"direction,omitempty"` // up/down
	GatewayMac EUI64          `db:"gwmac" json:"gwmac,omitempty"`
	Id         string         `db:"id" json:"id,omitempty"` // id,用于记录获取到多少条报文
	UserId     uuid.UUID      `db:"user_id,omitempty"`      //运维新增
	DevEui     EUI64          `db:"deveui" json:"devEui,omitempty"`
	Bw         int            `db:"bw" json:"bw,omitempty"`             // 带宽
	CodeRate   string         `db:"coderate" json:"codeRate,omitempty"` // 编码率
	Needopt    bool           `db:"needopt" json:"needopt,omitempty"`   //运维新增：待优化字段
}

type DeviceActivation struct {
	ID          int64     `db:"id"`
	CreatedAt   time.Time `db:"created_at" json:"createdAt,omitempty"`
	DevEUI      EUI64     `db:"dev_eui" json:"deveui,omitempty"`
	JoinEUI     EUI64     `db:"join_eui" json:"joinEui,omitempty"`
	DevAddr     DevAddr   `db:"dev_addr" json:"devAddr,omitempty"`
	AppsKey     AES128Key `db:"app_s_key" json:"appsKey,omitempty"` // abp专用 用于加密解密数据，
	FNwkSIntKey AES128Key `db:"f_nwk_s_int_key" json:"fNwkSIntKey,omitempty"`
	SNwkSIntKey AES128Key `db:"s_nwk_s_int_key" json:"sNwkSIntKey,omitempty"`
	NwkSEncKey  AES128Key `db:"nwk_s_enc_key" json:"nwkSEncKey,omitempty"`
	DevNonce    DevNonce  `db:"dev_nonce" json:"devNonce,omitempty"`
	JoinReqType JoinType  `db:"join_req_type" json:"joinReqType"`
}

type DeviceHistoryJson struct {
	Content map[string][]DeviceHistory `db:"content" json:"content,omitempty"`
}
