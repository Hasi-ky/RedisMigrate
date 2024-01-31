package main

import (
	"batch/common"
	"batch/global"
	"encoding/hex"
	"encoding/json"
	"flag"
	"strings"
	"time"

	"github.com/gofrs/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

var ()

func main() {
	flag.StringVar(&common.RedisHost, "rH", "redis-svc:6379", "-rH=redis-svc:6379")
	flag.StringVar(&common.RedisDBName, "rDB", "0", "-rDB=0")
	flag.StringVar(&common.RedisPwd, "rPD", "", "-rPD=123456")
	flag.StringVar(&common.REDIS_VERSION, "rV", "", "-rV=4")
	flag.BoolVar(&common.RedisCluster, "rC", false, "-rC=false")
	flag.StringVar(&common.PsqlHost, "pH", "127.0.0.1", "-pH=127.0.0.1")
	flag.StringVar(&common.PsqlUser, "pU", "iotware", "-pU=iotware")
	flag.StringVar(&common.PsqlPwd, "pP", "iotware", "-pP=iotware")
	flag.StringVar(&common.PsqlDBName, "pDB", "iotware", "-pDB=iotware")
	needHistory := flag.Bool("his", true, "set history switch")
	help := flag.Bool("help", false, "Display help infomation")
	flag.Parse()
	if *help {
		printHelp()
		return
	}
	global.GetRedisClient()
	global.GetPsqlClient()
	defer global.Rdb.CloseSession()
	defer global.Sdb.Close()
	storeAddrToPsql()
	restoreRedis()
	if *needHistory {
		openDevHisSwitch()
	}
}

func printHelp() {
	log.Infof(`
Usage:
	Psqlmigrate [-rH=127.0.0.1:6379] [-rDB=0] [-password=Auth] [-rC=F] [-rV=4] ....

Options:
	-rH=redisHost                 The redis instance (host:port).
	-rDB=redisDBName              The redis DBName use (0-15)
	-rPD=redisPassword            The redis Password
	-rV=redisVersion              The redis version
	-rC=redisCluster              Is Redis a cluster
	-pH=psqlHost                  The postgres instance
	-pU=psqlUserName              The postgres username
	-pP=psqlPassword              The postgres password
	-pDB=psqlDBName               The postgres dbname
	-his=psqlHistory              The postgres history message

Examples:
	$ Psqlmigrate -rH=127.0.0.1
	$ Psqlmigrate -rH=127.0.0.1 -rPD=123445
	$ Psqlmigrate -rH=127.0.0.1 -rPD=123445
	$ Psqlmigrate -rH=127.0.0.1 -rPD=123445 -rV=7
	`)
}

// 还原profile中对应内容
func storeAddrToPsql() {
	var (
		err  error
		keys []string
	)
	keys, err = global.Rdb.Scan(0, common.DevAddrKeyAll, 100)
	if err != nil {
		log.Fatal(err)
	}
	updateProfile := "UPDATE lora_device_profile SET dev_addr = $1 WHERE dev_eui = $2"
	for _, key := range keys {
		addr := getAddrFromKey(key)
		devEUIs, _ := global.Rdb.Smembers(key)
		for _, devEUI := range devEUIs {
			_, err = global.Sdb.Exec(updateProfile, addr[:], []byte(devEUI)[:])
			if err != nil {

				log.Errorf("设备[%s]数据库更新对应profile失效 %v\n", hex.EncodeToString([]byte(devEUI)), err)
			}
		}
	}
	log.Infoln("设备模板还原完成")
}

// history缓存中数据的user-id数据还原，以及ds当中数据的还原
func restoreRedis() {
	rows, err := global.Sdb.Query("SELECT user_id, dev_eui FROM lora_device_profile")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() { //根据数据库中实际存在的内容进行真实还原
		var userId uuid.UUID
		var devEuiB []byte
		err := rows.Scan(&userId, &devEuiB)
		if err != nil {
			log.Fatal(err)
		}
		devEui := common.ByteToEUI(devEuiB)
		restoreHisAndDs(userId, devEui)
	}
	insertActivation()
	sweepRedis()
}

// hitory还原, ds, 还原同时向数据库中插入activation 以及history相关数据
func restoreHisAndDs(userId uuid.UUID, devEui common.EUI64) {
	var (
		historyMsg []common.DeviceHistory
		err error
		devicePB   common.DeviceSessionPB
		devEuiStr = hex.EncodeToString(devEui[:])
	)
	//devicesession还原 =========== 分隔 ===============
	deviceSession, err := global.Rdb.Get(common.DevDeviceKey + devEuiStr)
	if err != nil {
		log.Errorf("设备[%s]获取Session缓存失败%v\n", devEuiStr, err)
		return
	}
	err = proto.Unmarshal([]byte(deviceSession), &devicePB)
	if err != nil {
		log.Errorf("设备[%s]缓存解码失败%v\n", devEuiStr, err)
		return
	}
	ds := common.DeviceSessionFromPB(&devicePB)
	ds.UserId = userId
	dsPB := common.DeviceSessionToPB(ds)
	newDeviceSession, err := proto.Marshal(dsPB)
	if err != nil {
		log.Errorf("设备[%s]缓存编码失败%v\n", devEuiStr, err)
		return
	}
	duration, err5 := global.Rdb.TTL(common.DevDeviceKey + devEuiStr)
	if err5 != nil {
		log.Errorf("设备[%s]对应截至时间已过期%v\n", devEuiStr, err5)
		duration = common.DeviceSessionSevenTTL
	}
	_, err6 := global.Rdb.Set(common.DevDeviceKey+devEuiStr, newDeviceSession, duration)
	if err6 != nil {
		log.Errorf("设备[%s]设置缓存过程异常%v\n", devEuiStr, err6)
	}
	//===============历史数据处理================
	historyByte, err := global.Rdb.HGet(common.DevDeviceHiskey, devEuiStr)
	if err != nil {
		log.Infof("设备[%s]无历史数据%v\n", devEuiStr, err)
		return
	}
	err = json.Unmarshal(historyByte, &historyMsg)
	if err != nil {
		log.Errorf("设备[%s]数据解析失败%v\n", devEuiStr, err)
		return
	}
	for i := 0; i < len(historyMsg); i++ {
		historyMsg[i].UserId = userId
	}
	byteNewHis, err := json.Marshal(historyMsg)
	if err != nil {
		log.Errorf("设备[%s]数据回填失败%v\n", devEuiStr, err)
		return
	}
	global.Rdb.HSet(common.DevDeviceHiskey, devEuiStr, byteNewHis) //缓存重置
	insertHistory(historyMsg)
}

func getAddrFromKey(key string) common.DevAddr {
	addrStr := strings.Split(key, ":")
	byteAddr, _ := hex.DecodeString(addrStr[len(addrStr)-1])
	return common.ByteToAddr(byteAddr)
}

// id作为消息总数标记，在datamigrate中完成迁移
func insertHistory(historyMsg []common.DeviceHistory) {
	for _, deviceHistory := range historyMsg {
		deviceHistory.Time = deviceHistory.Time.Add(-8 * time.Hour)
		_, err := global.Sdb.Exec(`
insert into lora_device_history(
   id, deveui, gwmac, type, lsnr, rssi, chan, rfch, freq, modulation, bw, sf,seq, coderate, adr, port,direction,content,time,user_id                        
)values ($1, $2, $3, $4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20) on conflict (id)
DO UPDATE SET id=$1,deveui=$2,gwmac=$3,type=$4,lsnr=$5,rssi=$6,chan=$7,rfch=$8,freq=$9,modulation=$10,bw=$11,sf=$12,seq=$13,coderate=$14,adr=$15,port=$16,direction=$17,content=$18,time=$19,user_id=$20`,
			deviceHistory.Id,
			deviceHistory.DevEui[:],
			deviceHistory.GatewayMac[:],
			deviceHistory.Type,
			deviceHistory.Lsnr,
			deviceHistory.Rssi,
			deviceHistory.Chan,
			deviceHistory.Rfch,
			deviceHistory.Freq,
			deviceHistory.Modulation,
			deviceHistory.Bw,
			deviceHistory.Sf,
			deviceHistory.Seq,
			deviceHistory.CodeRate,
			deviceHistory.Adr,
			deviceHistory.Port,
			deviceHistory.Direction,
			pq.Array(deviceHistory.Content),
			deviceHistory.Time,
			deviceHistory.UserId,
		)
		if err != nil {
			log.Fatal("数据插入数据库时发生错误!", err)
		}
	}
}

// 插入激活信息
func insertActivation() {
	activationMap, err := global.Rdb.HGetAll(common.DevActivationKey)
	if err != nil {
		log.Errorln("激活数据获取失败", err)
	}
	for devEUI, jsonStr := range activationMap {
		var activationData common.DeviceActivation
		err = json.Unmarshal([]byte(jsonStr), &activationData)
		if err != nil {
			log.Errorf("设备[%s] 激活信息部分解码出现错误!", devEUI)
			continue
		}
		err = sqlx.Get(global.Sdb, &activationData.ID, `
		insert into lora_device_activation (
			created_at,
			dev_eui,
			join_eui,
			dev_addr,
		    app_s_key,
			s_nwk_s_int_key,
			f_nwk_s_int_key,
			nwk_s_enc_key,
			dev_nonce,
			join_req_type
		) values ($1, $2, $3, $4, $5, $6, $7, $8, $9,$10)
		returning id`,
			activationData.CreatedAt,
			activationData.DevEUI[:],
			activationData.JoinEUI[:],
			activationData.DevAddr[:],
			activationData.AppsKey[:],
			activationData.SNwkSIntKey[:],
			activationData.FNwkSIntKey[:],
			activationData.NwkSEncKey[:],
			activationData.DevNonce,
			activationData.JoinReqType,
		)
		if err != nil {
			log.Errorf("设备[%s] 激活信息插入失败%v\n", devEUI, err)
		}
	}
}

//设置数据库中日志开启
func openDevHisSwitch() {
	updateDevHisSwitch := "UPDATE lora_device_history_switch SET switch_status = $1"
	_, err := global.Sdb.Exec(updateDevHisSwitch, "on")
	if err != nil {
		log.Errorf("更新历史数据开关状态失败:%v", err)
	}
}

// 扫尾处理
func sweepRedis() {
	_, err := global.Rdb.Del(common.DevActivationKey)
	if err != nil {
		log.Errorf("删除键值[%s]失败, 请查看redis %v\n", common.DevActivationKey, err)
	}
	_, err = global.Rdb.Del(common.DevDeviceHiskey)
	if err != nil {
		log.Errorf("删除键值[%s]失败, 请查看redis %v\n", common.DevDeviceHiskey, err)
	}
	log.Infoln("残余键扫尾工作完成")
}
