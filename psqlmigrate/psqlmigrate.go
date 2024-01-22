package main

import (
	"batch/common"
	"batch/db/redis"
	"batch/global"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"strconv"
	"strings"
	"time"
	log "github.com/sirupsen/logrus"	
	"github.com/gofrs/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"google.golang.org/protobuf/proto"
)

var (
	cursor        uint64
	keys          []string
	err           error
	host          = "127.0.0.1"
	port          = "5432"
	user          = "iotware"
	pwd           = "iotware"
	dbName        = "iotware"
	DevAddrKeyAll = "lora:ns:devaddr*"
)

var (
	redisHost    string
	redisDBName  string
	redisPwd     string
	psqlHost     string
	psqlUser     string
	psqlPwd      string
	psqlDBName   string
	redisVersion string
	redisCluster bool
)

func main() {
	flag.StringVar(&host, "rH", "redis-svc:6379", "-rH=172.0.0.1:6379")
	flag.StringVar(&redisDBName, "rDB", "0", "-rDB=0")
	flag.StringVar(&redisPwd, "rPD", "", "-rPD=123456")
	flag.StringVar(&psqlHost, "pH", "127.0.0.1", "-pH=127.0.0.1")
	flag.StringVar(&psqlUser, "pU", "iotware", "-pU=iotware")
	flag.StringVar(&psqlPwd, "pP", "iotware", "-pP=iotware")
	flag.StringVar(&psqlDBName, "pDB", "iotware", "-pDB=iotware")
	flag.StringVar(&redisVersion, "rV", "", "-rV=4")
	flag.BoolVar(&redisCluster, "rC", false, "-rC=false")
	help := flag.Bool("help", false, "Display help infomation")
	flag.Parse()
	if *help {
		printHelp()
	}

	getRedisClient()
	defer global.Rdb.CloseSession()
	getPsqlClient()
	defer global.Sdb.Close()
	storeAddrToPsql()
	// restoreRedis(d, rdb)
}

func printHelp() {
	fmt.Print(`
Usage:
	Psqlmigrate [-rH=127.0.0.1:6379] -rDB=0 [-password=Auth] [-rC=F] [-rV=4] ....

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

Examples:
	$ Psqlmigrate -rH=127.0.0.1
	$ Psqlmigrate -rH=127.0.0.1 -rPD=123445
	$ Psqlmigrate -rH=127.0.0.1 -rPD=123445
	$ Psqlmigrate -rH=127.0.0.1 -rPD=123445 -rV=7
	`)
}

func getRedisClient(){
	hostAndPort := strings.Split(redisHost, ":")
	var err error
	if redisCluster {
		servers := []string{redisHost}
		global.Rdb, err = redis.NewClusterClient(servers, redisPwd)
	} else {
		global.Rdb, err = redis.NewClient(hostAndPort[0], hostAndPort[1], redisPwd)
	}
	if err != nil {
		log.Fatal("redis client创键失败")
	}
	pingRes := global.Rdb.Ping()
	if !pingRes {
		log.Fatal("redis连接失败")
	}
	log.Infoln("redis连接失败")
}

func getPsqlClient() {
	var err error
	psqlInfo := fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable",
		psqlUser, psqlPwd, psqlHost, psqlDBName)
	global.Sdb, err = sqlx.Open("postgres", psqlInfo)
	if err != nil {
		log.Fatal("获取数据库连接失败!",err)
	}
	log.Infoln("获取数据库连接成功!")
}

// 还原profile中对应内容
func storeAddrToPsql() {
	for {
		keys, cursor, err = global.Rdb.Scan(cursor, DevAddrKeyAll, 100)
		if err != nil {
			log.Fatal(err)
		}
		updateProfile := "UPDATE lora_device_profile SET dev_addr = $1 WHERE dev_eui = $2"
		for _, key := range keys {
			fmt.Println(key)
			addr := getAddrFromKey(key)
			devEUIs, _ :=  global.Rdb.Smembers(key)
			for _, devEUI := range devEUIs {
				_, err = global.Sdb.Exec(updateProfile, addr[:], []byte(devEUI)[:])
				if err != nil {
					log.Fatal("数据库更新对应profile失效", err)
				}
			}
		}
		if cursor == 0 {
			break
		}
	}
}

// history缓存中数据的user-id数据还原，以及ds当中数据的还原
func restoreRedis(db *sqlx.DB, rdb *redis.Client) {
	rows, err := db.Query("SELECT user_id, dev_eui FROM lora_device_profile")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var userId uuid.UUID
		var devEuiB []byte
		err := rows.Scan(&userId, &devEuiB)
		if err != nil {
			log.Fatal(err)
		}
		devEui := common.ByteToEUI(devEuiB)
		restoreHisAndDs(userId, devEui, rdb, db)
	}
	insertActivation(rdb, db)
	sweepRedis(rdb)
}

// hitory还原, ds, 还原同时向数据库中插入activation 以及history相关数据
func restoreHisAndDs(userId uuid.UUID, devEui common.EUI64, rdb *redis.Client, db *sqlx.DB) {
	devEuiStr := hex.EncodeToString(devEui[:])
	historyByte, err1 := global.Rdb.HGet(common.DevDeviceHiskey, devEuiStr)
	if err1 != nil {
		log.Info("无历史数据",err1)
		return
	}
	var historyMsg []common.DeviceHistory
	err = json.Unmarshal(historyByte, &historyMsg)
	if err != nil {
		fmt.Println("<restoreRedis> 数据解析失败")
		log.Fatal(err)
	}
	for i := 0; i < len(historyMsg); i++ {
		historyMsg[i].UserId = userId
	}
	byteNewHis, err1 := json.Marshal(historyMsg)
	if err1 != nil {
		fmt.Println("<restoreRedis> 回填数据失败")
	}
	//着手插入history
	rdb.HSet(common.DevDeviceHiskey, devEuiStr, byteNewHis)
	insertHistory(db, historyMsg)

	//ds还原 ========= 分隔 ======
	deviceKey := common.DevDeviceKey + devEuiStr
	deviceSession := []byte(rdb.Get(deviceKey).Val())
	var devicePB common.DeviceSessionPB
	err1 = proto.Unmarshal(deviceSession, &devicePB)
	if err1 != nil {
		fmt.Println("重新编码失效")
		log.Fatal(err1)
	}
	ds := common.DeviceSessionFromPB(&devicePB)
	ds.UserId = userId
	dsPB := common.DeviceSessionToPB(ds)
	newDeviceSession, err2 := proto.Marshal(dsPB)
	if err2 != nil {
		log.Fatal(err2)
	}
	duration := rdb.TTL(deviceKey).Val()
	rdb.Set(deviceKey, newDeviceSession, duration)
}

func getAddrFromKey(key string) common.DevAddr {
	addrStr := strings.Split(key, ":")
	byteAddr, _ := hex.DecodeString(addrStr[len(addrStr)-1])
	return common.ByteToAddr(byteAddr)
}

// 消息插入时无排序要求
func insertHistory(db *sqlx.DB, historyMsg []common.DeviceHistory) {
	for index, deviceHistory := range historyMsg {
		deviceHistory.Time = deviceHistory.Time.Add(-8 * time.Hour)
		deviceHistory.Id = strconv.Itoa(index + 1)
		_, err = db.Exec(`
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
			fmt.Println("数据插入数据库时发生错误!")
			log.Fatal(err)
		}
	}
}

// 插入激活信息
func insertActivation(rdb *redis.Client, db *sqlx.DB) {
	activationMap := rdb.HGetAll(common.DevActivationKey).Val()
	for devEUI, jsonStr := range activationMap {
		var activationData common.DeviceActivation
		err := json.Unmarshal([]byte(jsonStr), &activationData)
		if err != nil {
			fmt.Printf("设备[%s] 激活信息部分解码出现错误!", devEUI)
			continue
		}
		err = sqlx.Get(db, &activationData.ID, `
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
			fmt.Printf("设备[%s] 激活信息插入失败", devEUI)
			log.Fatal(err)
		}
	}
}

// 扫尾处理
func sweepRedis(rdb *redis.Client) {
	err := rdb.Del(common.DevActivationKey).Err()
	if err != nil {
		fmt.Println("删除键值失败，请查看redis")
	}
	err = rdb.Del(common.DevDeviceHiskey).Err()
	if err != nil {
		fmt.Println("删除键值失败，请查看redis")
	}
	fmt.Println("扫尾工作完成")
}
