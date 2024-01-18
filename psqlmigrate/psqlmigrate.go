package main

import (
	"batch/common"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/go-redis/redis/v7"
	"github.com/gofrs/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"google.golang.org/protobuf/proto"
)

var (
	cursor   uint64
	keys     []string
	err      error
	host     = "127.0.0.1"
	port     = "5432"
	user     = "iotware"
	pwd      = "iotware"
	dbName   = "iotware"
	redisUrl = "redis-svc:6379"
)

func main() {
	rdb := getRedisClient(redisUrl)
	defer rdb.Close()
	d := getPsqlClient()
	defer d.Close()
	storeAddrToPsql(d, rdb)
	restoreRedis(d, rdb)
}

func getRedisClient(redisUrl string) *redis.Client {
	rdb := redis.NewClient(&redis.Options{
		Addr: redisUrl,
	})
	err = rdb.Ping().Err()
	if err != nil {
		fmt.Println("redis连接失败")
		log.Fatal(err)
	}
	fmt.Println("redis连接成功")
	return rdb
}

func getPsqlClient() *sqlx.DB {
	psqlInfo := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		user, pwd, host, port, dbName)
	d, err := sqlx.Open("postgres", psqlInfo)
	if err != nil {
		fmt.Println("获取数据库连接失败!")
		log.Fatal(err)
	}
	fmt.Println("获取数据库连接成功!")
	return d
}

//还原profile中对应内容
func storeAddrToPsql(db *sqlx.DB, rdb *redis.Client) {
	//首先将对应profile部分内容还原
	for {
		keys, cursor, err = rdb.Scan(cursor, "lora:ns:devaddr*", 100).Result()
		if err != nil {
			log.Fatal(err)
		}
		updateProfile := "UPDATE lora_device_profile SET dev_addr = $1 WHERE dev_eui = $2"
		for _, key := range keys {
			fmt.Println(key)
			addr := getAddrFromKey(key)
			devEUIs := rdb.SMembers(key).Val()
			for _, devEUI := range devEUIs {
				_, err = db.Exec(updateProfile, addr[:], []byte(devEUI)[:])
				if err != nil {
					fmt.Println("数据库更新对应profile失效")
					log.Fatal(err)
				}
			}
		}
		if cursor == 0 {
			break
		}
	}
}

//history缓存中数据的user-id数据还原，以及ds当中数据的还原
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
}

//hitory还原, ds, 还原同时向数据库中插入activation 以及history相关数据
func restoreHisAndDs(userId uuid.UUID, devEui common.EUI64, rdb *redis.Client, db *sqlx.DB) {
	devEuiStr := hex.EncodeToString(devEui[:])
	historyByte := []byte(rdb.HGet(common.DevDeviceHiskey, devEuiStr).Val())
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
	insertIHistory(db, historyMsg)

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

	//着手插入activation表 ========== 分隔 =========
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

//消息插入时无排序要求
func insertIHistory(db *sqlx.DB, historyMsg []common.DeviceHistory) {
	fmt.Println("等待插入的数据",len(historyMsg))
	for index, deviceHistory := range historyMsg {
		deviceHistory.Id = strconv.Itoa(index + 1)
		_, err := db.Exec(`
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
