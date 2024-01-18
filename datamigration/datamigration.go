package main

import (
	"batch/common"
	"bytes"
	"context"
	"encoding/gob"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/gofrs/uuid"
	"github.com/golang/protobuf/proto"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

//总体按照数据库的形式来分类
const (
	deviceSessionTTL      = time.Hour * 24 * 31
	deviceSessionSevenTTL = time.Hour * 24 * 7
)

var (
	devAddrKey          = "lora:ns:devaddr:" //该键值配合地址信息 value=set集合形式(devEui)  === lora_moteCfg中可以取到
	devDeviceKey        = "lora:ns:device:"
	devDeviceSuffixKey  = ":gwrx"
	devDeviceGwTopoKey  = "lora:topo:gw:"
	devDeviceDevTopoKey = "lora:topo:dev:"
	devDeviceTopoKey    = "lora:topo:"
	devDeviceHiskey     = "lora:his"
	devSeparator        = ":"
	needUrlGroup        = []string{"mongodb://lora_activeMote:lora_activeMote@%s/lora_activeMote",
		"mongodb://lora_moteData:lora_moteData@%s/lora_moteData"}
	collection         = map[string]string{}
	collectionForField = map[string][]string{}
	MongoHost          string
	MongoPort          string
	gatewayUrl         = "mongodb://lora_gateway:lora_gateway@%s/lora_gateway"
	globalDs           common.DeviceSession
)

func init() {
	collection["lora_activeMote"] = "activemotes" //activemotes_0_0
	collection["lora_moteData"] = "motedata"      //motedata_194
	collection["lora_gateway"] = "gateway_routes"
	collectionForField["lora_activeMote"] = []string{"devAddr", "ds"}
	collectionForField["lora_moteData"] = []string{"his"}
}

func main() {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr: "redis-cluster-svc-np:6379",
	})
	err := rdb.Ping().Err()
	if err != nil {
		fmt.Println("redis连接失败")
		log.Fatal(err)
	}
	defer rdb.Close()
	fmt.Println("redis数据库连接成功!")
	for _, url := range needUrlGroup {
		realUrl := fmt.Sprintf(url, "mongos-svc:27017")
		getValueFromMongo(ctx, realUrl, rdb)
	}
	fmt.Println("数据提取结束")
}

func getValueFromMongo(ctx context.Context, url string, rdb *redis.Client) {
	strSplit := strings.Split(url, "/")
	dbTableName := strSplit[len(strSplit)-1]
	clientOptions := options.Client().ApplyURI(url)  //这里完成url连接 "mongodb://wlan:wlan@localhost:port/admin"
	client, err := mongo.Connect(ctx, clientOptions) //dataB 如lora_moteCfg // collections 比如 motescfg
	if err != nil {
		fmt.Println("mongo数据库连接失败!")
		log.Fatal(err)
	}
	defer client.Disconnect(ctx)
	err = client.Ping(ctx, nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("mongo数据库连接成功!")
	//这个位置应该扫描是具有其特定前缀的内容
	collections, err := client.Database(dbTableName).ListCollectionNames(ctx, bson.D{})
	if err != nil {
		fmt.Println(dbTableName, "mongo数据库集合查询失败")
		log.Fatal(err)
	}
	for _, colName := range collections { //扫描数据库中对应存在的前缀
		if strings.HasPrefix(colName, collection[dbTableName]) {
			collect := client.Database(dbTableName).Collection(colName)
			fmt.Println("当前集合", collect)
			for _, needValue := range collectionForField[dbTableName] {
				cursor, err := collect.Find(ctx, bson.D{})
				if err != nil {
					fmt.Println("mongo中游标建立错误")
					log.Fatal(err)
				}
				defer cursor.Close(ctx)
				if needValue == "devAddr" { //需要
					var addrWithDevEui map[string]map[string]interface{} = make(map[string]map[string]interface{})
					for cursor.Next(ctx) {
						var result bson.M
						err := cursor.Decode(&result)
						if err != nil {
							log.Fatal(err)
						}
						fmt.Println("打印基础数据信息")
						fmt.Println(result)
						addr, devEui := result[needValue].(string), result["devEUI"].(string)
						if addr != "" {
							if _, ok := addrWithDevEui[string(addr)]; !ok {
								addrWithDevEui[addr] = make(map[string]interface{})
							}
							addrWithDevEui[addr][devEui] = true
						}
					}
					for addr, devEuiSet := range addrWithDevEui {
						addsFour := strings.Split(addr, ".")
						var str strings.Builder
						for _, nums := range addsFour { //地址十六进制字符串生成
							b, err1 := strconv.Atoi(nums)
							if err1 != nil {
								log.Fatal(err1)
							}
							hexString := strconv.FormatInt(int64(b), 16)
							for len(hexString) < 2 {
								hexString = "0" + hexString
							}
							str.WriteString(hexString)
						}
						key := devAddrKey + str.String()
						pipe := rdb.TxPipeline()
						for devE, _ := range devEuiSet {
							temp, _ := hex.DecodeString(strings.ToLower(devE))
							devByte := common.ByteToEUI(temp)
							pipe.SAdd(key, devByte[:])
							pipe.Expire(key, deviceSessionTTL)
						}
						if _, err := pipe.Exec(); err != nil {
							fmt.Println("执行redis存储失败")
							log.Fatal(err)
						}
					}
				} else if needValue == "ds" {
					for cursor.Next(ctx) {
						var result bson.M
						err := cursor.Decode(&result)
						if err != nil {
							log.Fatal(err)
						}
						fmt.Println("打印基础数据信息")
						fmt.Println(result)
						id, _ := uuid.NewV4()
						devAddrDot, _ := result["devAddr"].(string)
						devEUI, _ := hex.DecodeString(result["devEUI"].(string))
						appSKey, _ := hex.DecodeString(result["appSKey"].(string))
						nwkSKey, _ := hex.DecodeString(result["nwkSKey"].(string))
						gwmac, _ := hex.DecodeString(result["gwmac"].(string))
						addrTemp := strings.Split(devAddrDot, ".")
						byteForAddr := make([]byte, 4)
						for k, v := range addrTemp {
							num, _ := strconv.Atoi(v)
							byteForAddr[k] = byte(num)
						}
						ds := common.DeviceSession{ //采用activation中内容
							DeviceProfileID:          id,
							ServiceProfileID:         id,
							RoutingProfileID:         id,
							MACVersion:               "1.0.3",
							RXDelay:                  uint8(result["RX1delay"].(int32)),
							RX1DROffset:              uint8(result["RX1DRoffset"].(int32)),
							RX2DR:                    uint8(result["RX2DR"].(int32)),
							RX2Frequency:             int(result["RX2Freq"].(float64)),
							ExtraUplinkChannels:      make(map[int]common.Channel),
							SkipFCntValidation:       true, //写死
							NbTrans:                  1,
							BandName:                 "CN470", //仅保证中国地区
							Nation:                   "CHINA", //写死
							ADR:                      result["ADR"].(bool),
							MaxSupportedTXPowerIndex: 7,
							MinSupportedTXPowerIndex: 0,
							DeviceMode:               common.DeviceMode(result["classMode"].(string)),
							DevType:                  result["devType"].(string),
							Debug:                    false,
							RXWindow:                 0,
							RmFlag:                   false,
							ConfFCnt:                 uint32(result["FCntDown"].(int32)),
						}
						ds.UpdateTime = time.Now()
						ds.DevAddr = common.ByteToAddr(byteForAddr)
						if len(devEUI) != 0 {
							ds.DevEUI = common.ByteToEUI(devEUI)
						}
						if len(appSKey) != 0 {
							ds.AppSKey = common.ByteToAes(appSKey)
						}
						if len(nwkSKey) != 0 {
							ds.SNwkSIntKey = common.ByteToAes(nwkSKey)
							ds.FNwkSIntKey = ds.SNwkSIntKey //两部分码不确定
							ds.NwkSEncKey = ds.SNwkSIntKey
						}
						b, err := proto.Marshal(common.DeviceSessionToPB(ds))
						if err != nil {
							fmt.Println("压缩编码出现错误")
							log.Fatal(err)
						}
						err = rdb.Set(devDeviceKey+ds.DevEUI.String(), b, deviceSessionTTL).Err()
						if err != nil {
							fmt.Println("设置值到redis存在异常")
							log.Fatal(err)
						}
						globalDs = ds //全局备份后续使用
						//生成对应设备网关信息 =====================================gwrx
						gwrxKey := devDeviceKey + ds.DevEUI.String() + devDeviceSuffixKey
						rxInfoSet := common.DeviceGatewayRXInfoSet{
							DevEUI: ds.DevEUI,
							DR:     ds.DR,
						}
						//其余参数制空 --- 风险在于其中context参数
						rxInfoSet.Items = append(rxInfoSet.Items, common.DeviceGatewayRXInfo{
							GatewayID: common.ByteToEUI(gwmac),
							RSSI:      int(result["rssi"].(int32)),
							LoRaSNR:   float64(result["lsnr"].(int32)),
						})
						brx, err := proto.Marshal(common.DeviceGatewayRXInfoSetToPB(rxInfoSet))
						if err != nil {
							fmt.Println("网关压缩编码出现错误")
							log.Fatal(err)
						}
						err = rdb.Set(gwrxKey, brx, deviceSessionTTL).Err()
						if err != nil {
							fmt.Println("网关设置值到redis存在异常")
							log.Fatal(err)
						}
						lowerGw := strings.ToLower(result["gwmac"].(string))
						//topo 网关下辖 设备
						gwTopoDevKey := devDeviceGwTopoKey + lowerGw
						err = rdb.SAdd(gwTopoDevKey, hex.EncodeToString(devEUI[:])).Err()
						if err != nil {
							fmt.Println("网关关联至设备缓存异常")
						}
						rdb.Expire(gwTopoDevKey, deviceSessionSevenTTL)

						//设备上联网关
						devTopoGwKey := devDeviceDevTopoKey + hex.EncodeToString(devEUI[:])
						err = rdb.SAdd(devTopoGwKey, lowerGw).Err()
						if err != nil {
							fmt.Println("设备关联至网关缓存异常")
						}
						rdb.Expire(devTopoGwKey, deviceSessionSevenTTL)

						//网关设备关联处 userid缺失 //去其他数据库继续搜索
						data := common.TopologyRedisData{
							DevMode: string(ds.DeviceMode),
							Rssi:    strconv.Itoa(int(result["rssi"].(int32))),
						}
						gatewayURL := fmt.Sprintf(gatewayUrl, "mongos-svc:27017")
						getGatewayInfo(ctx, &data, result["gwmac"].(string), gatewayURL, "lora_gateway")
						var buf bytes.Buffer
						if err := gob.NewEncoder(&buf).Encode(data); err != nil {
							fmt.Println("设备与网关混合数据转化失败")
							log.Fatal(err)
						}
						valueJson, _ := json.Marshal(data)
						devTopoWithGwKey := devDeviceTopoKey + lowerGw + devSeparator + hex.EncodeToString(devEUI[:])
						err = rdb.Set(devTopoWithGwKey, valueJson, deviceSessionTTL).Err()
						if err != nil {
							fmt.Println("设备网关混合数据设置失败")
						}
					}
				} else if needValue == "his" {
					//key : devEui
					history := make(map[string][]common.DeviceHistory, 0)
					for cursor.Next(ctx) { //游标扫描全部数据
						var result bson.M
						err := cursor.Decode(&result)
						if err != nil {
							log.Fatal(err)
						}
						devEUI := strings.ToLower(result["devEUI"].(string))
						var (
							devEUIForHis []common.DeviceHistory
							ok           bool
						)
						if devEUIForHis, ok = history[devEUI]; !ok {
							devEUIForHis = make([]common.DeviceHistory, 0)
						}
						gwMac := "0000000000000000"
						if result["gwmac"] != nil {
							gwMac = strings.ToLower(result["gwmac"].(string))
						}
						gwMacToByte, _ := hex.DecodeString(gwMac)
						devEUIToByte, _ := hex.DecodeString(devEUI)
						devEUIForHis = append(devEUIForHis, common.DeviceHistory{
							Freq:       result["freq"].(float64),
							Sf:         int(result["SF"].(int32)),
							Rfch:       strconv.Itoa(int(result["rfch"].(int32))),
							Modulation: result["modulation"].(string),
							Type:       result["type"].(string),
							Time:       (result["time"].(primitive.DateTime)).Time(),
							Direction:  result["direction"].(string),
							GatewayMac: common.ByteToEUI(gwMacToByte),
							DevEui:     common.ByteToEUI(devEUIToByte),
							Bw:         int(result["BW"].(int32)),
						})
						szOfHistory := len(devEUIForHis) - 1
						if result["seq"] != nil {
							devEUIForHis[szOfHistory].Seq = int(result["seq"].(int32))
						}
						if result["chan"] != nil {
							devEUIForHis[szOfHistory].Chan = strconv.Itoa(int(result["chan"].(int32)))
						}
						if result["rssi"] != nil {
							devEUIForHis[szOfHistory].Rssi = strconv.Itoa(int(result["rssi"].(int32)))
						}
						if result["lsnr"] != nil {
							devEUIForHis[szOfHistory].Lsnr = strconv.Itoa(int(result["lsnr"].(int32)))
						}
						if result["port"] != nil {
							devEUIForHis[szOfHistory].Port = strconv.Itoa(int(result["port"].(int32)))
						}
						if result["coding"] != nil {
							devEUIForHis[szOfHistory].Port = result["coding"].(string)
						}
						if result["rssiOptimize"] != nil && result["snrOptimize"] != nil {
							devEUIForHis[szOfHistory].Needopt = result["rssiOptimize"].(bool) || result["snrOptimize"].(bool)
						}
						if result["content"] != nil {
							devEUIForHis[szOfHistory].Content = append(devEUIForHis[szOfHistory].Content, result["content"].(string))
						}
						if result["ADR"] != nil {
							devEUIForHis[szOfHistory].Adr = strconv.FormatBool((result["ADR"].(bool)))
						}
						history[devEUI] = devEUIForHis
					}
					for dev, data := range history { //存在就加入，不存在就直接生成
						fmt.Printf("设备devEUI:%s 且其对应的数据量为:%v\n", dev, len(data))
						isExist, _ := rdb.HExists(devDeviceHiskey, dev).Result()
						var valueJson []byte
						if isExist {
							var tempExistHistory []common.DeviceHistory
							existData := []byte(rdb.HGet(devDeviceHiskey, dev).Val())
							json.Unmarshal(existData, &tempExistHistory)
							tempExistHistory = append(tempExistHistory, data...)
							valueJson, _ = json.Marshal(tempExistHistory)
							fmt.Println("下看数据量为:", len(tempExistHistory))
						} else {
							fmt.Println("下看数据量为:", len(data))
							valueJson, _ = json.Marshal(data)
						}
						rdb.HSet(devDeviceHiskey, dev, valueJson)
					}
				}
			}
		}
	}
}

func getGatewayInfo(ctx context.Context, redisData *common.TopologyRedisData, gwMac, url, dbName string) {
	clientOptions := options.Client().ApplyURI(url)  //这里完成url连接 "mongodb://wlan:wlan@localhost:port/admin"
	client, err := mongo.Connect(ctx, clientOptions) //dataB 如lora_moteCfg // collections 比如 motescfg
	if err != nil {
		fmt.Println("<getGatewayInfo> mongo数据库连接失败!")
		log.Fatal(err)
	}
	defer client.Disconnect(ctx)
	err = client.Ping(ctx, nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("<getGatewayInfo> mongo数据库连接成功!")
	collect := client.Database(dbName).Collection(collection[dbName])
	fmt.Println("当前集合", collect)
	cursor, err := collect.Find(ctx, bson.D{})
	if err != nil {
		log.Fatal(err)
	}
	defer cursor.Close(ctx)
	for cursor.Next(ctx) {
		var result bson.M
		err := cursor.Decode(&result)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("<getGatewayInfo> 打印基础数据信息:", result)
		if result["gwmac"].(string) == gwMac {
			redisData.GatewayName = result["devSN"].(string)
			redisTime := result["time"].(primitive.DateTime)
			redisData.Time = redisTime.Time().Unix()
			return
		}
	}
	fmt.Println("<getGatewayInfo> 没有找到对应网关!")
}
