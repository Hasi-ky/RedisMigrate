package main

import (
	"batch/common"
	"batch/global"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/gofrs/uuid"
	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	redis7 "github.com/go-redis/redis/v7"
	redis9 "github.com/redis/go-redis/v9"
)

var (
	needUrlGroup       = []string{"mongodb://lora_activeMote:lora_activeMote@%s/lora_activeMote"}
	collection         = map[string]string{}
	collectionForField = map[string][]string{}
	needHistory        *bool
	gatewayUrl         = "mongodb://lora_gateway:lora_gateway@%s/lora_gateway"
	historyUrl         = "mongodb://lora_moteData:lora_moteData@%s/lora_moteData"
	globalDs           common.DeviceSession
	ctx                = context.Background()
)

func init() {
	collection["lora_activeMote"] = "activemotes" //activemotes_0_0
	collection["lora_moteData"] = "motedata"      //motedata_194
	collection["lora_gateway"] = "gateway_routes"
	collectionForField["lora_activeMote"] = []string{"devAddr", "ds"}
	collectionForField["lora_moteData"] = []string{"his"}
}

func helpPrint() {
	log.Infof(`
	Usage:
		Datamigrate [-rH=127.0.0.1:6379] [-rDB=0] [-password=Auth] [-rC=F] [-rV=4] ....
	
	Options:
		-rH=redisHost                 The redis instance (host:port).
		-rDB=redisDBName              The redis DBName use (0-15)
		-rPD=redisPassword            The redis Password
		-rV=common.REDIS_VERSION      The redis version
		-rC=redisCluster              Is Redis a cluster
		-his=needHistory              Is need restore history data
	
	Examples:
		$ Datamigrate -rH=127.0.0.1
		$ Datamigrate -rH=127.0.0.1 -rPD=123445
		$ Datamigrate -rH=127.0.0.1 -rPD=123445
		$ Datamigrate -rH=127.0.0.1 -rPD=123445 -rV=7
		$ Datamigrate -rH=127.0.0.1 -rPD=123445 -rV=7 -his=true
		`)
}

func main() {
	flag.StringVar(&common.RedisHost, "rH", "redis-cluster-svc-np:6379", "-rH=redis-cluster-svc-np:6379")
	flag.StringVar(&common.RedisDBName, "rDB", "0", "-rDB=0")
	flag.StringVar(&common.RedisPwd, "rPD", "", "-rPD=")
	flag.StringVar(&common.REDIS_VERSION, "rV", "", "-rV=4")
	flag.BoolVar(&common.RedisCluster, "rC", false, "-rC=false")
	flag.StringVar(&common.MongoHost, "mH", "mongos-svc:27017", "-mH=mongos-svc:27017")
	help := flag.Bool("help", false, "Display help infomation")
	needHistory = flag.Bool("his", true, "Display help infomation")
	flag.Parse()
	if *help {
		helpPrint()
		return
	}
	global.GetRedisClient()
	defer global.Rdb.CloseSession()
	if *needHistory {
		needUrlGroup = append(needUrlGroup, historyUrl)
	}
	for _, url := range needUrlGroup {
		realUrl := fmt.Sprintf(url, common.MongoHost)
		getDataFromMongo(ctx, realUrl)
	}
	log.Infoln("数据提取完成")
}

func getDataFromMongo(ctx context.Context, url string) {
	strSplit := strings.Split(url, "/")
	dbTableName := strSplit[len(strSplit)-1]
	clientOptions := options.Client().ApplyURI(url)  //这里完成url连接 "mongodb://wlan:wlan@localhost:port/admin"
	client, err := mongo.Connect(ctx, clientOptions) //dataB 如lora_moteCfg // collections 比如 motescfg
	defer client.Disconnect(ctx)
	if err != nil {
		log.Fatal("mongo数据库连接失败", err)
	}
	err = client.Ping(ctx, nil)
	if err != nil {
		log.Fatal("mongo数据库无法连通", err)
	}
	log.Infoln("mongo数据库连接成功!")
	collections, err := client.Database(dbTableName).ListCollectionNames(ctx, bson.D{})
	if err != nil {
		log.Fatalln(dbTableName, "mongo数据库集合查询失败", err)
	}
	for _, colName := range collections { //扫描数据库中对应存在的前缀
		if strings.HasPrefix(colName, collection[dbTableName]) {
			collect := client.Database(dbTableName).Collection(colName)
			log.Infoln("当前检测集合为", colName)
			for _, needValue := range collectionForField[dbTableName] {
				cursor, err := collect.Find(ctx, bson.D{})
				if err != nil {
					log.Errorf("集合[%s]中游标创建失败\n", colName)
					break
				}
				defer cursor.Close(ctx)
				if needValue == "devAddr" { //需要
					dealDevAddr(cursor, needValue)
				} else if needValue == "ds" {
					dealDevSessionAndOther(cursor)
				} else if needValue == "his" {
					dealDevHistory(cursor)
				}
			}
		}
	}
}

//网关信息
func getGatewayInfo(ctx context.Context, redisData *common.TopologyRedisData, gwMac, url, dbName string) error {
	clientOptions := options.Client().ApplyURI(url)  //这里完成url连接 "mongodb://wlan:wlan@localhost:port/admin"
	client, err := mongo.Connect(ctx, clientOptions) //dataB 如lora_moteCfg // collections 比如 motescfg
	if err != nil {
		return errors.New("<getGatewayInfo> mongo数据库连接失败" + err.Error())
	}
	defer client.Disconnect(ctx)
	err = client.Ping(ctx, nil)
	if err != nil {
		log.Fatal(err)
	}
	log.Infoln("<getGatewayInfo> mongo数据库连接成功!")
	collections, err1 := client.Database(dbName).ListCollectionNames(ctx, bson.D{})
	if err1 != nil {
		log.Fatalln(dbName, "mongo数据库集合查询失败", err)
	}
	for _, colName := range collections { //找到有对应的网关信息就直接处理录入返回即可
		if strings.HasPrefix(colName, collection[dbName]) {
			collect := client.Database(dbName).Collection(collection[dbName])
			cursor, err := collect.Find(ctx, bson.D{})
			if err != nil {
				log.Fatal(err)
			}
			defer cursor.Close(ctx)
			for cursor.Next(ctx) {
				var result bson.M
				err = cursor.Decode(&result)
				if err != nil {
					log.Fatal(err)
				}
				if result["gwmac"] != nil {
					if result["gwmac"].(string) == gwMac {
						if result["devSN"] != nil {
							redisData.GatewayName = result["devSN"].(string)
						}
						if result["time"] != nil {
							redisData.Time = result["time"].(primitive.DateTime).Time().Unix()
						}
						return nil
					}
				}
			}
		}
	}
	log.Warnln("<getGatewayInfo> 没有找到对应网关信息")
	return nil
}

//激活数据的构成方式应该为map
//唯一设备映射唯一激活
func getActivationData(result bson.M, pipeline *interface{}) error {
	activationData := common.DeviceActivation{
		DevEUI:      globalDs.DevEUI,
		DevAddr:     globalDs.DevAddr,
		SNwkSIntKey: globalDs.SNwkSIntKey,
		FNwkSIntKey: globalDs.FNwkSIntKey,
		NwkSEncKey:  globalDs.NwkSEncKey,
		JoinReqType: common.JoinRequestType, //默认为首次入网迁移
	}
	if result["createTime"] != nil {
		activationData.CreatedAt = result["createTime"].(primitive.DateTime).Time()
	}
	if result["devNonce"] != nil {
		activationData.DevNonce = common.StringToDevNounce(result["devNonce"].(string))
	}
	var err error
	valueJson, _ := json.Marshal(activationData)
	if common.REDIS_VERSION == "7" {
		_, err = (*pipeline).(redis9.Pipeliner).HSet(ctx, common.DevActivationKey, globalDs.DevEUI.String(), valueJson).Result()
	} else {
		_, err = (*pipeline).(redis7.Pipeliner).HSet(common.DevActivationKey, globalDs.DevEUI.String(), valueJson).Result()
	}
	if err != nil {
		return errors.New("设备激活数据插入错误" + err.Error())
	}
	return nil
}

//设备地址信息提取
func dealDevAddr(cursor *mongo.Cursor, needValue string) {
	log.Infoln("设备地址信息提取开始")
	var addrWithDevEui map[string]map[string]interface{} = make(map[string]map[string]interface{})
	for cursor.Next(ctx) {
		var result bson.M
		err := cursor.Decode(&result)
		if err != nil {
			log.Fatal(err)
		}
		if result[needValue] == nil || result["devEUI"] == nil {
			log.Warnln("设备addr值或设备唯一标识为空，放弃该设备信息处理")
			continue
		}
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
		var (
			str  strings.Builder
			flag = false
		)
		for _, nums := range addsFour { //地址十六进制字符串生成
			b, err1 := strconv.Atoi(nums)
			if err1 != nil {
				log.Errorf("地址转化时出现异常", err1)
				flag = true
			}
			hexString := strconv.FormatInt(int64(b), 16)
			for len(hexString) < 2 {
				hexString = "0" + hexString
			}
			str.WriteString(hexString)
		}
		if flag {
			continue
		}
		key := common.DevAddrKey + str.String()
		for devE, _ := range devEuiSet {
			tempDev, _ := hex.DecodeString(strings.ToLower(devE))
			devByte := common.ByteToEUI(tempDev)
			global.Rdb.Sadd(key, devByte[:])
			global.Rdb.Expire(key, common.DeviceSessionTTL)
		}
	}
	log.Infoln("设备地址信息提取结束")
}

//devicession数据生成
func dealDevSessionAndOther(cursor *mongo.Cursor) {
	log.Infoln("设备会话信息等关联缓存提取开始")
	for cursor.Next(ctx) {
		var (
			result        bson.M
			gwMac, devEUI []byte
			pipeline      interface{}
		)
		pipeline = global.Rdb.TxPipeline()
		err := cursor.Decode(&result)
		if err != nil {
			log.Fatal(err)
		}
		id, _ := uuid.NewV4()
		ds := common.DeviceSession{
			DeviceProfileID:          id,
			ServiceProfileID:         id,
			RoutingProfileID:         id,
			MACVersion:               "1.0.3",
			ExtraUplinkChannels:      make(map[int]common.Channel),
			SkipFCntValidation:       true, //写死
			NbTrans:                  1,
			BandName:                 "CN470", //仅保证中国地区
			Nation:                   "CHINA", //写死
			MaxSupportedTXPowerIndex: 7,
			MinSupportedTXPowerIndex: 0,
			Debug:                    false,
			RXWindow:                 0,
			RmFlag:                   false,
		}
		if result["devAddr"] != nil {
			addrTemp := strings.Split(result["devAddr"].(string), ".")
			byteForAddr := make([]byte, 4)
			for k, v := range addrTemp {
				num, _ := strconv.Atoi(v)
				byteForAddr[k] = byte(num)
			}
			ds.DevAddr = common.ByteToAddr(byteForAddr)
		}
		if result["devEUI"] != nil {
			devEUI, _ = hex.DecodeString(result["devEUI"].(string))
			if len(devEUI) != 0 {
				ds.DevEUI = common.ByteToEUI(devEUI)
			}
		}
		if result["appSKey"] != nil {
			appSKey, _ := hex.DecodeString(result["appSKey"].(string))
			ds.AppSKey = common.ByteToAes(appSKey)
		}
		if result["nwkSKey"] != nil {
			nwkSKey, _ := hex.DecodeString(result["nwkSKey"].(string))
			ds.SNwkSIntKey = common.ByteToAes(nwkSKey)
			ds.FNwkSIntKey = ds.SNwkSIntKey //两部分码不确定
			ds.NwkSEncKey = ds.SNwkSIntKey
		}
		if result["RX1delay"] != nil {
			ds.RXDelay = uint8(result["RX1delay"].(int32))
		}
		if result["RX1DRoffset"] != nil {
			ds.RX1DROffset = uint8(result["RX1DRoffset"].(int32))
		}
		if result["RX2DR"] != nil {
			ds.RX2DR = uint8(result["RX2DR"].(int32))
		}
		if result["RX2Freq"] != nil {
			ds.RX2Frequency = int(result["RX2Freq"].(float64))
		}
		if result["ADR"] != nil {
			ds.ADR = result["ADR"].(bool)
		}
		if result["classMode"] != nil {
			ds.DeviceMode = common.DeviceMode(result["classMode"].(string))
		}
		if result["devType"] != nil {
			ds.DevType = result["devType"].(string)
		}
		if result["FCntDown"] != nil {
			ds.ConfFCnt = uint32(result["FCntDown"].(int32))
		}
		if result["gwmac"] != nil {
			gwMac, _ = hex.DecodeString(result["gwmac"].(string))
		}
		ds.UpdateTime = time.Now()
		b, err := proto.Marshal(common.DeviceSessionToPB(ds))
		if err != nil {
			log.Errorf("设备[%s]压缩编码出现错误,数据格式有误\n", ds.DevEUI.String(), err) //进行下一轮扫描
			continue
		}
		if common.REDIS_VERSION == "7" {
			_, err = pipeline.(redis9.Pipeliner).Set(ctx, common.DevDeviceKey+ds.DevEUI.String(), b, common.DeviceSessionTTL).Result()
		} else {
			_, err = pipeline.(redis7.Pipeliner).Set(common.DevDeviceKey+ds.DevEUI.String(), b, common.DeviceSessionTTL).Result()
		}
		if err != nil {
			log.Errorf("设备[%s]设置deviceSession异常，%v\n", ds.DevEUI.String(), err)
			continue
		}
		globalDs = ds //全局备份
		lowerGw := strings.ToLower(result["gwmac"].(string))
		err = devToGateway(gwMac, result, &pipeline)
		if err != nil {
			log.Errorln(err)
			continue
		}
		err = gatewayTopoDev(result, &pipeline, lowerGw)
		if err != nil {
			log.Errorln(err)
			continue
		}
		err = devTopoGateway(result, &pipeline, lowerGw)
		if err != nil {
			log.Errorln(err)
			continue
		}
		err = gateWayMixedDev(result, &pipeline, lowerGw)
		if err != nil {
			log.Errorln(err)
			continue
		}
		err = getActivationData(result, &pipeline)
		if err != nil {
			log.Errorln(err)
			continue
		}
		if common.REDIS_VERSION == "7" {
			_, err = pipeline.(redis9.Pipeliner).Exec(ctx)
		} else {
			_, err = pipeline.(redis7.Pipeliner).Exec()
		}
		if err != nil {
			log.Fatal("事务写入Redis存在异常", err)
		}
	}
	log.Infoln("设备会话信息等关联缓存提取结束")
}

//设备对应网关信息生成
func devToGateway(gwMac []byte, result primitive.M, pipeline *interface{}) error {
	gwrxKey := common.DevDeviceKey + globalDs.DevEUI.String() + common.DevDeviceSuffixKey
	rxInfoSet := common.DeviceGatewayRXInfoSet{
		DevEUI: globalDs.DevEUI,
		DR:     globalDs.DR,
	}
	item := common.DeviceGatewayRXInfo{
		GatewayID: common.ByteToEUI(gwMac),
	}
	if result["rssi"] != nil {
		item.RSSI = int(result["rssi"].(int32))
	}
	if result["lsnr"] != nil {
		item.LoRaSNR = float64(result["lsnr"].(int32))
	}
	//其余参数制空 --- 风险在于其中context参数
	rxInfoSet.Items = append(rxInfoSet.Items, item)
	brx, err := proto.Marshal(common.DeviceGatewayRXInfoSetToPB(rxInfoSet))
	if err != nil {
		return errors.New("<设备至网关> 压缩编码出现错误")
	}
	if common.REDIS_VERSION == "7" {
		_, err = (*pipeline).(redis9.Pipeliner).Set(ctx, gwrxKey, brx, common.DeviceSessionTTL).Result()
	} else {
		_, err = (*pipeline).(redis7.Pipeliner).Set(gwrxKey, brx, common.DeviceSessionTTL).Result()
	}
	if err != nil {
		return errors.New("<设备至网关> redis存在异常")
	}
	return nil
}

//topo 网关下辖 设备
func gatewayTopoDev(result primitive.M, pipeline *interface{}, lowerGw string) error {
	var err error
	gwTopoDevKey := common.DevDeviceGwTopoKey + lowerGw
	if common.REDIS_VERSION == "7" {
		_, err = (*pipeline).(redis9.Pipeliner).SAdd(ctx, gwTopoDevKey, globalDs.DevEUI.String()).Result()
		(*pipeline).(redis9.Pipeliner).Expire(ctx, gwTopoDevKey, common.DeviceSessionSevenTTL)
	} else {
		_, err = (*pipeline).(redis7.Pipeliner).SAdd(gwTopoDevKey, globalDs.DevEUI.String()).Result()
		(*pipeline).(redis7.Pipeliner).Expire(gwTopoDevKey, common.DeviceSessionSevenTTL)
	}
	if err != nil {
		return errors.New(err.Error() + "网关关联至设备缓存异常")
	}
	return nil
}

//设备上联网关
func devTopoGateway(result primitive.M, pipeline *interface{}, lowerGw string) error {
	var err error
	devTopoGwKey := common.DevDeviceDevTopoKey + globalDs.DevEUI.String()
	if common.REDIS_VERSION == "7" {
		_, err = (*pipeline).(redis9.Pipeliner).SAdd(ctx, devTopoGwKey, lowerGw).Result()
		(*pipeline).(redis9.Pipeliner).Expire(ctx, devTopoGwKey, common.DeviceSessionSevenTTL)
	} else {
		_, err = (*pipeline).(redis7.Pipeliner).SAdd(devTopoGwKey, lowerGw).Result()
		(*pipeline).(redis7.Pipeliner).Expire(devTopoGwKey, common.DeviceSessionSevenTTL)
	}
	if err != nil {
		return errors.New(err.Error() + "设备关联至网关缓存异常")
	}
	return nil
}

//网关设备关联处
func gateWayMixedDev(result primitive.M, pipeline *interface{}, lowerGw string) error {
	data := common.TopologyRedisData{
		DevMode: string(globalDs.DeviceMode),
	}
	if result["rssi"] != nil {
		data.Rssi = strconv.Itoa(int(result["rssi"].(int32)))
	}
	gatewayURL := fmt.Sprintf(gatewayUrl, common.MongoHost)
	err := getGatewayInfo(ctx, &data, result["gwmac"].(string), gatewayURL, "lora_gateway")
	if err != nil {
		return err
	}
	valueJson, _ := json.Marshal(data)
	devTopoWithGwKey := common.DevDeviceTopoKey + lowerGw + common.DevSeparator + globalDs.DevEUI.String()
	if common.REDIS_VERSION == "7" {
		_, err = (*pipeline).(redis9.Pipeliner).Set(ctx, devTopoWithGwKey, valueJson, common.DeviceSessionTTL).Result()
	} else {
		_, err = (*pipeline).(redis7.Pipeliner).Set(devTopoWithGwKey, valueJson, common.DeviceSessionTTL).Result()
	}
	if err != nil {
		return errors.New("设备网关混合数据设置失败" + err.Error())
	}
	return nil
}

func dealDevHistory(cursor *mongo.Cursor) {
	log.Infoln("设备历史数据缓存提取开始")
	hisCount := 0
	history := make(map[string][]common.DeviceHistory, 0)
	for cursor.Next(ctx) { //游标扫描全部数据
		var (
			result       bson.M
			devEUI       string
			devEUIForHis []common.DeviceHistory
			ok           bool
		)
		err := cursor.Decode(&result)
		if err != nil {
			log.Fatal(err)
		}
		if result["devEUI"] != nil {
			devEUI = strings.ToLower(result["devEUI"].(string))
		} else {
			continue
		}
		if devEUIForHis, ok = history[devEUI]; !ok {
			devEUIForHis = make([]common.DeviceHistory, 0)
		}
		gwMac := "0000000000000000" //意外防治
		if result["gwmac"] != nil {
			gwMac = strings.ToLower(result["gwmac"].(string))
		}
		gwMacToByte, _ := hex.DecodeString(gwMac)
		devEUIToByte, _ := hex.DecodeString(devEUI)
		hisCount++
		devEUIForHis = append(devEUIForHis, common.DeviceHistory{
			Id:         strconv.Itoa(hisCount),
			GatewayMac: common.ByteToEUI(gwMacToByte),
			DevEui:     common.ByteToEUI(devEUIToByte),
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
			devEUIForHis[szOfHistory].CodeRate = result["coding"].(string)
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
		if result["freq"] != nil {
			devEUIForHis[szOfHistory].Freq = result["freq"].(float64)
		}
		if result["SF"] != nil {
			devEUIForHis[szOfHistory].Sf = int(result["SF"].(int32))
		}
		if result["rfch"] != nil {
			devEUIForHis[szOfHistory].Rfch = strconv.Itoa(int(result["rfch"].(int32)))
		}
		if result["modulation"] != nil {
			devEUIForHis[szOfHistory].Modulation = result["modulation"].(string)
		}
		if result["type"] != nil {
			devEUIForHis[szOfHistory].Type = result["type"].(string)
		}
		if result["time"] != nil {
			devEUIForHis[szOfHistory].Time = (result["time"].(primitive.DateTime)).Time()
		}
		if result["direction"] != nil {
			devEUIForHis[szOfHistory].Direction = result["direction"].(string)
		}
		if result["BW"] != nil {
			devEUIForHis[szOfHistory].Bw = int(result["BW"].(int32))
		}
		history[devEUI] = devEUIForHis
	}
	for dev, data := range history { //存在就加入，不存在就直接生成
		log.Infof("设备devEUI:%s 且其对应的数据量为:%v\n", dev, len(data))
		isExist, _ := global.Rdb.HExists(common.DevDeviceHiskey, dev)
		var valueJson []byte
		if isExist {
			var tempExistHistory []common.DeviceHistory
			existData, _ := global.Rdb.HGet(common.DevDeviceHiskey, dev)
			json.Unmarshal(existData, &tempExistHistory)
			tempExistHistory = append(tempExistHistory, data...)
			valueJson, _ = json.Marshal(tempExistHistory)
			log.Infof("设备[%s]历史数据数据量为:%v\n", dev, len(tempExistHistory))
		} else {
			valueJson, _ = json.Marshal(data)
			log.Infof("设备[%s]历史数据数据量为:%v\n", dev, len(data))
		}
		err := global.Rdb.HSet(common.DevDeviceHiskey, dev, valueJson)
		if err != nil {
			log.Errorf("设备[%s]设置数据进入redis发生错误", err)
		}
	}
	log.Infoln("设备历史数据缓存提取结束")
}
