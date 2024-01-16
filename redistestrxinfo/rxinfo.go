package main

import (
	"fmt"
	"log"

	"github.com/go-redis/redis/v7"
	"github.com/golang/protobuf/proto"
)

type EUI64 [8]byte

// DeviceGatewayRXInfoSet contains the rx-info set of the receiving gateways
// for the last uplink.
type DeviceGatewayRXInfoSet struct {
	DevEUI EUI64
	DR     int
	Items  []DeviceGatewayRXInfo
}

// DeviceGatewayRXInfo holds the meta-data of a gateway receiving the last
// uplink message.
type DeviceGatewayRXInfo struct {
	GatewayID EUI64
	RSSI      int
	LoRaSNR   float64
	Antenna   uint32
	Board     uint32
	Context   []byte
	RfChain   uint32
}

func main() {
	var rxInfoSetPB DeviceGatewayRXInfoSetPB
	rdb := redis.NewClient(&redis.Options{
		Addr: "redis-svc:6379",
	})
	fmt.Println("redis连接成功")
	b, _ := rdb.Get("lora:ns:device:38ad8efffe618e4c:gwrx").Bytes()
	err := proto.Unmarshal(b, &rxInfoSetPB)
	if err != nil {
		log.Fatal(err)
	}
	dgrs := deviceGatewayRXInfoSetFromPB(&rxInfoSetPB)
	fmt.Println(dgrs.DR)
	fmt.Println(dgrs.DevEUI)
	fmt.Println(dgrs.Items[0].GatewayID)
	fmt.Println(dgrs.Items[0].LoRaSNR)
	fmt.Println(dgrs.Items[0].RSSI)
	fmt.Println(dgrs.Items[0].Antenna)
	fmt.Println(dgrs.Items[0].Board)
	fmt.Println(dgrs.Items[0].Context)
}

func deviceGatewayRXInfoSetFromPB(d *DeviceGatewayRXInfoSetPB) DeviceGatewayRXInfoSet {
	out := DeviceGatewayRXInfoSet{
		DR: int(d.Dr),
	}
	copy(out.DevEUI[:], d.DevEui)
	for i := range d.Items {
		var id EUI64
		copy(id[:], d.Items[i].GatewayId)
		out.Items = append(out.Items, DeviceGatewayRXInfo{
			GatewayID: id,
			RSSI:      int(d.Items[i].Rssi),
			LoRaSNR:   d.Items[i].LoraSnr,
			Board:     d.Items[i].Board,
			Antenna:   d.Items[i].Antenna,
			Context:   d.Items[i].Context,
		})
	}
	return out
}
