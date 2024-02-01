package commands

import (
	"batch/common"
	"batch/db/redis"
	"batch/global"
	"encoding/base64"
	"encoding/json"
	"os"
	"strings"

	log "github.com/sirupsen/logrus"
)

type Dumper struct {
	Client     redis.DBClient
	Path       string
	DatabaseId uint64
	stream     *os.File
	Count      uint64
}

func (d *Dumper) Dump() {
	cursor := uint64(0)
	keys, err := d.scan(cursor)
	if err != nil {
		log.Errorf("Error: Scan keys error, %s\n", err)
		return
	}
	for _, key := range keys {
		if strings.Contains(key, "lora") {
			record := &Record{Key: key}
			record.Value, err = d.getSerializeString(key)
			if err != nil {

				log.Errorf("Error: Get key serialize string error, %s\n", err)
				break
			}

			record.TTL, err = d.getTTL(key)
			if err != nil {

				log.Errorf("Error: Get key ttl error, %s\n", err)
				break
			}

			record.DatabaseId = d.DatabaseId

			d.writeRecord(record)
			d.Count++

			if d.Count%1000 == 0 {
				d.PrintReport()
			}
		}
	}
	d.CloseStream()
	d.CloseClient()
	d.PrintReport()
}

func (d *Dumper) CloseStream() {

	if d.stream == nil {
		return
	}

	d.stream.Close()
	d.stream = nil
}

func (d *Dumper) scan(cursor uint64) (keys []string, err error) {
	keys, err = d.Client.Scan(cursor, "", 100)
	return
}

func (d *Dumper) getSerializeString(key string) (value string, err error) {
	value, err = d.Client.Dump(key)
	return
}

func (d *Dumper) getTTL(key string) (ttl int64, err error) {
	duration, err := d.Client.TTL(key)
	ttl = int64(duration.Seconds())
	return
}

func (d *Dumper) writeRecord(record *Record) {
	if !d.initWriter() {
		return
	}
	record.Value = base64.StdEncoding.EncodeToString([]byte(record.Value))
	jsonBytes, err := json.Marshal(record)
	if err != nil {
		log.Errorf("Marshal data error , %s\n", err)
		return
	}
	d.stream.Write(jsonBytes)
	d.stream.WriteString("\n")
}

func (d *Dumper) initWriter() bool {

	if d.stream != nil {

		return true
	}

	fs, err := os.Create(d.Path)
	if err != nil {

		log.Errorf("Init file error , %s\n", err)
		return false
	}

	d.stream = fs
	return true
}

func (d *Dumper) CloseClient() {
	d.Client.CloseSession()
}

func (d *Dumper) PrintReport() {

	log.Infof("DB %d dumped %d Record(s).\n", d.DatabaseId, d.Count)
}

func Dump(host, password, path string, databaseCount uint64) {
	if common.RedisCluster {
		dumper := &Dumper{
			Client:     global.Rdb,
			Path:       path,
			DatabaseId: 0,
		}
		dumper.Dump()
	} else {
		var currentDatabase uint64
		for currentDatabase = 0; currentDatabase < databaseCount; currentDatabase++ {
			dumper := &Dumper{
				Client:     global.Rdb,
				Path:       path,
				DatabaseId: currentDatabase,
			}
			dumper.Dump()
		}
	}
}
