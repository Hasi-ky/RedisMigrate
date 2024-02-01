package commands

import (
	"batch/common"
	"batch/db/redis"
	"bufio"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"time"
	log "github.com/sirupsen/logrus"
)

type Restorer struct {
	Host           string
	Password       string
	Client         map[uint64]redis.DBClient
	Stream         *os.File
	Count          uint64
	jsonStringList chan string
}

func (r *Restorer) Init() {
	r.jsonStringList = make(chan string, 1)
	r.Client = make(map[uint64]redis.DBClient)
	r.Count = 0
}

func (r *Restorer) Restore() {

	r.readFile()
	for {
		jsonString := r.getLine()
		if jsonString == "" {
			break
		}
		record := &Record{}
		err := json.Unmarshal([]byte(jsonString), &record)
		if err != nil {
			log.Errorf("Unmarshal %s error , %s\n", jsonString, err)
			continue
		}
		b, err := base64.StdEncoding.DecodeString(record.Value)
		if err != nil {
			log.Errorf("base64 decode %s error , %s\n", record.Value, err)
			continue
		}
		record.Value = string(b)
		duration, err := time.ParseDuration(fmt.Sprintf("%ds", record.TTL))
		if err != nil {
			log.Errorf("Parse ttl(%d) error, %s\n", record.TTL, err)
			continue
		}
		client := r.getClient(record.DatabaseId)
		if client == nil {
			continue
		}
		if duration > 0 {
			_, err = client.RestoreReplace(record.Key, duration, record.Value)
		} else {
			_, err = client.RestoreReplace(record.Key, 0, record.Value)
		}
		if err != nil {
			log.Errorf("Restore error , struct: %#v , error: %s\n", record, err)
			break
		}

		r.Count++
		if r.Count%1000 == 0 {
			r.PrintReport()
		}
	}

	r.CloseClients()
	r.CloseStream()

	r.PrintReport()
}

func (r *Restorer) getLine() string {

	var jsonString string

	for {
		select {
		case jsonString = <-r.jsonStringList:
			return jsonString

		default:

			time.Sleep(10 * time.Millisecond)
			continue
		}
	}
}
 
 
func (r *Restorer) getClient(dbId uint64) (client redis.DBClient) {
	var isExist bool
	if client, isExist = r.Client[dbId]; isExist {
		return
	}

	if !common.RedisCluster {
		r.Client[dbId], _ = redis.NewClientForDB(r.Host,r.Password,int(dbId))
	} else {
		server := []string{r.Host}
		r.Client[dbId], _ = redis.NewClusterClient(server,r.Password)
	}
	return r.Client[dbId]
}

func (r *Restorer) readFile() {

	go func(stream *os.File, list chan string) {
		scanner := bufio.NewScanner(stream)
		for scanner.Scan() {

			list <- scanner.Text()
		}
		close(list)
	}(r.Stream, r.jsonStringList)
}

func (r *Restorer) CloseClients() {

	for dbId, client := range r.Client {
		client.CloseSession()
		delete(r.Client, dbId)
	}
}

func (r *Restorer) CloseStream() {

	if r.Stream == nil {

		return
	}

	r.Stream.Close()
	r.Stream = nil
}

func (r *Restorer) PrintReport() {

	log.Infof("Restored %d Record(s).\n", r.Count)
}

func Restore(host, password, path string) {

	fp, err := os.Open(path)
	if err != nil {

		log.Errorf("Open data file error, %s\n", err)
		return
	}
	restorer := &Restorer{
		Host:     host,
		Password: password,
		Stream:   fp,
	}

	restorer.Init()
	restorer.Restore()
}
