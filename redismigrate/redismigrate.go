package main

import (
	"batch/common"
	"batch/global"
	"batch/redismigrate/commands"
	"flag"
	"fmt"
	"strconv"

	log "github.com/sirupsen/logrus"
)

const ModeDump = "dump"
const ModeRestore = "restore"

func main() {
	flag.StringVar(&common.Mode, "mode", "", "-mode=[dump|restore]")
	flag.StringVar(&common.RedisHost, "host", "redis-svc:6379", "-host=127.0.0.1:6379")
	flag.StringVar(&common.RedisPwd, "password", "", "-password=your_password")
	flag.StringVar(&common.Output, "output", "dump.json", "-output=/path/to/file")
	flag.StringVar(&common.Input, "input", "dump.json", "-input=/path/to/file")
	flag.StringVar(&common.DatabaseCountString, "database-count", "16", "-database-count=16")
	flag.BoolVar(&common.RedisCluster, "rC", false, "-rC=false")
	flag.StringVar(&common.REDIS_VERSION, "rV", "4", "-rV=4")
	help := flag.Bool("help", false, "Display help infomation")
	flag.Parse()
	if *help {
		printHelp()
	} else {
		global.GetRedisClient()
		defer global.Rdb.CloseSession()
		if common.Mode == ModeDump {
			var databaseCount uint64
			if common.DatabaseCountString != "" {
				var err error
				databaseCount, err = strconv.ParseUint(common.DatabaseCountString, 10, 64)
				if err != nil {
					log.Printf("Parse database-count err, %s\n", err)
					return
				}
			}
			commands.Dump(common.RedisHost, common.RedisPwd, common.Output, databaseCount)
		} else if common.Mode == ModeRestore {
			commands.Restore(common.RedisHost, common.RedisPwd, common.Input)
		} else {
			log.Warnln("请键入正确的运行模式")
		}
	}

}
func printHelp() {

	fmt.Print(`
Usage:
	dumper -mode=[dump|restore] -host=127.0.0.1:6379 [-password=Auth] [-database-count=16] [-output=/path/to/file] [-input=/path/to/file] [-rC=true]

Options:
	-mode=MODE                        Select dump mode, or restore mode. Options: Dump, Restore.
	-host=NODE                        The redis instance (host:port).
	-password=PASSWORD                The redis authorization password, if empty then no use this parameter.
	-input=FILE                       Use for restore data file.
	-output=FILE                      Use for save the dump data file.
	-rC=true                          The redis instance cluster
	-rV=7                             The redis-server version is 7

Examples:
	$ redis-dump-restore -mode=dump
	$ redis-dump-restore -mode=dump -host=127.0.0.1:6379
	$ redis-dump-restore -mode=dump -host=127.0.0.1:6379 -output=/tmp/dump.json
	$ redis-dump-restore -mode=dump -host=127.0.0.1:6379 -password=Password -output=/tmp/dump.json
	$ redis-dump-restore -mode=restore
	$ redis-dump-restore -mode=restore -host=127.0.0.1:6379
	$ redis-dump-restore -mode=restore -host=127.0.0.1:6379 -input=/tmp/dump.json
	$ redis-dump-restore -mode=restore -host=127.0.0.1:6379 -password=Password -input=/tmp/dump.json
`)
}
