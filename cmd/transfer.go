package main

import (
	"logCollection/internal/transfer/service"
	"logCollection/pkg/config"
	"sync"
)

func main() {
	//etcdctl put web_log_config '[{"file_path":"runtime/logs/error.log","topic":"error_log"},{"file_path":"runtime/logs/debug.log","topic":"debug_log"}]'
	//etcdctl put web_log_config '[{"file_path":"runtime/logs/error.log","topic":"error_log"}]'
	//jsonStr := `[{"file_path":"runtime/logs/error.log","topic":"error_log"},{"file_path":"runtime/logs/debug.log","topic":"debug_log"}]`
	//etcd.Put(applicationConfig.Log.EtcdKey, jsonStr)

	ws := sync.WaitGroup{}

	ws.Add(1)
	service.NewLogTransfer(config.NewConfig("configs/app.ini")).Run()

	ws.Wait()
}