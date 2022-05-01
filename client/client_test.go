package client

import (
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/gohutool/log4go"
	"testing"
)

/**
* golang-sample源代码，版权归锦翰科技（深圳）有限公司所有。
* <p>
* 文件名称 : client_test.go
* 文件路径 :
* 作者 : DavidLiu
× Email: david.liu@ginghan.com
*
* 创建日期 : 2022/5/1 20:24
* 修改历史 : 1. [2022/5/1 20:24] 创建文件 by LongYong
*/

func init() {
	log4go.LoggerManager.InitWithDefaultConfig()
}

func TestDB(t *testing.T) {
	err := EtcdClient.Init([]string{"192.168.56.101:32379"}, "", "", 30)

	if err == nil {
		fmt.Println("Etcd is connect")
	} else {
		panic("Etcd can not connect")
	}

	go EtcdClient.WatchKeyWithPrefix("/gateway4go/database/cert-data/", func(event *clientv3.Event) {
		Logger.Info("Event : %+v IsCreate %+v IsModify %+v %+v %+v ",
			event.Type, event.IsCreate(), event.IsModify(), event.Kv, event.PrevKv)
	}, clientv3.WithPrevKV())

	fmt.Println("Watch")

	select {}
}
