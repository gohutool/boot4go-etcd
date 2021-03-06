package client

import (
	"encoding/json"
	"errors"
	"fmt"
	fastjson "github.com/gohutool/boot4go-fastjson"
	"github.com/gohutool/boot4go-util"
	"github.com/gohutool/log4go"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"golang.org/x/net/context"
	"os"
	"reflect"
	"strconv"
	"time"
)

/**
* golang-sample源代码，版权归锦翰科技（深圳）有限公司所有。
* <p>
* 文件名称 : client.go
* 文件路径 :
* 作者 : DavidLiu
× Email: david.liu@ginghan.com
*
* 创建日期 : 2022/5/1 10:46
* 修改历史 : 1. [2022/5/1 10:46] 创建文件 by LongYong
*/

var logger = log4go.LoggerManager.GetLogger("gohutool.etcd4go.client")

const (
	DEFAULT_DIAL_TIMEOUT  = 3 * time.Second
	DEFAULT_READ_TIMEOUT  = 3 * time.Second
	DEFAULT_WRITE_TIMEOUT = 3 * time.Second
	DATA_TTL              = 1800
)

type SortOrder clientv3.SortOrder
type SortTarget clientv3.SortTarget

const (
	SortNone    SortOrder = SortOrder(clientv3.SortNone)
	SortAscend  SortOrder = SortOrder(clientv3.SortAscend)
	SortDescend SortOrder = SortOrder(clientv3.SortDescend)
)

const (
	SortByKey            SortTarget = SortTarget(clientv3.SortByKey)
	SortByVersion        SortTarget = SortTarget(clientv3.SortByVersion)
	SortByCreateRevision SortTarget = SortTarget(clientv3.SortByCreateRevision)
	SortByModRevision    SortTarget = SortTarget(clientv3.SortByModRevision)
	SortByValue          SortTarget = SortTarget(clientv3.SortByValue)
)

var SortMode = sortMode{}

type sortMode struct {
	SortOrder  SortOrder
	SortTarget SortTarget
}

func (s sortMode) New(target SortTarget, order SortOrder) sortMode {
	sm := sortMode{}
	sm.SortOrder = order
	sm.SortTarget = target
	return sm
}

type etcdClient struct {
	impl      *clientv3.Client
	localIP   string
	processID int
}

func (ec etcdClient) Get() *clientv3.Client {
	return ec.impl
}

var EtcdClient = etcdClient{}

func (ec *etcdClient) NewClient() etcdClient {
	return etcdClient{}
}

func (ec *etcdClient) ClientID() string {
	return fmt.Sprintf("%v@%v", ec.localIP, ec.processID)
}

func (ec *etcdClient) Init(endPoints []string, username, password string, dialTimeoutSec int) error {

	var dialTimeout time.Duration

	if int(dialTimeoutSec) <= 0 {
		dialTimeout = DEFAULT_DIAL_TIMEOUT
	} else {
		dialTimeout = time.Duration(dialTimeoutSec) * time.Second
	}

	ec.localIP = *util4go.GuessIP(endPoints[0])
	ec.processID = os.Getpid()

	c, err := clientv3.New(clientv3.Config{
		Endpoints:   endPoints,
		DialTimeout: dialTimeout,
		Username:    username,
		Password:    password,
	})
	if err != nil {
		return err
	}
	ec.impl = c
	return nil
}

func (ec *etcdClient) Close() error {
	if ec.impl != nil {
		return ec.impl.Close()
	}
	return errors.New("NullPointException")
}

func (ec *etcdClient) KeyValue(key string, readTimeoutSec int, opts ...clientv3.OpOption) (string, error) {

	var readTimeout time.Duration

	if int(readTimeoutSec) <= 0 {
		readTimeout = DEFAULT_READ_TIMEOUT
	} else {
		readTimeout = time.Duration(readTimeoutSec) * time.Second
	}

	ctx, cancel := context.WithTimeout(context.Background(), readTimeout)
	defer cancel()

	rsp, err := ec.impl.Get(ctx, key, opts...)

	if err == nil {
		if len(rsp.Kvs) == 0 {
			return "", errors.New("NotFound")
		} else {
			return string(rsp.Kvs[0].Value), nil
		}
	} else {
		return "", err
	}
}

func (ec *etcdClient) KeyObject(key string, t reflect.Type, readTimeoutSec int, opts ...clientv3.OpOption) any {
	str, err := ec.KeyValue(key, readTimeoutSec, opts...)
	if err != nil {
		return nil
	}

	if len(str) == 0 {
		return nil
	}

	rtn := util4go.NewInstanceValue(t)

	if err := json.Unmarshal([]byte(str), rtn.Interface()); err == nil {
		return rtn.Interface()
	} else {
		panic(err)
	}

}

func (ec *etcdClient) KeyMapObject(key string, readTimeoutSec int, opts ...clientv3.OpOption) (map[string]any, error) {
	str, err := ec.KeyValue(key, readTimeoutSec, opts...)
	if err != nil {
		return nil, err
	}

	rtn := make(map[string]any)

	if json.Unmarshal([]byte(str), rtn) != nil {
		return nil, err
	}

	return rtn, nil
}

func (ec *etcdClient) CountWithPrefix(prefix string, readTimeoutSec int, opts ...clientv3.OpOption) int {
	var readTimeout time.Duration

	if int(readTimeoutSec) <= 0 {
		readTimeout = DEFAULT_READ_TIMEOUT
	} else {
		readTimeout = time.Duration(readTimeoutSec) * time.Second
	}

	opts1 := make([]clientv3.OpOption, 0, len(opts)+1)
	opts1 = append(opts1, clientv3.WithPrefix())
	opts1 = append(opts1, clientv3.WithCountOnly())
	opts1 = append(opts1, opts...)

	ctx, cancel := context.WithTimeout(context.Background(), readTimeout)
	defer cancel()

	rsp, err := ec.impl.Get(ctx, prefix, opts1...)

	if err == nil {
		return int(rsp.Count)
		//if len(rsp.Count) == 0 {
		//	return 0
		//} else {
		//	if count, error := strconv.Atoi(rsp.Kvs[0].String()); error == nil {
		//		return count
		//	} else {
		//		panic(err.Error())
		//	}
		//}
	} else {
		panic(err.Error())
	}
}

func (ec *etcdClient) GetKeyValuesWithPrefix(prefix string, order *sortMode, skip, count int, readTimeoutSec int,
	opts ...clientv3.OpOption) []string {
	var readTimeout time.Duration

	if int(readTimeoutSec) <= 0 {
		readTimeout = DEFAULT_READ_TIMEOUT
	} else {
		readTimeout = time.Duration(readTimeoutSec) * time.Second
	}

	ctx, cancel := context.WithTimeout(context.Background(), readTimeout)
	defer cancel()

	var ops = make([]clientv3.OpOption, 0, len(opts)+3)
	ops = append(ops, clientv3.WithPrefix())

	if order != nil {
		ops = append(ops, clientv3.WithSort(
			clientv3.SortTarget(order.SortTarget), clientv3.SortOrder(order.SortOrder)))
	}

	if count > 0 {
		ops = append(ops, clientv3.WithLimit(int64(skip+count)))
	}

	ops = append(ops, opts...)

	rsp, err := ec.impl.Get(ctx, prefix, ops...)

	if err == nil {
		if len(rsp.Kvs) == 0 {
			return nil
		} else {
			rtn := make([]string, 0, len(rsp.Kvs))
			for idx, b := range rsp.Kvs {
				if idx < skip {
					continue
				}

				rtn = append(rtn, string(b.Value))
			}
			return rtn
		}
	} else {
		return nil
	}
}

type KeyValue struct {
	Key   string
	Value string
}

func (ec *etcdClient) GetKeyAndValuesWithPrefix(prefix string, order *sortMode, skip, count int, readTimeoutSec int,
	opts ...clientv3.OpOption) []KeyValue {
	var readTimeout time.Duration

	if int(readTimeoutSec) <= 0 {
		readTimeout = DEFAULT_READ_TIMEOUT
	} else {
		readTimeout = time.Duration(readTimeoutSec) * time.Second
	}

	ctx, cancel := context.WithTimeout(context.Background(), readTimeout)
	defer cancel()

	var ops = make([]clientv3.OpOption, 0, len(opts)+3)
	ops = append(ops, clientv3.WithPrefix())

	if order != nil {
		ops = append(ops, clientv3.WithSort(
			clientv3.SortTarget(order.SortTarget), clientv3.SortOrder(order.SortOrder)))
	}

	if count > 0 {
		ops = append(ops, clientv3.WithLimit(int64(skip+count)))
	}

	ops = append(ops, opts...)

	rsp, err := ec.impl.Get(ctx, prefix, ops...)

	if err == nil {
		if len(rsp.Kvs) == 0 {
			return nil
		} else {
			rtn := make([]KeyValue, 0, len(rsp.Kvs))
			for idx, b := range rsp.Kvs {
				if idx < skip {
					continue
				}

				rtn = append(rtn, KeyValue{string(b.Key), string(b.Value)})
			}
			return rtn
		}
	} else {
		return nil
	}
}

func (ec *etcdClient) GetKeyObjectsWithPrefix(prefix string, t reflect.Type, order *sortMode, skip, count int,
	readTimeoutSec int, opts ...clientv3.OpOption) []any {
	var readTimeout time.Duration

	if int(readTimeoutSec) <= 0 {
		readTimeout = DEFAULT_READ_TIMEOUT
	} else {
		readTimeout = time.Duration(readTimeoutSec) * time.Second
	}

	ctx, cancel := context.WithTimeout(context.Background(), readTimeout)
	defer cancel()

	var ops = make([]clientv3.OpOption, 0, len(opts)+3)
	ops = append(ops, clientv3.WithPrefix())

	if order != nil {
		ops = append(ops, clientv3.WithSort(
			clientv3.SortTarget(order.SortTarget), clientv3.SortOrder(order.SortOrder)))
	}

	if count > 0 {
		ops = append(ops, clientv3.WithLimit(int64(skip+count)))
	}

	ops = append(ops, opts...)

	rsp, err := ec.impl.Get(ctx, prefix, ops...)

	if err == nil {
		if len(rsp.Kvs) == 0 {
			return nil
		} else {
			rtn := make([]any, 0, len(rsp.Kvs))
			for idx, b := range rsp.Kvs {
				if idx < skip {
					continue
				}

				obj := util4go.NewInstanceValue(t)

				if error := fastjson.UnmarshalObject(string(b.Value), obj.Interface().(fastjson.Unmarshalable)); error == nil {
					//if !obj.IsNil() {
					//	rtn = append(rtn, obj.Interface())
					//} else {
					//	rtn = append(rtn, nil)
					//}
					rtn = append(rtn, obj.Interface())
				} else {
					panic(error.Error())
				}

				/*if error := json.Unmarshal([]byte(b.Value), obj.Interface()); error == nil {
					rtn = append(rtn, obj.Interface())
				} else {
					panic(error.Error())
				}*/
			}
			return rtn
		}
	} else {
		return nil
	}

}

func (ec *etcdClient) PutValueOp(key string, data any,
	opts ...clientv3.OpOption) clientv3.Op {
	var v string

	switch data.(type) {
	case string:
		v = data.(string)
	default:
		v = ec.Obj2str(data)
	}

	return clientv3.OpPut(key, v, opts...)
}

func (ec *etcdClient) PutLeasedValueOp(key string, data any, leaseId clientv3.LeaseID,
	opts ...clientv3.OpOption) []clientv3.Op {

	if leaseId == clientv3.NoLease {
		return []clientv3.Op{
			ec.PutValueOp(key, data),
		}
	} else {
		var ops = make([]clientv3.OpOption, 0, len(opts)+1)
		ops = append(ops, opts...)
		ops = append(ops, clientv3.WithLease(leaseId))

		return []clientv3.Op{
			ec.PutValueOp(key, data, ops...),
			//ec.PutValueOp(ec.formatLeaseKey(leaseId, key), "", clientv3.WithLease(leaseId)),
			//ec.PutValueOp(ec.formatLeaseObjectKey(leaseId, key), "", clientv3.WithLease(leaseId)),
		}
	}
}

func (ec *etcdClient) PutLeasedValue(key string, data any, leaseId clientv3.LeaseID,
	writeTimeoutSec int,
	opts ...clientv3.OpOption) (string, error) {
	var writeTimeout time.Duration

	if int(writeTimeoutSec) <= 0 {
		writeTimeout = DEFAULT_WRITE_TIMEOUT
	} else {
		writeTimeout = time.Duration(writeTimeoutSec) * time.Second
	}

	ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
	defer cancel()

	if leaseId != clientv3.NoLease && int64(leaseId) > 0 {
		var ops = make([]clientv3.OpOption, 0, len(opts)+1)
		ops = append(ops, opts...)
		ops = append(ops, clientv3.WithLease(leaseId))

		v := ec.Obj2str(data)
		_, err := ec.impl.Put(ctx, key, v, ops...)

		if err != nil {
			return v, err
		}

		//_, err = ec.impl.Put(ctx, ec.formatLeaseKey(leaseId, key), "", clientv3.WithLease(leaseId))
		//_, err = ec.impl.Put(ctx, ec.formatLeaseObjectKey(leaseId, key), "", clientv3.WithLease(leaseId))

		return v, err
	} else {
		v := ec.Obj2str(data)
		_, err := ec.impl.Put(ctx, key, v, opts...)

		return v, err
	}

}

func (ec *etcdClient) PutText(key string, data string, writeTimeoutSec int,
	opts ...clientv3.OpOption) error {
	var writeTimeout time.Duration

	if int(writeTimeoutSec) <= 0 {
		writeTimeout = DEFAULT_WRITE_TIMEOUT
	} else {
		writeTimeout = time.Duration(writeTimeoutSec) * time.Second
	}

	ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
	defer cancel()

	_, err := ec.impl.Put(ctx, key, data, opts...)

	return err
}

func (ec *etcdClient) PutValue(key string, data any, writeTimeoutSec int,
	opts ...clientv3.OpOption) (string, error) {

	var v string

	switch data.(type) {
	case string:
		v = data.(string)
	default:
		v = ec.Obj2str(data)
	}

	var writeTimeout time.Duration

	if int(writeTimeoutSec) <= 0 {
		writeTimeout = DEFAULT_WRITE_TIMEOUT
	} else {
		writeTimeout = time.Duration(writeTimeoutSec) * time.Second
	}

	ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
	defer cancel()

	_, err := ec.impl.Put(ctx, key, v, opts...)

	return v, err
}

func (ec *etcdClient) PutValuePlus(key string, data any, leaseSec, writeTimeoutSec int,
	opts ...clientv3.OpOption) (string, clientv3.LeaseID, error) {

	var v string

	switch data.(type) {
	case string:
		v = data.(string)
	default:
		v = ec.Obj2str(data)
	}

	var ops = make([]clientv3.OpOption, 0, len(opts)+1)
	ops = append(ops, opts...)

	var writeTimeout time.Duration

	if int(writeTimeoutSec) <= 0 {
		writeTimeout = DEFAULT_WRITE_TIMEOUT
	} else {
		writeTimeout = time.Duration(writeTimeoutSec) * time.Second
	}

	ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
	defer cancel()

	if leaseSec > 0 {

		if lease, err := ec.impl.Grant(ctx, int64(leaseSec)); err == nil {
			ops = append(ops, clientv3.WithLease(lease.ID))
			_, err1 := ec.impl.Put(ctx, key, v, ops...)

			if err1 != nil {
				return v, clientv3.NoLease, err1
			}

			//_, err1 = ec.impl.Put(ctx, ec.formatLeaseKey(lease.ID, key), "", clientv3.WithLease(lease.ID))
			//
			//if err1 != nil {
			//	return v, clientv3.NoLease, err1
			//}
			//
			//_, err1 = ec.impl.Put(ctx, ec.formatLeaseObjectKey(lease.ID, key), "", clientv3.WithLease(lease.ID))
			//
			//if err1 != nil {
			//	return v, clientv3.NoLease, err1
			//}

			return v, lease.ID, err1

		} else {
			return v, lease.ID, err
		}
	} else {

		_, err1 := ec.impl.Put(ctx, key, v, ops...)

		if err1 != nil {
			return v, clientv3.NoLease, err1
		}

		return v, clientv3.NoLease, nil
	}
}

type KeepAliveEventListener func(lease clientv3.LeaseID) error

func (ec *etcdClient) PutKeepAliveValue(key string, data any, leaseSec, writeTimeoutSec int,
	listener KeepAliveEventListener,
	opts ...clientv3.OpOption) (string, clientv3.LeaseID, error) {

	var ops = make([]clientv3.OpOption, 0, len(opts)+1)
	ops = append(ops, opts...)

	var writeTimeout time.Duration

	if int(writeTimeoutSec) <= 0 {
		writeTimeout = DEFAULT_WRITE_TIMEOUT
	} else {
		writeTimeout = time.Duration(writeTimeoutSec) * time.Second
	}

	if leaseSec <= 0 {
		leaseSec = DATA_TTL
	}

	ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
	defer cancel()

	v := ec.Obj2str(data)

	if lease, err := ec.impl.Grant(ctx, int64(leaseSec)); err == nil {
		ops = append(ops, clientv3.WithLease(lease.ID))
		_, err1 := ec.impl.Put(ctx, key, v, ops...)

		if err1 != nil {
			return v, lease.ID, err1
		}

		if keepRespChan, err := ec.impl.KeepAlive(context.Background(), lease.ID); err == nil {
			go func() {
				defer func() {
					if err := recover(); err != nil {
						logger.Debug("*****退出自动续租Keepalive(%v) 错误状态 :%v", lease.ID, err)
					} else {
						logger.Debug("*****退出自动续租Keepalive(%v)", lease.ID)
					}
					if listener != nil {
						if err := listener(lease.ID); err != nil {
							//		logger.Debug("自动续租(%v)应答处理错误:%v", lease.ID, err)
						} else {
							//		logger.Debug("自动续租(%v)应答处理完毕", lease.ID)
						}
					}
				}()

				for {
					breakNow := false

					select {
					case <-keepRespChan:
						{
							if keepRespChan == nil {
								logger.Debug("租约(%v)已经失效, 退出自动续约", lease.ID)
								return
							} else { //每秒会续租一次，所以就会受到一次应答
								rs, err := ec.impl.TimeToLive(context.TODO(), lease.ID)

								if err != nil {
									logger.Debug("收到自动续租(%v)应答, Lease已经失效", lease.ID)
									breakNow = true
									break
								}

								if rs.TTL <= 0 {
									logger.Debug("收到自动续租(%v)应答, Lease已经失效", lease.ID)
									breakNow = true
									break
								}

								logger.Debug("收到自动续租(%v)应答, TTL(%v)", lease.ID, lease.TTL)
							}
						}
					}

					if breakNow {
						break
					}

					time.Sleep(100 * time.Millisecond)
				}
			}()
		} else {
			return v, lease.ID, err
		}

		return v, lease.ID, err1
	} else {
		return v, lease.ID, err
	}
}

func (ec *etcdClient) DeleteOp(key string,
	opts ...clientv3.OpOption) clientv3.Op {
	//
	//_, err := ec.impl.Get(ctx, key)
	//if err != nil {
	//	return false
	//}
	return clientv3.OpDelete(key, opts...)
}

func (ec *etcdClient) Delete(key string, writeTimeoutSec int,
	opts ...clientv3.OpOption) bool {
	var writeTimeout time.Duration

	if int(writeTimeoutSec) <= 0 {
		writeTimeout = DEFAULT_WRITE_TIMEOUT
	} else {
		writeTimeout = time.Duration(writeTimeoutSec) * time.Second
	}

	ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
	defer cancel()
	//
	//_, err := ec.impl.Get(ctx, key)
	//if err != nil {
	//	return false
	//}
	rsp, _ := ec.impl.Delete(ctx, key, opts...)

	return rsp.Deleted > 0
}

// LeaseOpBuild return clientv3.Op[], string to description of lease, error if raise error
type LeaseOpBuild func(leaseID clientv3.LeaseID) ([]clientv3.Op, string, error)

// TxnBuild return clientv3.Op[], string to description of lease, error if raise error
type TxnBuild func(txn clientv3.Txn, leaseID clientv3.LeaseID) (clientv3.Txn, string, error)

func (ec *etcdClient) BulkOpsPlus(txnBuild TxnBuild, leaseTtl, writeTimeoutSec int) (clientv3.LeaseID, error) {

	var writeTimeout time.Duration

	if int(writeTimeoutSec) <= 0 {
		writeTimeout = DEFAULT_WRITE_TIMEOUT
	} else {
		writeTimeout = time.Duration(writeTimeoutSec) * time.Second
	}

	ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
	defer cancel()

	txn := ec.impl.Txn(ctx)

	if leaseTtl > 0 {
		if lease, err := ec.impl.Grant(ctx, int64(leaseTtl)); err == nil {

			//var leaseDesc string

			if txnBuild != nil {
				txn, _, err = txnBuild(txn, lease.ID)
				if err != nil {
					return clientv3.NoLease, err
				}
				//txn.Then(
				//	ec.PutValueOp(ec.formatLeaseKey(lease.ID, leaseDesc), "", clientv3.WithLease(lease.ID)),
				//	ec.PutValueOp(ec.formatLeaseObjectKey(lease.ID, leaseDesc), "", clientv3.WithLease(lease.ID)),
				//)
			}

			rsp, err := txn.Commit()

			if err != nil {
				return clientv3.NoLease, err
			}

			if rsp.Succeeded {
				return lease.ID, nil
			} else {
				return clientv3.NoLease, errors.New("rsp is fail")
			}
		} else {
			return clientv3.NoLease, err
		}
	} else {
		if txnBuild != nil {
			var err error
			txn, _, err = txnBuild(txn, 0)
			if err != nil {
				return clientv3.NoLease, err
			}
		}

		rsp, err := txn.Commit()

		if err != nil {
			return clientv3.NoLease, err
		}

		if rsp.Succeeded {
			return clientv3.NoLease, nil
		} else {
			return clientv3.NoLease, errors.New("rsp is fail")
		}
	}

}

func (ec *etcdClient) BulkOps(fn LeaseOpBuild, leaseTtl, writeTimeoutSec int) (clientv3.LeaseID, error) {

	var writeTimeout time.Duration

	if int(writeTimeoutSec) <= 0 {
		writeTimeout = DEFAULT_WRITE_TIMEOUT
	} else {
		writeTimeout = time.Duration(writeTimeoutSec) * time.Second
	}

	ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
	defer cancel()

	txn := ec.impl.Txn(ctx)

	if leaseTtl > 0 {
		if lease, err := ec.impl.Grant(ctx, int64(leaseTtl)); err == nil {
			var ops []clientv3.Op

			//var leaseDesc string

			if fn != nil {
				ops, _, err = fn(lease.ID)
				if err != nil {
					return clientv3.NoLease, err
				}
			} else {
				ops = []clientv3.Op{}
			}

			//if len(ops) > 0 && !util4go.IsEmpty(leaseDesc) {
			//	ops = append(ops,
			//		ec.PutValueOp(ec.formatLeaseKey(lease.ID, leaseDesc), "", clientv3.WithLease(lease.ID)),
			//		ec.PutValueOp(ec.formatLeaseObjectKey(lease.ID, leaseDesc), "", clientv3.WithLease(lease.ID)))
			//}

			//ec.PutValueOp(ec.formatLeaseKey(leaseId), key, clientv3.WithLease(leaseId)),

			rsp, err := txn.Then(ops...).Commit()

			if err != nil {
				return clientv3.NoLease, err
			}

			if rsp.Succeeded {
				return lease.ID, nil
			} else {
				return clientv3.NoLease, errors.New("")
			}
		} else {
			return clientv3.NoLease, err
		}
	} else {
		var ops []clientv3.Op

		if fn != nil {
			var err error
			ops, _, err = fn(clientv3.NoLease)
			if err != nil {
				return clientv3.NoLease, err
			}
		} else {
			return clientv3.NoLease, nil
			//ops = []clientv3.Op{}
		}

		rsp, err := txn.Then(ops...).Commit()

		if err != nil {
			return clientv3.NoLease, err
		}

		if rsp.Succeeded {
			return clientv3.NoLease, nil
		} else {
			return clientv3.NoLease, errors.New("")
		}
	}
}

func (ec *etcdClient) Obj2str(data any) string {
	if data == nil {
		return ""
	}
	//v := reflect.ValueOf(data)
	//if v.Kind() == reflect.Pointer {
	//	v = v.Elem()
	//}
	//if v.Kind() == reflect.Struct {
	//	return json.Marshal()
	//}
	rtn, err := json.Marshal(data)
	if err == nil {
		return string(rtn)
	} else {
		return ""
	}
}

type WatchChannelEventListener func(event *clientv3.Event)

func (ec *etcdClient) WatchKey(key string, listener WatchChannelEventListener, opts ...clientv3.OpOption) {
	rch := ec.impl.Watch(context.Background(), key, opts...)

	for rsp := range rch {
		for _, ev := range rsp.Events {
			if listener != nil {
				go func() {
					defer func() {
						if err := recover(); err != nil {
							logger.Warning("Watch Event raise error %v", err)
						}
					}()
					listener(ev)
				}()
			}
		}
	}
}

func (ec *etcdClient) WatchKeyWithPrefix(prefix string, listener WatchChannelEventListener, opts ...clientv3.OpOption) {
	opts1 := make([]clientv3.OpOption, 0, len(opts)+1)
	opts1 = append(opts1, clientv3.WithPrefix())
	opts1 = append(opts1, opts...)

	rch := ec.impl.Watch(context.Background(), prefix, opts1...)

	for rsp := range rch {
		for _, ev := range rsp.Events {
			if listener != nil {
				go func() {
					defer func() {
						if err := recover(); err != nil {
							logger.Warning("Watch Event raise error %v", err)
						}
					}()
					listener(ev)
				}()
			}
		}
	}
}

type Lock struct {
	s *concurrency.Session
	m *concurrency.Mutex
	//key string
	//ec  *etcdClient
}

const DEFAULT_LOCK_TTL = 60

var LockError = lockError{"lock error"}

var LockByOther = lockError{"lock by other"}

type lockError struct {
	s string
}

func (l *lockError) New(s string) lockError {
	return lockError{s: s}
}

func (l lockError) Error() string {
	return l.s
}

func (l *Lock) Session() *concurrency.Session {
	return l.s
}

func (l *Lock) Mutex() *concurrency.Mutex {
	return l.m
}

// LockAndDo
// Lock and Do something, after something is done will unlock and release lock resource automatically
// If key is locker by other, current goroutine will be blocked
func (ec *etcdClient) LockAndDo(key string, leaseSecond int, do func() (any, error)) (rtn any, rtnErr error) {
	lock, err := EtcdClient.Lock(key, leaseSecond)

	if err != nil {
		return nil, err
	}

	defer func() {
		if err := recover(); err != nil {
			rtn = nil

			if lErr, isLockError := err.(lockError); isLockError {
				rtnErr = lErr
			} else {
				rtnErr = errors.New(fmt.Sprintf("%v", err))
			}
		}
		k := lock.m.Key()
		lock.UnLock()
		logger.Debug("session[%q][%q] 解锁。 time：%d", key, k, time.Now().Unix())
	}()

	k := lock.m.Key()
	logger.Debug("session[%q][%q] 上锁成功。 time：%d", key, k, time.Now().Unix())

	return do()
}

// TryLock
// Use this function, You must invoke UnLock in defer segment to release the lock resource. The best way is use LockAndDo,
// it will release all resources automatically
// If key is locker by other, it wil return lock object with nil without blocking
func (ec *etcdClient) TryLock(key string, leaseSecond int) (rtn *Lock, rtnErr error) {
	var oldKey = key
	key = ec.formatLockKey(key)
	if leaseSecond <= 0 {
		leaseSecond = DEFAULT_LOCK_TTL
	}

	s2, err := concurrency.NewSession(ec.Get(), concurrency.WithTTL(leaseSecond))

	if err != nil {
		return nil, LockError.New(fmt.Sprintf("Lock error : %v", err))
	}

	ec.recordLockRequestInfo(oldKey, s2.Lease())

	defer func() {
		if err := recover(); err != nil {
			rtn = nil
			rtnErr = LockError.New(fmt.Sprintf("Lock error : %v", err))
			logger.Warning("Lock error : %v", err)
			s2.Close()
		}
	}()

	m2 := concurrency.NewMutex(s2, key)

	myKey := fmt.Sprintf("%s%x", key+"/", s2.Lease())
	cmp := clientv3.Compare(clientv3.CreateRevision(myKey), "=", 0)
	// put self in lock waiters via myKey; oldest waiter holds lock
	put := clientv3.OpPut(myKey, "", clientv3.WithLease(s2.Lease()))
	// reuse key in case this session already holds the lock
	get := clientv3.OpGet(myKey)
	// fetch current holder to complete uncontended path with only one RPC
	getOwner := clientv3.OpGet(key+"/", clientv3.WithFirstCreate()...)
	resp, err := ec.Get().Txn(context.TODO()).If(cmp).Then(put, getOwner).Else(get, getOwner).Commit()
	if err != nil {
		return nil, err
	}
	myRev := resp.Header.Revision
	if !resp.Succeeded {
		myRev = resp.Responses[0].GetResponseRange().Kvs[0].CreateRevision
	}

	if err := util4go.SetUnExportFieldValue(m2, "myKey", myKey); err != nil {
		panic(err.Error())
	}
	if err := util4go.SetUnExportFieldValue(m2, "myRev", myRev); err != nil {
		panic(err.Error())
	}

	// if no key on prefix / the minimum rev is key, already hold the lock
	ownerKey := resp.Responses[1].GetResponseRange().Kvs

	if len(ownerKey) == 0 || ownerKey[0].CreateRevision == myRev {

		if err := util4go.SetUnExportFieldValue(m2, "hdr", resp.Header); err != nil {
			panic(err.Error())
		}

		//_, err = ec.impl.Put(context.TODO(), ec.formatLeaseKey(s2.Lease(), key), "", clientv3.WithLease(s2.Lease()))
		//
		//if err != nil {
		//	logger.Warning("**** Lease error at Lock session : %v", err)
		//}

		//_, err = ec.impl.Put(context.TODO(), ec.formatLeaseObjectKey(s2.Lease(), key), "", clientv3.WithLease(s2.Lease()))
		//
		//if err != nil {
		//	logger.Warning("**** Lease error at Lock session : %v", err)
		//}

		ec.recordLockInfo(oldKey, s2.Lease())

		return &Lock{s: s2, m: m2}, nil
	} else {
		ec.Get().Delete(context.TODO(), myKey)
		s2.Close()
	}

	return nil, LockByOther
}

// Lock
// Use this function, You must invoke UnLock in defer segment to release the lock resource. The best way is use LockAndDo,
// it will release all resources automatically
// If key is locker by other, current goroutine will be blocked
func (ec *etcdClient) Lock(key string, leaseSecond int) (rtn *Lock, rtnErr error) {
	var oldKey = key
	key = ec.formatLockKey(key)
	if leaseSecond <= 0 {
		leaseSecond = DEFAULT_LOCK_TTL
	}

	s2, err := concurrency.NewSession(ec.Get(), concurrency.WithTTL(leaseSecond))

	if err != nil {
		return nil, LockError.New(fmt.Sprintf("Lock error : %v", err))
	}

	ec.recordLockRequestInfo(oldKey, s2.Lease())

	defer func() {
		if err := recover(); err != nil {
			rtn = nil
			rtnErr = LockError.New(fmt.Sprintf("Lock error : %v", err))
			logger.Warning("Lock error : %v", err)
			s2.Close()
		}
	}()

	m2 := concurrency.NewMutex(s2, key)
	rtn = &Lock{s: s2, m: m2}
	rtnErr = nil

	if err := m2.Lock(context.TODO()); err != nil {
		rtn = nil
		logger.Warning("Lock error : %v", err)
		rtnErr = LockError.New(fmt.Sprintf("Lock error : %v", err))
	} else {

		ec.recordLockInfo(oldKey, s2.Lease())

		//leaseKey := ec.formatLeaseKey(s2.Lease(), key)
		//_, err = s2.Client().Put(context.TODO(), leaseKey, "", clientv3.WithLease(s2.Lease()))
		//
		//if err != nil {
		//	logger.Warning("**** Lease error at Lock session : %v", err)
		//}

		//leaseObjectKey := ec.formatLeaseObjectKey(s2.Lease(), key)
		//_, err = s2.Client().Put(context.TODO(), leaseObjectKey, "", clientv3.WithLease(s2.Lease()))
		//
		//if err != nil {
		//	logger.Warning("**** Lease error at Lock session : %v", err)
		//}
	}

	return
}

// UnLock
// You should invoke Unlock function to release lock resources in defer segment
func (l *Lock) UnLock() error {
	defer func() {
		if err := recover(); err != nil {
			logger.Warning("Unlock error : %v", err)
		}

		if l.s != nil {
			l.s.Close()
		}

		//l.ec.removeLockRequestInfo(l.key, l.Session().Lease())
		//l.ec.removeLockInfo(l.key)
	}()

	if err := l.m.Unlock(context.TODO()); err != nil {
		return err
	}

	return nil
}

type etcdError struct {
	s string
}

var NotFoundKeyError = etcdError{"NotFoundKey"}

func (e etcdError) Error() string {
	return e.s
}

func (ec *etcdClient) GetInt(key string, readTimeoutSec int) (int64, error) { // opts ...clientv3.OpOption

	var readTimeout time.Duration

	if int(readTimeoutSec) <= 0 {
		readTimeout = DEFAULT_READ_TIMEOUT
	} else {
		readTimeout = time.Duration(readTimeoutSec) * time.Second
	}

	ctx, cancel := context.WithTimeout(context.Background(), readTimeout)
	defer cancel()

	//rsp, err := ec.impl.Get(ctx, key, opts...)
	rsp, err := ec.impl.Get(ctx, key)

	if err == nil {
		if len(rsp.Kvs) == 0 {
			return 0, NotFoundKeyError
		} else {
			//var count int64
			count, err := strconv.ParseInt(string(rsp.Kvs[0].Value), 10, 64)

			if err == nil {
				return count, nil
			} else {
				return 0, err
			}
		}
	} else {
		return 0, err
	}
}

func (ec *etcdClient) PutInt(key string, i int64, writeTimeoutSec int) error {
	return ec.PutText(key, strconv.FormatInt(i, 10), writeTimeoutSec)
}

const LOCK_PREFIX = "/etcd4go-lock/#lock/_%s"
const LOCK_INFO_PREFIX = "/etcd4go-lock/#info/_%s"
const LOCK_REQUEST_INFO_PREFIX = "/etcd4go-lock/#request/_%s/%v"

//const LEASE_PREFIX = "/etcd4go-lease/#lease/_%s#/_%s"
//const LEASE_OBJECT_PREFIX = "/etcd4go-lease/#object/_%s#/_%s"

func (ec *etcdClient) formatLockKey(key string) string {
	return fmt.Sprintf(LOCK_PREFIX, key+"#")
}

func (ec *etcdClient) formatLockInfoKey(key string) string {
	return fmt.Sprintf(LOCK_INFO_PREFIX, key)
}

func (ec *etcdClient) formatLockRequestInfoKey(key string, leaseId clientv3.LeaseID) string {
	return fmt.Sprintf(LOCK_REQUEST_INFO_PREFIX, key, leaseId)
}

func (ec *etcdClient) recordLockInfo(key string, leaseid clientv3.LeaseID) {
	info := make(map[string]any)
	info["client"] = ec.ClientID()
	info["time"] = time.Now().Format("2006/01/02 15:04:05")
	info["leaseid"] = strconv.FormatInt(int64(leaseid), 10)
	info["lock_key"] = key
	str, _ := json.Marshal(info)
	ec.Get().Put(context.TODO(), ec.formatLockInfoKey(key), string(str), clientv3.WithLease(leaseid))
}

func (ec *etcdClient) recordLockRequestInfo(key string, leaseid clientv3.LeaseID) {
	info := make(map[string]any)
	info["client"] = ec.ClientID()
	info["time"] = time.Now().Format("2006/01/02 15:04:05")
	info["leaseid"] = strconv.FormatInt(int64(leaseid), 10)
	info["lock_key"] = key
	str, _ := json.Marshal(info)
	ec.Get().Put(context.TODO(), ec.formatLockRequestInfoKey(key, leaseid), string(str), clientv3.WithLease(leaseid))
}

//func (ec *etcdClient) removeLockInfo(key string) {
//	ec.Get().Delete(context.TODO(), ec.formatLockInfoKey(key))
//}

//func (ec *etcdClient) removeLockRequestInfo(key string, leaseid clientv3.LeaseID) {
//	ec.Get().Delete(context.TODO(), ec.formatLockRequestInfoKey(key, leaseid))
//}

//func (ec *etcdClient) formatLeaseKey(id clientv3.LeaseID, leaseObject string) string {
//	return fmt.Sprintf(LEASE_PREFIX, strconv.FormatInt(int64(id), 16), leaseObject)
//}
//
//func (ec *etcdClient) formatLeaseObjectKey(id clientv3.LeaseID, leaseObject string) string {
//	return fmt.Sprintf(LEASE_OBJECT_PREFIX, leaseObject, strconv.FormatInt(int64(id), 16))
//}

func (ec *etcdClient) Incr(key string, count int64, leaseSecond int) (int64, error) {
	rtn, err := ec.LockAndDo(key, leaseSecond, func() (any, error) {
		v, er := ec.GetInt(key, 0)

		if er != nil {
			if er != NotFoundKeyError {
				return 0, er
			} else {
				v = 0
			}
		}
		v = v + count
		er = ec.PutInt(key, v, 0)

		if er == nil {
			return v, nil
		} else {
			return 0, er
		}
	})

	if err != nil {
		return 0, err
	} else {
		return rtn.(int64), nil
	}
}
