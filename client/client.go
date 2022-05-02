package client

import (
	"encoding/json"
	"errors"
	"github.com/coreos/etcd/clientv3"
	fastjson "github.com/gohutool/boot4go-fastjson"
	"github.com/gohutool/boot4go-util"
	"github.com/gohutool/log4go"
	"golang.org/x/net/context"
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
	impl *clientv3.Client
}

func (ec etcdClient) Get() *clientv3.Client {
	return ec.impl
}

var EtcdClient = etcdClient{}

func (ec *etcdClient) NewClient() etcdClient {
	return etcdClient{}
}

func (ec *etcdClient) Init(endPoints []string, username, password string, dialTimeoutSec int) error {

	var dialTimeout time.Duration

	if int(dialTimeoutSec) <= 0 {
		dialTimeout = DEFAULT_DIAL_TIMEOUT
	} else {
		dialTimeout = time.Duration(dialTimeoutSec) * time.Second
	}

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
	opts1 = append(opts1, opts1...)

	ctx, cancel := context.WithTimeout(context.Background(), readTimeout)
	defer cancel()

	rsp, err := ec.impl.Get(ctx, prefix, opts1...)

	if err == nil {
		if len(rsp.Kvs) == 0 {
			return 0
		} else {
			if count, error := strconv.Atoi(rsp.Kvs[0].String()); error == nil {
				return count
			} else {
				panic(err.Error())
			}
		}
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
	v := ec.Obj2str(data)
	return clientv3.OpPut(key, v, opts...)
}

func (ec *etcdClient) PutLeasedValueOp(key string, data any, leaseId clientv3.LeaseID,
	opts ...clientv3.OpOption) clientv3.Op {
	var ops = make([]clientv3.OpOption, 0, len(opts)+1)
	ops = append(ops, opts...)
	ops = append(ops, clientv3.WithLease(leaseId))

	return ec.PutValueOp(key, data, ops...)
}

func (ec *etcdClient) PutValue(key string, data any, writeTimeoutSec int,
	opts ...clientv3.OpOption) (string, error) {
	var writeTimeout time.Duration

	if int(writeTimeoutSec) <= 0 {
		writeTimeout = DEFAULT_WRITE_TIMEOUT
	} else {
		writeTimeout = time.Duration(writeTimeoutSec) * time.Second
	}

	ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
	defer cancel()

	v := ec.Obj2str(data)
	_, err := ec.impl.Put(ctx, key, v, opts...)

	return v, err
}

func (ec *etcdClient) PutValuePlus(key string, data any, leaseSec, writeTimeoutSec int,
	opts ...clientv3.OpOption) (string, error) {

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
		return v, err1
	} else {
		return v, err
	}
}

type KeepAliveEventListener func(leaseID clientv3.LeaseID, keepAliveResponse clientv3.LeaseKeepAliveResponse) error

func (ec *etcdClient) PutKeepAliveValue(key string, data any, leaseSec, writeTimeoutSec int,
	listener KeepAliveEventListener,
	opts ...clientv3.OpOption) (string, error) {

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
			return v, err1
		}

		if keepRespChan, err := ec.impl.KeepAlive(context.Background(), lease.ID); err == nil {
			go func() {
				defer func() {
					if err := recover(); err != nil {
						logger.Debug("退出自动续租Keepalive(%v) 错误状态 :%v", lease.ID, err)
					} else {
						logger.Debug("退出自动续租Keepalive(%v)", lease.ID)
					}
				}()

				for {
					select {
					case keepResp := <-keepRespChan:
						{
							if keepRespChan == nil {
								logger.Debug("租约(%v)已经失效, 退出自动续约", lease.ID)
								return
							} else { //每秒会续租一次，所以就会受到一次应答
								logger.Debug("收到自动续租(%v)应答", lease.ID)
								if listener != nil {
									if err := listener(lease.ID, *keepResp); err != nil {
										logger.Debug("自动续租(%v)应答处理错误:%v", lease.ID, err)
									} else {
										logger.Debug("自动续租(%v)应答处理完毕", lease.ID)
									}
								}
							}
						}
					}
				}
			}()
		} else {
			return v, err
		}

		return v, err1
	} else {
		return v, err
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

type LeaseOpBuild func(leaseID clientv3.LeaseID) ([]clientv3.Op, error)

type TxnBuild func(txn clientv3.Txn, leaseID clientv3.LeaseID) (clientv3.Txn, error)

func (ec *etcdClient) BulkOpsPlus(txnBuild TxnBuild, leaseTtl, writeTimeoutSec int) error {

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
			if txnBuild != nil {
				txn, err = txnBuild(txn, lease.ID)
				if err != nil {
					return err
				}
			}

			rsp, err := txn.Commit()

			if err != nil {
				return err
			}

			if rsp.Succeeded {
				return nil
			} else {
				return errors.New("rsp is fail")
			}
		} else {
			return err
		}
	} else {
		if txnBuild != nil {
			var err error
			txn, err = txnBuild(txn, 0)
			if err != nil {
				return err
			}
		}

		rsp, err := txn.Commit()

		if err != nil {
			return err
		}

		if rsp.Succeeded {
			return nil
		} else {
			return errors.New("rsp is fail")
		}
	}

}

func (ec *etcdClient) BulkOps(fn LeaseOpBuild, leaseTtl, writeTimeoutSec int) error {

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

			if fn != nil {
				ops, err = fn(lease.ID)
				if err != nil {
					return err
				}
			} else {
				ops = []clientv3.Op{}
			}

			rsp, err := txn.Then(ops...).Commit()

			if err != nil {
				return err
			}

			if rsp.Succeeded {
				return nil
			} else {
				return errors.New("")
			}
		} else {
			return err
		}
	} else {
		var ops []clientv3.Op

		if fn != nil {
			var err error
			ops, err = fn(0)
			if err != nil {
				return err
			}
		} else {
			return nil
			//ops = []clientv3.Op{}
		}

		rsp, err := txn.Then(ops...).Commit()

		if err != nil {
			return err
		}

		if rsp.Succeeded {
			return nil
		} else {
			return errors.New("")
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
