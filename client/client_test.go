package client

import (
	"fmt"
	"github.com/gohutool/log4go"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"golang.org/x/net/context"
	"strconv"
	"sync"
	"testing"
	"time"
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
		logger.Info("Event : %+v IsCreate %+v IsModify %+v %+v %+v ",
			event.Type, event.IsCreate(), event.IsModify(), event.Kv, event.PrevKv)
	}, clientv3.WithPrevKV())

	fmt.Println("Watch")

	select {}
}

func TestLockOne(t *testing.T) {
	err := EtcdClient.Init([]string{"192.168.56.101:32379"}, "", "", 30)

	if err == nil {
		fmt.Println("Etcd is connect")
	} else {
		panic("Etcd can not connect")
	}

}

func TestLockTwo(t *testing.T) {
	err := EtcdClient.Init([]string{"192.168.56.101:32379"}, "", "", 30)

	if err == nil {
		fmt.Println("Etcd is connect")
	} else {
		panic("Etcd can not connect")
	}
	defer EtcdClient.Close()

	s1, err := concurrency.NewSession(EtcdClient.Get(), concurrency.WithTTL(5))
	if err != nil {
		logger.Critical(err)
	}
	defer s1.Close()

	//会话1上锁成功，然后开启goroutine去新建一个会话去上锁，5秒钟后会话1解锁。
	m1 := concurrency.NewMutex(s1, "mylock")
	if err := m1.Lock(context.TODO()); err != nil {
		logger.Critical(err)
	}
	fmt.Printf("session1 上锁成功。 time：%d \n", time.Now().Unix())

	g2 := make(chan struct{})
	go func() {
		defer close(g2)
		s2, err := concurrency.NewSession(EtcdClient.Get(), concurrency.WithTTL(5))
		if err != nil {
			logger.Critical(err)
		}
		defer s2.Close()
		m2 := concurrency.NewMutex(s2, "mylock")
		if err := m2.Lock(context.TODO()); err != nil {
			logger.Critical(err)
		}
		fmt.Printf("session2 上锁成功。 time：%d \n", time.Now().Unix())
		if err := m2.Unlock(context.TODO()); err != nil {
			logger.Critical(err)
		}
		fmt.Printf("session2 解锁。 time：%d \n", time.Now().Unix())

	}()

	time.Sleep(5 * time.Second)

	if err := m1.Unlock(context.TODO()); err != nil {
		logger.Critical(err)
	}

	fmt.Printf("session1 解锁。 time：%d \n", time.Now().Unix())

	<-g2
}

func TestLockTwoTwo(t *testing.T) {

	wg := &sync.WaitGroup{}
	wg.Add(2)

	err := EtcdClient.Init([]string{"192.168.56.101:32379"}, "", "", 30)

	if err == nil {
		fmt.Println("Etcd is connect")
	} else {
		panic("Etcd can not connect")
	}
	defer EtcdClient.Close()

	go func() {
		s1, err := concurrency.NewSession(EtcdClient.Get(), concurrency.WithTTL(5))
		if err != nil {
			logger.Critical(err)
		}
		defer wg.Done()
		defer s1.Close()

		//会话1上锁成功，然后开启goroutine去新建一个会话去上锁，5秒钟后会话1解锁。
		m1 := concurrency.NewMutex(s1, "mylock")
		if err := m1.Lock(context.TODO()); err != nil {
			logger.Critical(err)
		}
		fmt.Printf("session1 上锁成功。 time：%d \n", time.Now().Unix())

		time.Sleep(10 * time.Second)

		s1.Close()
		fmt.Printf("session1 已经关闭。 time：%d \n", time.Now().Unix())

		//if err := m1.Unlock(context.TODO()); err != nil {
		//	logger.Critical(err)
		//}

		fmt.Printf("session1 解锁。 time：%d \n", time.Now().Unix())

	}()

	go func() {
		time.Sleep(1 * time.Second)
		s2, err := concurrency.NewSession(EtcdClient.Get(), concurrency.WithTTL(5))
		if err != nil {
			logger.Critical(err)
		}
		defer wg.Done()
		defer s2.Close()

		m2 := concurrency.NewMutex(s2, "mylock")
		if err := m2.Lock(context.TODO()); err != nil {
			logger.Critical(err)
		}
		fmt.Printf("session2 上锁成功。 time：%d \n", time.Now().Unix())

		time.Sleep(5 * time.Second)
		if err := m2.Unlock(context.TODO()); err != nil {
			logger.Critical(err)
		}
		fmt.Printf("session2 解锁。 time：%d \n", time.Now().Unix())

	}()

	wg.Wait()
}

func TestLockTwoWithClient(t *testing.T) {

	wg := &sync.WaitGroup{}
	wg.Add(4)

	err := EtcdClient.Init([]string{"192.168.56.101:32379"}, "", "", 30)

	if err == nil {
		logger.Info("Etcd is connect")
	} else {
		panic("Etcd can not connect")
	}
	defer EtcdClient.Close()

	go func() {
		logger.Info("Come int goroutine 1")
		logger.Info("Prepare lock for goroutine 1")
		lock, err := EtcdClient.Lock("mylock", 0)

		if err != nil {
			logger.Critical(err)
		}
		logger.Info("session1 上锁成功。 time：%d", time.Now().Unix())

		defer wg.Done()
		defer func() {
			lock.UnLock()
			logger.Info("session1 解锁。 time：%d", time.Now().Unix())
		}()

		time.Sleep(10 * time.Second)

		//fmt.Printf("session1 已经关闭。 time：%d \n", time.Now().Unix())

	}()

	go func() {
		logger.Info("Come int goroutine 2")
		time.Sleep(1 * time.Second)
		logger.Info("Prepare lock for goroutine 2")
		lock2, err := EtcdClient.Lock("mylock", 0)
		logger.Info("session2 上锁成功。 time：%d", time.Now().Unix())

		if err != nil {
			logger.Critical(err)
		}
		defer wg.Done()
		defer func() {
			lock2.UnLock()
			logger.Info("session1 解锁。 time：%d", time.Now().Unix())
		}()

		time.Sleep(5 * time.Second)
	}()

	go func() {
		logger.Info("Come int goroutine 3")
		time.Sleep(1 * time.Second)
		logger.Info("Prepare lock for goroutine 3")
		lock3, err := EtcdClient.Lock("mylock", 0)
		logger.Info("session3 上锁成功。 time：%d", time.Now().Unix())

		if err != nil {
			logger.Critical(err)
		}
		defer wg.Done()
		defer func() {
			lock3.UnLock()
			logger.Info("session3 解锁。 time：%d", time.Now().Unix())
		}()

		time.Sleep(5 * time.Second)
	}()

	go func() {
		logger.Info("Come int goroutine 4")
		time.Sleep(1 * time.Second)
		logger.Info("Prepare lock for goroutine 4")
		lock2, err := EtcdClient.Lock("mylock2", 0)
		logger.Info("session4 上锁成功。 time：%d", time.Now().Unix())

		if err != nil {
			logger.Critical(err)
		}
		defer wg.Done()
		defer func() {
			lock2.UnLock()
			logger.Info("session4 解锁。 time：%d", time.Now().Unix())
		}()

		time.Sleep(5 * time.Second)
	}()

	wg.Wait()
}

func TestLockTwoWithClientLockAndDO(t *testing.T) {

	wg := &sync.WaitGroup{}
	wg.Add(4)

	err := EtcdClient.Init([]string{"192.168.56.101:32379"}, "", "", 30)

	if err == nil {
		logger.Info("Etcd is connect")
	} else {
		panic("Etcd can not connect")
	}
	defer EtcdClient.Close()

	go func() {

		defer wg.Done()

		logger.Info("Come int goroutine 1")
		logger.Info("Prepare lock for goroutine 1")

		_, err := EtcdClient.LockAndDo("mylock", 0, func() (any, error) {
			logger.Info("session1 上锁成功。 time：%d", time.Now().Unix())
			time.Sleep(10 * time.Second)
			return nil, nil
		})

		if err != nil {
			logger.Critical(err)
		} else {
			logger.Info("session1 LockAndDo over。 time：%d", time.Now().Unix())
		}

	}()

	go func() {
		defer wg.Done()

		logger.Info("Come int goroutine 2")
		time.Sleep(1 * time.Second)
		logger.Info("Prepare lock for goroutine 2")

		_, err := EtcdClient.LockAndDo("mylock", 0, func() (any, error) {
			logger.Info("session2 上锁成功。 time：%d", time.Now().Unix())

			time.Sleep(5 * time.Second)

			return nil, nil
		})

		if err != nil {
			logger.Critical(err)
		} else {
			logger.Info("session2 LockAndDo over。 time：%d", time.Now().Unix())
		}

	}()

	go func() {
		defer wg.Done()

		logger.Info("Come int goroutine 3")
		time.Sleep(1 * time.Second)
		logger.Info("Prepare lock for goroutine 3")

		_, err := EtcdClient.LockAndDo("mylock", 0, func() (any, error) {
			logger.Info("session3 上锁成功。 time：%d", time.Now().Unix())

			time.Sleep(5 * time.Second)

			return nil, nil
		})

		if err != nil {
			logger.Critical(err)
		} else {
			logger.Info("session3 LockAndDo over。 time：%d", time.Now().Unix())
		}

	}()

	go func() {
		defer wg.Done()

		logger.Info("Come int goroutine 4")
		time.Sleep(1 * time.Second)
		logger.Info("Prepare lock for goroutine 4")

		_, err := EtcdClient.LockAndDo("mylock2", 0, func() (any, error) {
			logger.Info("session4 上锁成功。 time：%d", time.Now().Unix())

			time.Sleep(5 * time.Second)

			return nil, nil
		})

		if err != nil {
			logger.Critical(err)
		} else {
			logger.Info("session4 LockAndDo over。 time：%d", time.Now().Unix())
		}
	}()

	wg.Wait()
}

func TestTryLock(t *testing.T) {

	wg := &sync.WaitGroup{}
	wg.Add(2)

	err := EtcdClient.Init([]string{"192.168.56.101:32379"}, "", "", 30)

	if err == nil {
		logger.Info("Etcd is connect")
	} else {
		panic("Etcd can not connect")
	}

	defer EtcdClient.Close()

	go func() {

		defer wg.Done()

		logger.Info("Come int goroutine 1")
		time.Sleep(2 * time.Second)
		logger.Info("Prepare lock for goroutine 1")

		_, err := EtcdClient.LockAndDo("mylock", 60, func() (any, error) {
			logger.Info("session1 上锁成功。 time：%d", time.Now().Unix())
			time.Sleep(50 * time.Second)
			return nil, nil
		})

		if err != nil {
			logger.Critical(err)
		} else {
			logger.Info("session1 LockAndDo over。 time：%d", time.Now().Unix())
		}

	}()

	go func() {
		defer wg.Done()
		logger.Info("Come int goroutine 2")
		time.Sleep(1 * time.Second)
		logger.Info("Prepare lock for goroutine 2")

		if l, err := EtcdClient.TryLock("mylock", 180); err != nil {
			logger.Critical(err)
			logger.Info("session tryLock fail。 time：%d", time.Now().Unix())
		} else {
			time.Sleep(10 * time.Second)
			l.UnLock()
			logger.Info("session tryLock over。 time：%d", time.Now().Unix())
		}

	}()

	wg.Wait()
	time.Sleep(2 * time.Second)
}

func TestWithMaxCreateRev(t *testing.T) {

	wg := &sync.WaitGroup{}
	wg.Add(1)

	err := EtcdClient.Init([]string{"192.168.56.101:32379"}, "", "", 30)

	if err == nil {
		logger.Info("Etcd is connect")
	} else {
		panic("Etcd can not connect")
	}

	go func() {
		var getOps []clientv3.OpOption
		getOps = append(getOps, clientv3.WithLastCreate()...)
		getOps = append(getOps, clientv3.WithMaxCreateRev(2290))

		//getOps = append(getOps, clientv3.WithMaxCreateRev(2255))
		resp, err := EtcdClient.Get().Get(context.TODO(), "/test/k/", getOps...)
		if err != nil {
			logger.Critical(err)
		}
		if len(resp.Kvs) != 0 {
			logger.Info("[%v] %s -> %s, create rev=%d, head rev=%d\n",
				len(resp.Kvs),
				string(resp.Kvs[0].Key),
				string(resp.Kvs[0].Value),
				resp.Kvs[0].CreateRevision,
				resp.Header.Revision)
		}
		wg.Done()
	}()

	wg.Wait()
	time.Sleep(2 * time.Second)
}

func TestIncr(t *testing.T) {
	err := EtcdClient.Init([]string{"192.168.56.101:32379"}, "", "", 30)

	if err == nil {
		logger.Info("Etcd is connect")
	} else {
		panic("Etcd can not connect")
	}

	key := "/test/test_lock/incr/sample"

	v, err := EtcdClient.Incr(key, 1, 60)
	if err == nil {
		logger.Info("%v", v)
	} else {
		logger.Info("%v", err.Error())
	}

	time.Sleep(2 * time.Second)
}

func TestIncrConcurrent(t *testing.T) {

	count := 300
	wg := &sync.WaitGroup{}
	wg.Add(count)

	err := EtcdClient.Init([]string{"192.168.56.101:32379"}, "", "", 30)

	if err == nil {
		logger.Info("Etcd is connect")
	} else {
		panic("Etcd can not connect")
	}

	loop := 10
	key := "/test/test_lock/incr/sample"

	for idx := 1; idx <= count; idx++ {
		go func(idx int) {
			for j := 0; j < loop; j++ {
				v, err := EtcdClient.Incr(key+strconv.Itoa(j), 1, 60)
				if err == nil {
					logger.Info("goroutine-%v=%v", idx, v)
				} else {
					logger.Info("%v", err.Error())
				}

				//time.Sleep(1 * time.)
			}

			logger.Info("goroutine-%v is over", idx)

			wg.Done()
		}(idx)
	}
	logger.Info("Test Over")
	wg.Wait()
	time.Sleep(2 * time.Second)
}

func TestPutWithLease(t *testing.T) {

	key := "/test/test_lease/sample"

	err := EtcdClient.Init([]string{"192.168.56.101:32379"}, "", "", 30)

	if err == nil {
		logger.Info("Etcd is connect")
	} else {
		panic("Etcd can not connect")
	}

	EtcdClient.PutValue(key, "hello world", 0)

	EtcdClient.PutLeasedValue(key, "hello world", 20, 0)

	EtcdClient.PutValuePlus(key, "hello world", 60, 0)

	logger.Info("Test Over")
	time.Sleep(2 * time.Second)
}

func TestKeepAlive(t *testing.T) {

	key := "foo"

	err := EtcdClient.Init([]string{"192.168.56.101:32379"}, "", "", 30)

	if err == nil {
		logger.Info("Etcd is connect")
	} else {
		panic("Etcd can not connect")
	}

	EtcdClient.PutKeepAliveValue(key, "hello world", 30, 0, func(lease clientv3.LeaseID) error {
		logger.Info(lease)
		return nil
	})

	time.Sleep(300 * time.Second)
	logger.Info("Test Over")
	time.Sleep(2 * time.Second)
}
