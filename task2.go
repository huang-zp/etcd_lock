package main

import (
	"context"
	"fmt"
	"go.etcd.io/etcd/clientv3"
	"time"
)

func main() {
	//变量声明区域
	var (
		config clientv3.Config
		client *clientv3.Client
		err error
		lease clientv3.Lease
		leaseGrantResp *clientv3.LeaseGrantResponse
		leaseId clientv3.LeaseID
		keepRespChan <-chan *clientv3.LeaseKeepAliveResponse
		keepResp *clientv3.LeaseKeepAliveResponse
		ctx context.Context
		cancelFunc context.CancelFunc
		kv clientv3.KV
		txn clientv3.Txn
		txnResp *clientv3.TxnResponse
	)
	//客户端配置
	config = clientv3.Config{
		//etcd集合，这里只有一个etcd节点
		Endpoints: []string{"127.0.0.1:2379"},
		//5秒连接不到超时
		DialTimeout: 5 * time.Second,
	}
	//与etcd建立连接
	if client, err = clientv3.New(config);err != nil {
		fmt.Println(err)
		return
	}

	//租约客户端
	lease = clientv3.NewLease(client)

	//申请一个5秒的租约（就是说把创建kv时指定此租约，则kv生存时间只有5s）
	if leaseGrantResp, err = lease.Grant(context.TODO(), 5); err != nil {
		fmt.Println(err)
		return
	}
	// 拿到申请到5s租约的id
	leaseId = leaseGrantResp.ID


	// 这是用于取消自动续租的context
	ctx, cancelFunc = context.WithCancel(context.TODO())
	// 确保函数退出后，自动续租会停止
	defer cancelFunc()
	// 这个是直接让etcd把这个租约释放掉
	defer lease.Revoke(context.TODO(), leaseId)

	// 这是对租约自动续租
	if keepRespChan, err = lease.KeepAlive(ctx, leaseId); err != nil {
		fmt.Println(err)
		return
	}

	// 处理续租应答的协程，观察续租情况
	go func() {
		for {
			select {
			case keepResp = <- keepRespChan:
				if keepRespChan == nil {
					fmt.Println("租约失效")
					goto END
				} else {
					fmt.Println("收到自动续约的应答:", keepResp.ID)
				}

			}
		}
		END:
	}()


	kv = clientv3.NewKV(client)
	// 创建事务
	txn = kv.Txn(context.TODO())

	// if:key不存在 then:设置它，抢锁成功 else:抢锁失败
	txn.If(clientv3.Compare(clientv3.CreateRevision("lock"), "=", 0)).
		Then(clientv3.OpPut("lock", "task2", clientv3.WithLease(leaseId))).
		Else(clientv3.OpGet("lock"))

	// 提交事务
	if txnResp, err = txn.Commit(); err != nil {
		fmt.Println(err)
		return
	}

	// 如果没有抢到锁，就是执行了else
	if !txnResp.Succeeded {
		fmt.Println("锁被", string(txnResp.Responses[0].GetResponseRange().Kvs[0].Value), "占用")
		return
	}

	// 抢到锁后执行任务
	fmt.Println("处理任务")
	time.Sleep(10 * time.Second)
}
