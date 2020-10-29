# 延迟队列服务
## 整体架构

## 背景

我们先看看以下业务场景：
* 当订单一直处于未支付状态时，如何及时的关闭订单，并退还库存？
* 3天后将订单待收货状态改为已收货
* 3天处方自动变为已审核
* 定时任务系统组件 等等

为了解决以上问题，目前已有实现逻辑：
* 1：定时扫描数据表
* 2：基于队列(Kakfa、Redis)，如果消息未到执行时间重回队列
* 3：框架自带的延迟队列组件（基于Redis）

当业务越来越多时，我们会发现这部分的逻辑非常类似。考虑将这部分逻辑从具体的业务逻辑里面抽出来，变成一个公共的部分。
对于延迟任务来说不要求时间完全对等，有一定误差是可以接受的，这是能抽离成服务的一个关键点。

## 设计要点
### Role
1: Timer 
	a: 任务投递到ReadyQueue之前需要先生成检测类型的任务到Bucket中（为了防止任务中断
	b: 接受任务并且负责扫描任务，把过期任务丢进执行队列中
2: Worker
	a: 仅从执行队列取出任务进行回调处理
3: Timer&Worker

#### Timer

这里从性能来说时间轮可能更为适合，从实现角度来讲定时扫描ZSET更为合适，不过整体更倾向于时间轮服务

#### Worker
* 任务确认&任务回调
* 重试次数

#### Message
消息状态
* delay：不可执行状态，等待时钟周期。
* ready：可执行状态，等待消费。
* reserved：已被消费者读取，执行中。
* finished: 已被消费完成。
* retry: 等待重试中。

消息类型
* 检测（Timer&Worker判断任务是否完成，是否需要重新执行，检测任务时间和次数由配置决定）
* 普通（Worker回调任务）

#### Ready-Queue
Redis List & Kakfa都可以

### 访问
#### GRPC
参考：https://github.com/zywaited/delay-queue/blob/master/protocol/pb/protobuf/service.proto
#### HTTP
* Add：/task/add
* Get: /task/get
* Remove: /task/remove

## TODO(设计)
### 注册
#### Key
服务类
* /med-delay-queue/role/timer/{groupId}/{timerName}  timer列表
* /med-delay-queue/role/worker/{groupId}/{workerName}  worker列表
* /med-delay-queue/bucket/num/{bucketName} 数量（不用实时，可一段时间同步数据，用于缩减或者选择Timer时使用）
* /med-delay-queue/config/[timer|worker] 配置
* /med-delay-queue/config/[timer|worker]/status 配置执行状态

锁类
* /med-delay-queue/lock/bucket/add/{bucketName} Bucket抢占锁
* /med-delay-queue/lock/bucket/reduce/{bucketName} Bucket抢占锁
* /med-delay-queue/lock/bucket/listen/{bucketName} Bucket抢占锁
* /med-delay-queue/lock/config 配置锁(主要用于新增或缩减Bucket)

### 部分细节
#### Timer
* 增加&缩减Bucket，抢占监听Bucket（预分配Bucket，每个Timer监听Bucket自身的Lock来新增或者缩减）
* 当前组内任务到达阈值或者是承重比列过高(ETCD有对应节点数据)，转投到其他Timer节点中(依赖服务注册)，
  如果只是当前节点承重高但组内整体不高(这里暂时简单用数量权衡)，则投递到组内其他Bucket中
* Bucket预分配，当有可用或者缩减Bucket时，先抢占配置锁，然后按照承重比列分配每个节点的Bucket，
  这一步需要监听listen相关的锁，如果监听到自身有新增就加入到处理，有减少则当前Bucket不再接受新的任务，
  当前Bucket还存在的任务转移到自身或者组内其他或者其他节点的Bucket中（分配任务切分，按新任务重新加入到节点中）
* 自身Bucket丢失，需要判断当前Bucket是否出现在其他节点，没有则是缩减，有则是变更

#### Worker
* 告警

### 重载
1：重载完所有数据后才进行删除操作 【仅与删除互斥】
2：重载完所有数据后才进行其他操作 【互斥】

目前为了性能选择了第一种方案，那么timer和worker不能独立部署，涉及到删除一致性 (后续可改成删除统一进行处理)

