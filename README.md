# 延迟队列服务
## 设计要点
### Role
* 1: Timer 
	* a: 任务投递到ReadyQueue之前需要先生成检测类型的任务到Bucket中（为了防止任务中断)
	* b: 接受任务并且负责扫描任务，把过期任务丢进执行队列中
* 2: Worker
	* a: 仅从执行队列取出任务进行回调处理
* 3: Timer&Worker

#### Timer
* 1: redis zset
* 2: 本地时间轮【如果是该类型，timer启动时会重载之前的数据进入内存】

#### Worker
* 任务确认&任务回调
* 重试次数

#### Ready-Queue
* 1: redis list
* 2: memory chan 【role: timer&worker】

## 配置
参考: https://github.com/zywaited/delay-queue/blob/master/config/config.example.toml

### 如果使用mongo存储
```mongo
# 创建uid唯一索引
db.task.createIndex({"uid":1},{"unique":1})
# 创建分数&重载索引
db.task.createIndex({"token":1, "score":1},{"unique":1})
```

## 访问
参考: https://github.com/zywaited/delay-queue/blob/master/visit.md

## 其他
### 任务检测
* 目前配置的最大检测次数是8次
* 每次回调配置次数是3次

### 回调时间
* 2次重试回调的时间：2，4 (s)
* 回调HTTP必须是返回的状态码为<400才算成功, GRPC不返回err即可

### 检测
* 实际检测次数会比配置多一次，比如配置8次，实际检测是9次，最后一次不进入队列，只是为了确认任务是否回调成功，没有成功就打印日志并且去除当前任务(防止无效任务过多)

##### 每次检测延后执行时间
* 1-3：15、30、60 (s)
* 4-6：15、30、60 (m)
* 7-9：4、8、12 (h)
* 10及以上：1 (d)

