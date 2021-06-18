### 1 协议

#### 1.1 GRPC

参考: https://github.com/zywaited/delay-queue/blob/master/protocol/pb/protobuf/service.proto

#### 1.2 HTTP

| 类型     | 方法         | 方式         |
| -------- | ------------ | ------------ |
| 添加任务 | /task/add    | POST         |
| 删除任务 | /task/remove/:uid | POST、DELETE |
| 查询任务 | /task/get/:uid    | GET          |

### 2 方法
#### 2.1 添加任务
##### 参数说明
|       字段       |     类型     | 是否必须 |                             说明                             |
| -------------- | ---------- | ------ | ---------------------------------------------------------- |
|       name       |    string    |    是    |    业务名称    |
|       args       |    string    |    否    |    业务自定义数据（尽量不要传输复杂逻辑，业务方自行存储）    |
|       time       | object(json) |    否    |                           执行时间                           |
|  time.duration   |      int      |    否    |              执行时间戳(如果是相对则是延迟时间)              |
|  time.relative   |     bool     |    否    | 是否是相对时间(如果是相对时间，那么真正执行时间为NOW()+duration) |
|     callback     | object(json) |    是    |        回调路径(如果是HTTP，uri为schema+address+path)        |
| callback.schema  |    string     |    是    |                  协议: 支持http:https:grpc                   |
| callback.address |    string    |    是    |                   地址（域名或者ip+port）                    |
|  callback.path   |    string    |    否    |                             路径                             |

##### 返回值(json)
| 字段 | 类型   | 说明                                                         |
| ---- | ------ | ------------------------------------------------------------ |
| uid  | string | 任务唯一标识（根据传入参数计算，相对时间会转为绝对时间计算，尽量都用绝对时间） |

#### 2.2 查询与删除
| 字段 | 类型   | 说明                                                         |
| ---- | ------ | ------------------------------------------------------------ |
| uid  | string | 添加任务返回 |

### 3 回调
#### 参数(json)

| 字段 | 类型   | 是否必须 | 说明                   |
| ---- | ------ | -------- | ---------------------- |
| uid  | string | 是       | 与添加任务返回值一致   |
| name | string | 是       | 与添加任务参数name一致 |
| args | string | 是       | 与添加任务参数args一致 |
| url  | string | 是       | 拼接的回调地址         |

#### 协议
##### HTTP
回调只能是POST方式

##### GRPC
* 1: 当path为空时，需要实现pb协议中的CallbackServer，回调时执行该方法
* 2: 当path不为空时，那么直接回调指定方法（特别注意该路径是sdk生成的路径，也就是服务名+方法，如：/delay-queue.Callback/Send）（推荐使用该逻辑）