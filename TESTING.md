# Testing Record

## rpc-v1-sync-short-connection

### 功能
- [x] LoginSuccess
- [x] LoginWrongPassword
- [x] LoginEmptyName
- [x] RegisterSuccess
- [x] RegisterEmptyName

### 错误路径
- [x] ServerNotRunning
- [x] InvalidIp
- [x] ServiceNotFound
- [x] MethodNotFound
- [x] BadPacket

### 结果
同步短连接 RPC 主链路通过。

---

## rpc-v2-business-thread-pool

### 功能
- [x] 原 v1 consumer_test 全部通过
- [x] 业务线程池开启后 Login/Register 正常
- [x] controller 错误响应正常返回

### 并发
- [x] 50 个 consumer 并发调用通过
- [x] 10000 次循环调用无协议解析错误
- [x] 慢业务 sleep 测试：IO 线程不被阻塞

### 工具
- [x] ASan 通过
- [x] TSan 通过

### 修复记录
- 修复跨线程 send 时局部 send_buf 生命周期问题。
- 修复 request_frame 未 resize 就 memcpy 的问题。