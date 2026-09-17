# mini_rpc

基于 mini_muduo 实现的第一版 C++ RPC。

## Scope

- 服务端注册 service 和 method。
- 客户端通过 service、method 和字符串 payload 发起异步调用。
- 服务端返回成功 payload 或失败 errorMessage。

本版本不包含：
- 复杂参数与复杂序列化；
- 服务发现、负载均衡、注册中心；
- 服务端业务线程池调度；
- 断线后的 RPC 自动重发。

## Build and run

```bash
cmake -S . -B build
cmake --build build
./build/rpc_echo_example
```
rpc_echo_example 会在同一进程中启动 Echo 服务端和客户端，输出响应 payload 后优雅退出。

## 基本用法
### 服务端：
``` C+
RpcServer server(&loop, serverFd);

bool registered = server.registerMethod(
    "EchoService", "Echo",
    [](const std::string& payload) -> RpcServer::RpcResult {
      return {ResponseResult::kSuccess, payload, ""};
    });

server.start();
```

### 客户端：
``` C+
RpcClient client(&loop, serverAddr);

client.call(
    "EchoService", "Echo", "hello",
    [](const RpcResponse& response) {
      // 根据 response.getResponseResult()
      // 读取 payload 或 errorMessage
    });
```

## 协议
每条 RPC 消息先由 LengthHeaderCodec 分帧；它只处理消息边界，
不理解 RPC 请求或响应的字段含义。

|Message|Fields|
| :---: | :---: |
|Request|messageType, requestId, service, method, payload|
|Response|messageType, requestId, result, payload, errorMessage|


- requestId 由客户端生成，服务端原样返回。
- 客户端用 requestId 匹配 pending callback。
- service、method、payload、errorMessage 都是二进制安全字符串。
- 成功响应的 errorMessage 为空；失败响应的 payload 为空。


## 对象所属关系和线程
- EventLoop 由调用方拥有；
- RpcServer 拥有 TcpServer 和服务端 codec；
- RpcClient 拥有 TcpClient 和客户端 codec；
- handler 在连接所属 IO loop 执行；
- client 的连接回调和 RPC callback 在 client EventLoop 执行；
- `threadNum == 0` 时服务端 handler 在 base EventLoop 执行。

## 请求生命周期
- timeout == 0ms 表示永不超时；
- 超时 callback 收到失败响应；
- 迟到响应会被忽略；
- 真正未知的 response requestId 会关闭连接；
- 连接关闭会使 pending 请求收到 "connect closed"；
- enableRetry() 只恢复 TCP 连接，不重发旧 RPC。

## 目前缺陷
这里简短重申第一版的边界，并链接到：
- examples/rpc_echo_example.cc
- tests/rpc_server_test.cc