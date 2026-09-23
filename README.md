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


``` C++
RpcServer server(&loop, serverFd);
// 该方法用于取消已注册进Server中的方法。该方法和registerMethod都只能在start前调用，且所调用线程为当前的base EventLoop

bool unregistered =
    server.unregisterMethod("EchoService", "Echo");

server.start();
```
<br>

``` C++
RpcServer server(&loop, serverFd);

bool registered = server.registerMethod(
    "EchoService", "Echo",
    [](const std::string& payload) -> RpcServer::RpcResult {
      return {ResponseResult::kSuccess, payload, ""};
    });

server.start();
```
<br>


### 客户端：
``` C++
RpcClient client(&loop, serverAddr);

client.call(
    "EchoService", "Echo", "hello",
    [](const RpcResponse& response) {
      // 根据 response.getResponseResult()
      // 读取 payload 或 errorMessage
    });
```
<br>

``` C++
// 这里两个call，主要的区别在最后是否带一个过期时间。四参数版本使用默认超时，默认值初始为 0ms，表示永不超时；但使用带过期时间的，则过期时间以调用时的为准，当调用时传递的为0则表示该调用永不过期。

client.call("EchoService", "Echo", "hello world",
    [&](const RpcResponse &response) {
    // 根据 response.getResponseResult()
    // 读取 payload 或 errorMessage
    });

client.call(
    "EchoService", "Echo", "request from thread 2",
    [&](const RpcResponse &response) {
    // 根据 response.getResponseResult()
    // 读取 payload 或 errorMessage
    },
    std::chrono::milliseconds(0));
```
<br>


``` C++
// 该方法用于取消对应请求的调用，若请求仍在等待响应，调用其原 callback 并返回失败响应；找不到 requestId 时静默返回。

uint64_t requestId = client.call("EchoService", "Echo", "hello world",
    [&](const RpcResponse &response) {
    // 根据 response.getResponseResult()
    // 读取 payload 或 errorMessage
    });
client.cancel(requestId);
```


``` C++
// 该方法用于设置默认过期时间,只能在客户端所属 EventLoop 调用,只能在首次 connect() 前调用。

client.setDefaultTimeout(std::chrono::milliseconds(100));
```
<br>

``` C++
// 该方法用于当前RpcClient同时最大的请求承载量，若当前Client的pending 请求数达到上限时直接返回调用错误的回调。默认不限制RpcClient的同时最大请求量。只能在客户端所属 EventLoop 调用,只能在首次 connect() 前调用。

client.setMaxPendingRequests(1);
```
<br>


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
- `call`支持跨线程调用，但对应的回调函数仍在 `RpcClient` 所在的 `EventLoop` 中执行。
- `cancel`支持跨线程调用，但对应的回调函数仍在 `RpcClient` 所在的 `EventLoop` 中执行。

## 请求生命周期
- `timeout == 0ms` 表示永不超时；
- 超时 callback 收到失败响应；
- 迟到响应会被忽略；
- 真正未知的 response requestId 会关闭连接；
- 连接关闭会使 pending 请求收到 "connect closed"；
- enableRetry() 只恢复 TCP 连接，不重发旧 RPC。
- 默认超时为永不超时
- 当超过pending上限后直接调用回调函数并提示请求失败
- 取消请求后直接调用回调函数并提示请求失败，若请求在取消前已发送给服务端在一定时间内客户端收到响应后直接忽略

## 当前限制

- 复杂序列化
- 服务端业务线程池
- 服务发现
- 断线自动重发
- 请求取消并不会通知服务端