#include <assert.h>
#include <cstdlib>
#include <iostream>
#include <sys/socket.h>

#include "EventLoop.h"
#include "InetAddress.h"
#include "RpcClient.h"
#include "RpcServer.h"

int main() {
  int serverFd =
      ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
  assert(serverFd >= 0);

  InetAddress serverAddr("127.0.0.1", 0);
  socklen_t socketLen = sizeof(sockaddr_in);
  assert(0 ==
         ::bind(serverFd,
                reinterpret_cast<const sockaddr *>(serverAddr.getSockAddr()),
                socketLen));
  assert(0 ==
         ::getsockname(serverFd,
                       reinterpret_cast<sockaddr *>(serverAddr.getSockAddr()),
                       &socketLen));

  EventLoop loop;
  RpcServer server(&loop, serverFd);
  server.setStopCompleteCallback(
      [&]() { loop.queueInLoop([&]() { loop.quit(); }); });
  bool res = server.registerMethod(
      "service", "method",
      [&](const std::string &payload) -> RpcServer::RpcResult {
        return RpcServer::RpcResult{ResponseResult::kSuccess, payload, ""};
      });
  assert(res);
  server.start();

  RpcClient client(&loop, serverAddr);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      assert(loop.isInLoopThread());

      client.call("service", "method", "hello world!",
                  [&](const RpcResponse &response) {
                    if (response.getResponseResult() ==
                        ResponseResult::kSuccess) {
                      std::cout << "response payload: " << response.getPayload()
                                << std::endl;
                    } else if (response.getResponseResult() ==
                               ResponseResult::kUnsuccess) {
                      std::cout << "response error message: "
                                << response.getErrorMessage() << std::endl;
                    }
                    client.disconnect();
                  });
    } else {
      server.stop(std::chrono::milliseconds(0));
    }
  });
  client.setConnectionErrorCallback([&](int) { std::abort(); });
  client.connect();

  loop.loop();
}