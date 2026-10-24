#include "CalculatorClient.h"
#include "CalculatorProtocol.h"
#include "CalculatorService.h"
#include "RpcClient.h"
#include "RpcServer.h"

#include <assert.h>
#include <future>
#include <string>

int main() {
  int serverFd =
      ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
  assert(serverFd >= 0);

  InetAddress serverAddr("127.0.0.1", 0);
  socklen_t socketLen = sizeof(sockaddr_in);

  int res = ::bind(serverFd,
                   reinterpret_cast<const sockaddr *>(serverAddr.getSockAddr()),
                   socketLen);
  assert(0 == res);

  res = ::getsockname(serverFd,
                      reinterpret_cast<sockaddr *>(serverAddr.getSockAddr()),
                      &socketLen);

  assert(0 == res);

  std::promise<void> testCompletePromise;
  auto testCompleteFuture = testCompletePromise.get_future();

  EventLoop loop;
  RpcServer server(&loop, serverFd);

  server.setStopCompleteCallback([&]() {
    testCompletePromise.set_value();
    loop.quit();
  });

  CalculatorService service;
  bool registered = service.registerCalculatorService(server);
  assert(registered);

  server.start();

  CalculatorClient client(&loop, serverAddr);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      client.add(-7, 12, [&](AddResult result) {
        if (result.status == ResponseResult::kSuccess) {
          std::cout << "-7 + 12 = " << result.value << std::endl;
        } else if (result.status == ResponseResult::kUnsuccess) {
          std::cout << "error: " << result.errorMessage << std::endl;
        }

        client.disconnect();
      });
    } else {
      server.stop(std::chrono::milliseconds(0));
    }
  });
  client.setConnectionErrorCallback([&](int) { std::abort(); });
  client.connect();

  loop.runAfter(std::chrono::milliseconds(3000), []() {
    std::cerr << "test timeout" << std::endl;
    std::abort();
  });

  loop.loop();

  auto waitForRes =
      testCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for server destroy timeout" << std::endl;
    std::abort();
  }

  return 0;
}