#include "CalculatorProtocol.h"
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
  RpcClient client(&loop, serverAddr);

  server.setStopCompleteCallback([&]() {
    testCompletePromise.set_value();
    loop.quit();
  });
  bool registered = server.registerAsyncMethod(
      "Calculator", "Add",
      [&](const std::string &payload, RpcCancellationToken cancellation,
          RpcServer::RpcReply reply) {
        std::string errorMessage;
        CalculatorProtocol calculatorAdd;
        bool res = calculatorAdd.requestDecode(payload, errorMessage);

        if (!res || !errorMessage.empty()) {
          reply(RpcServer::RpcResult{ResponseResult::kUnsuccess, "",
                                     "invalid add request"});
          return;
        }

        if (cancellation.isCanceled()) {
          return;
        }

        int64_t result =
            static_cast<int64_t>(calculatorAdd.getA()) + calculatorAdd.getB();
        if (result > INT32_MAX || result < INT32_MIN) {
          reply(RpcServer::RpcResult{ResponseResult::kUnsuccess, "",
                                     "integer overflow"});
          return;
        }

        calculatorAdd.setResult(result);
        std::string msg = calculatorAdd.resultEncode();
        reply(RpcServer::RpcResult{ResponseResult::kSuccess, msg, ""});
      });
  assert(registered);

  server.start();

  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      CalculatorProtocol calculatorAdd(-7, 12);
      std::string msg = calculatorAdd.requestEncode();

      client.call("Calculator", "Add", msg, [&](const RpcResponse &response) {
        if (response.getResponseResult() == ResponseResult::kSuccess) {
          CalculatorProtocol calculatorAdd;
          std::string errorMessage;
          bool result =
              calculatorAdd.resultDecode(response.getPayload(), errorMessage);
          if (!result || !errorMessage.empty()) {
            std::cout << "calculator decode error: " << errorMessage << std::endl;
            return;
          }

          std::cout << "-7 + 12 = " << calculatorAdd.getResult() << std::endl;
        } else {
          std::cout << "response error: " << response.getErrorMessage()
                    << std::endl;
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