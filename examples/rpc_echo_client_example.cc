#include "EventLoop.h"
#include "InetAddress.h"
#include "RpcClient.h"

#include <stdint.h>
#include <sys/types.h>
#include <unistd.h>

int main(int argc, char *argv[]) {
  if (argc != 3) {
    std::cerr << "usage: ./rpc_echo_client_example ip port" << std::endl;
    return 1;
  }

  char *ip = argv[1];

  int value = std::stoi(std::string(argv[2]));
  if (value < 1 || value > 65535) {
    std::cerr << "usage: ./rpc_echo_client_example ip port(1-65535)" << std::endl;
    return 1;
  }
  uint16_t port = value;

  int callbackCnt = 0;

  EventLoop loop;
  RpcClient client(&loop, InetAddress(ip, port));
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      std::cout << "connected" << std::endl;
      client.call("service", "echo", "Hello World",
                  [&client, &callbackCnt](const RpcResponse &response) {
                    if (response.getResponseResult() ==
                        ResponseResult::kSuccess) {
                      std::cout << "response payload: " << response.getPayload()
                                << std::endl;
                    } else {
                      std::cerr << "call error: " << response.getErrorMessage()
                                << std::endl;
                    }

                    ++callbackCnt;
                    if (callbackCnt == 3) {
                      client.disconnect();
                    }
                  });
      client.call("service", "async_echo", "Hello World async",
                  [&client, &callbackCnt](const RpcResponse &response) {
                    if (response.getResponseResult() ==
                        ResponseResult::kSuccess) {
                      std::cout << "response payload: " << response.getPayload()
                                << std::endl;
                    } else {
                      std::cerr << "call error: " << response.getErrorMessage()
                                << std::endl;
                    }

                    ++callbackCnt;
                    if (callbackCnt == 3) {
                      client.disconnect();
                    }
                  });
      client.call("service", "async_echo_2", "Hello World async 2",
                  [&client, &callbackCnt](const RpcResponse &response) {
                    if (response.getResponseResult() ==
                        ResponseResult::kSuccess) {
                      std::cout << "response payload: " << response.getPayload()
                                << std::endl;
                    } else {
                      std::cerr << "call error: " << response.getErrorMessage()
                                << std::endl;
                    }

                    ++callbackCnt;
                    if (callbackCnt == 3) {
                      client.disconnect();
                    }
                  });
    } else {
      loop.quit();
    }
  });
  client.setConnectionErrorCallback([&](int err) {
    std::cerr << "connect error, error code: " << err << std::endl;
    std::abort();
  });
  client.connect();
  loop.loop();
}