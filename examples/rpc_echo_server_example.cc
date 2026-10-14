#include "InetAddress.h"
#include "RpcServer.h"

#include <iostream>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

int main(int argc, char *argv[]) {
  uint16_t port = 0;
  if (argc == 2) {
    int value = std::stoi(std::string(argv[1]));
    if (value < 0 || value > 65535) {
      std::cerr << "usage: ./rpc_echo_server_example port(0-65535)"
                << std::endl;
      return 1;
    }
    port = value;
  } else {
    port = 0;
  }

  int serverFd =
      ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
  socklen_t socketLen = sizeof(sockaddr_in);
  InetAddress serverAddr("127.0.0.1", port);
  if (serverFd < 0) {
    std::cerr << "create socket error";
    return 1;
  }
  assert(serverFd >= 0);

  int res = ::bind(serverFd,
                   reinterpret_cast<const sockaddr *>(serverAddr.getSockAddr()),
                   sizeof(sockaddr_in));
  if (res != 0) {
    std::cerr << "bind error, error msg: ";
    perror("bind");
    close(serverFd);
    return 1;
  }
  assert(0 == res);

  res = ::getsockname(serverFd,
                      reinterpret_cast<sockaddr *>(serverAddr.getSockAddr()),
                      &socketLen);
  if (res != 0) {
    std::cerr << "get socket port error";
    perror("getsockname");
    close(serverFd);
    return 1;
  }
  assert(0 == res);

  std::cout << "server: " << serverAddr.toIp() << ":" << serverAddr.port()
            << std::endl;

  std::cout << "pid: " << (int)getpid() << std::endl;

  EventLoop loop;
  RpcServer server(&loop, serverFd);

  bool registered = server.registerMethod(
      "service", "echo",
      [&](const std::string &payload) -> RpcServer::RpcResult {
        return {ResponseResult::kSuccess, payload, ""};
      });
  assert(registered);

  registered = server.registerAsyncMethod(
      "service", "async_echo",
      [&](const std::string &payload, RpcCancellationToken cancellation,
          RpcServer::RpcReply reply) {
        if (cancellation.isCanceled()) {
          return;
        }

        reply({ResponseResult::kSuccess, payload, ""});
      });
  assert(registered);

  registered = server.registerAsyncMethod(
      "service", "async_echo_2",
      [&](const std::string &payload, RpcCancellationToken cancellation,
          RpcServer::RpcReply reply) {
        loop.runAfter(
            std::chrono::milliseconds(500),
            [payload, cancellation, reply = std::move(reply)]() mutable {
              if (cancellation.isCanceled()) {
                return;
              }

              reply({ResponseResult::kSuccess, payload, ""});
            });
      });
  assert(registered);

  server.start();
  loop.loop();
}