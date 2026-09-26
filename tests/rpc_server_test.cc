#include <assert.h>
#include <atomic>
#include <chrono>
#include <functional>
#include <future>
#include <iostream>
#include <memory>
#include <mutex>
#include <set>
#include <stdexcept>
#include <sys/socket.h>
#include <thread>
#include <vector>

#include "EventLoopThread.h"
#include "InetAddress.h"
#include "LengthHeaderCodec.h"
#include "RpcClient.h"
#include "RpcMessage.h"
#include "RpcServer.h"
#include "TcpClient.h"

void rpc_server_processes_echo_request_test() {
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

  std::promise<void> stopCompletePromise;
  auto stopCompleteFuture = stopCompletePromise.get_future();

  int connected = 0;
  int rpcRoundSuccess = 0;
  int serverStopped = 0;

  EventLoop loop;
  std::unique_ptr<RpcServer> rpcServer =
      std::make_unique<RpcServer>(&loop, serverFd);
  rpcServer->setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      loop.queueInLoop([&]() {
        rpcServer.reset();
        ++serverStopped;
        stopCompletePromise.set_value();
        loop.quit();
      });
    });
  });
  bool registered = rpcServer->registerMethod(
      "EchoService", "Echo", [](const std::string &payload) {
        return RpcServer::RpcResult{ResponseResult::kSuccess, payload, ""};
      });
  assert(registered);

  rpcServer->start();

  LengthHeaderCodec codec;
  TcpClient client(&loop, serverAddr);
  codec.setMessageCallback(
      [&](const TcpConnectionPtr &, const std::string &msg) {
        RpcResponse response;
        std::string errorMsg;
        bool res = response.decode(msg, errorMsg);
        if (!res || !errorMsg.empty()) {
          std::cerr << "response decode error" << std::endl;
          std::abort();
        }

        assert(response.getRequestId() == 1);
        assert(response.getResponseResult() == ResponseResult::kSuccess);
        assert(response.getPayload() == "hello");
        assert(response.getErrorMessage().empty());

        ++rpcRoundSuccess;
        client.disconnect();
      });

  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;
      RpcRequest request(1, "EchoService", "Echo", "hello");
      std::string output;
      std::string errorMsg;
      bool res = request.encode(output, errorMsg);
      if (!res || !errorMsg.empty()) {
        assert(res && errorMsg.empty());
        std::abort();
      }

      codec.send(conn, output.data(), output.length());
    } else {
      rpcServer->stop(std::chrono::milliseconds(0));
    }
  });
  client.setPeerHalfCloseCallback(
      [&](const TcpConnectionPtr &conn) { conn->shutdown(); });
  client.setMessageCallback(std::bind(&LengthHeaderCodec::onMessage, &codec,
                                      std::placeholders::_1,
                                      std::placeholders::_2));
  client.setConnectionErrorCallback([&](int) {
    std::cerr << "client connect error" << std::endl;
    std::abort();
  });
  client.connect();

  loop.runAfter(std::chrono::milliseconds(3000), []() {
    std::cerr << "test timeout" << std::endl;
    std::abort();
  });

  loop.loop();

  auto waitForRes =
      stopCompleteFuture.wait_for(std::chrono::milliseconds(1000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(rpcRoundSuccess == 1);
  assert(serverStopped == 1);
}

void rpc_client_calls_echo_service_test() {
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

  std::promise<void> stopCompletePromise;
  auto stopCompleteFuture = stopCompletePromise.get_future();

  int connected = 0;
  int rpcRoundSuccess = 0;
  int serverStopped = 0;

  EventLoop loop;
  RpcServer server(&loop, serverFd);
  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      stopCompletePromise.set_value();
      loop.quit();
    });
  });
  bool registered = server.registerMethod(
      "EchoService", "Echo", [](const std::string &payload) {
        return RpcServer::RpcResult{ResponseResult::kSuccess, payload, ""};
      });
  assert(registered);

  server.start();

  RpcClient client(&loop, serverAddr);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;
      client.call("EchoService", "Echo", "hello world",
                  [&](const RpcResponse &response) {
                    assert(response.getResponseResult() ==
                           ResponseResult::kSuccess);
                    assert(response.getPayload() == "hello world");

                    ++rpcRoundSuccess;
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
      stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(rpcRoundSuccess == 1);
  assert(serverStopped == 1);
}

void rpc_client_receives_error_for_unknown_service_test() {
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

  std::promise<void> stopCompletePromise;
  auto stopCompleteFuture = stopCompletePromise.get_future();

  int connected = 0;
  int rpcErrorResponseReceived = 0;
  int serverStopped = 0;

  EventLoop loop;
  RpcServer server(&loop, serverFd);
  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      stopCompletePromise.set_value();
      loop.quit();
    });
  });
  server.start();

  RpcClient client(&loop, serverAddr);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;
      client.call("UnknownService", "Echo", "hello world",
                  [&](const RpcResponse &response) {
                    assert(response.getResponseResult() ==
                           ResponseResult::kUnsuccess);
                    assert(response.getPayload().empty());
                    assert(!response.getErrorMessage().empty());

                    ++rpcErrorResponseReceived;
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
      stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(rpcErrorResponseReceived == 1);
  assert(serverStopped == 1);
}

void rpc_client_reports_error_when_not_connected_test() {
  int callResponseCnt = 0;

  EventLoop loop;
  InetAddress serverAddr("127.0.0.1", 0);
  RpcClient client(&loop, serverAddr);

  client.call("UnknownService", "Echo", "hello world",
              [&](const RpcResponse &response) {
                assert(response.getResponseResult() ==
                       ResponseResult::kUnsuccess);
                assert(response.getPayload().empty());
                assert(!response.getErrorMessage().empty());
                ++callResponseCnt;
              });

  assert(callResponseCnt == 1);
}

void rpc_client_reports_error_when_connection_closes_with_pending_request_test() {
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

  std::promise<void> stopCompletePromise;
  auto stopCompleteFuture = stopCompletePromise.get_future();

  int connected = 0;
  int rpcErrorCallbackCount = 0;
  int clientDisconnected = 0;
  int serverRequestReceived = 0;
  int serverStopped = 0;
  bool rpcFailureReported = false;

  EventLoop loop;
  LengthHeaderCodec codec;
  TcpServer server(&loop, serverFd);

  codec.setMessageCallback(
      [&](const TcpConnectionPtr &conn, const std::string &) {
        ++serverRequestReceived;
        conn->forceClose();
        return;
      });
  server.setMessageCallback(std::bind(&LengthHeaderCodec::onMessage, &codec,
                                      std::placeholders::_1,
                                      std::placeholders::_2));

  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      stopCompletePromise.set_value();
      loop.quit();
    });
  });
  server.start();

  RpcClient client(&loop, serverAddr);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;
      client.call("EchoService", "Echo", "hello world",
                  [&](const RpcResponse &response) {
                    assert(response.getResponseResult() ==
                           ResponseResult::kUnsuccess);
                    assert(response.getPayload().empty());
                    assert(!response.getErrorMessage().empty());

                    ++rpcErrorCallbackCount;
                    rpcFailureReported = true;
                  });
    } else {
      assert(rpcFailureReported);
      ++clientDisconnected;
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
      stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(serverRequestReceived == 1);
  assert(rpcErrorCallbackCount == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
}

void rpc_server_rejects_duplicate_method_registration_test() {
  EventLoop loop;
  int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
  assert(fd >= 0);

  RpcServer server(&loop, fd);
  bool res = server.registerMethod(
      "EchoService", "echo",
      [&](const std::string &payload) -> RpcServer::RpcResult {
        return RpcServer::RpcResult{};
      });
  assert(res == true);

  res = server.registerMethod(
      "EchoService", "reverse",
      [&](const std::string &payload) -> RpcServer::RpcResult {
        return RpcServer::RpcResult{};
      });
  assert(res == true);

  res = server.registerMethod(
      "EchoService", "echo",
      [&](const std::string &payload) -> RpcServer::RpcResult {
        return RpcServer::RpcResult{};
      });
  assert(res == false);
}

void rpc_client_receives_error_for_invalid_handler_result_test() {
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

  std::promise<void> stopCompletePromise;
  auto stopCompleteFuture = stopCompletePromise.get_future();

  int connected = 0;
  int handlerInvoked = 0;
  int rpcErrorResponseReceived = 0;
  int clientDisconnected = 0;
  int serverStopped = 0;

  EventLoop loop;
  RpcServer server(&loop, serverFd);
  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      stopCompletePromise.set_value();
      loop.quit();
    });
  });
  bool registered = server.registerMethod(
      "Return fail service", "return fail",
      [&](const std::string &payload) -> RpcServer::RpcResult {
        ++handlerInvoked;
        return RpcServer::RpcResult{ResponseResult::kUnsuccess,
                                    "payload must be empty for an error result",
                                    ""};
      });
  assert(registered);

  server.start();

  RpcClient client(&loop, serverAddr);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;
      client.call(
          "Return fail service", "return fail", "hello world",
          [&](const RpcResponse &response) {
            assert(response.getResponseResult() == ResponseResult::kUnsuccess);
            assert(response.getPayload().empty());
            assert(response.getErrorMessage() == "invalid handler result");

            ++rpcErrorResponseReceived;
            client.disconnect();
          });
    } else {
      ++clientDisconnected;
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
      stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(handlerInvoked == 1);
  assert(rpcErrorResponseReceived == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
}

void rpc_client_matches_out_of_order_responses_by_request_id_test() {
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

  int serverRequestReceived = 0;
  int firstCallbackCount = 0;
  int secondCallbackCount = 0;
  int clientDisconnected = 0;
  int serverStopped = 0;

  bool secondCallbackRan = false;

  std::vector<RpcRequest> requests;

  std::promise<void> stopCompletePromise;
  auto stopCompleteFuture = stopCompletePromise.get_future();

  EventLoop loop;
  LengthHeaderCodec codec;
  TcpServer server(&loop, serverFd);

  codec.setMessageCallback(
      [&](const TcpConnectionPtr &conn, const std::string &msg) {
        ++serverRequestReceived;

        RpcRequest request;
        std::string errorMessage;
        bool res = request.decode(msg, errorMessage);
        if (!res || !errorMessage.empty()) {
          conn->forceClose();
          return;
        }

        if (serverRequestReceived == 1) {
          assert(request.getService() == "first service");
          assert(request.getMethod() == "first method");
        } else if (serverRequestReceived == 2) {
          assert(request.getService() == "second service");
          assert(request.getMethod() == "second method");
        }

        requests.emplace_back(request);
        if (requests.size() == 2) {
          for (int i = requests.size() - 1; i >= 0; --i) {
            RpcResponse response(
                requests.at(i).getRequestId(), ResponseResult::kSuccess,
                (i == 0) ? "response for first" : "response for second", "");
            std::string outputMessage;
            errorMessage.clear();
            bool res = response.encode(outputMessage, errorMessage);
            if (!res || !errorMessage.empty()) {
              conn->forceClose();
              return;
            }

            if (conn->connected()) {
              codec.send(conn, outputMessage.data(), outputMessage.length());
            }
          }
        }
      });
  server.setMessageCallback(std::bind(&LengthHeaderCodec::onMessage, &codec,
                                      std::placeholders::_1,
                                      std::placeholders::_2));
  server.setPeerHalfCloseCallback(
      [&](const TcpConnectionPtr &conn) { conn->shutdown(); });
  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      stopCompletePromise.set_value();
      loop.quit();
    });
  });
  server.start();

  RpcClient client(&loop, serverAddr);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      client.call("first service", "first method", "first",
                  [&](const RpcResponse &response) {
                    ++firstCallbackCount;
                    assert(response.getRequestId() == 1);
                    assert(response.getResponseResult() ==
                           ResponseResult::kSuccess);
                    assert(response.getPayload() == "response for first");
                    assert(response.getErrorMessage().empty());

                    assert(secondCallbackRan);
                    client.disconnect();
                  });
      client.call("second service", "second method", "second",
                  [&](const RpcResponse &response) {
                    ++secondCallbackCount;
                    assert(response.getRequestId() == 2);
                    assert(response.getResponseResult() ==
                           ResponseResult::kSuccess);
                    assert(response.getPayload() == "response for second");
                    assert(response.getErrorMessage().empty());

                    secondCallbackRan = true;
                  });
    } else {
      ++clientDisconnected;
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
      stopCompleteFuture.wait_for(std::chrono::milliseconds(3000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(serverRequestReceived == 2);
  assert(firstCallbackCount == 1);
  assert(secondCallbackCount == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
}

void rpc_client_closes_connection_for_unknown_response_request_id_test() {
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

  int serverRequestReceived = 0;
  int forgedResponseSent = 0;
  int pendingRequestFailed = 0;
  int clientDisconnected = 0;
  int serverStopped = 0;

  bool pendingFailureReported = false;

  std::promise<void> stopCompletePromise;
  auto stopCompleteFuture = stopCompletePromise.get_future();

  EventLoop loop;
  LengthHeaderCodec codec;
  TcpServer server(&loop, serverFd);

  codec.setMessageCallback([&](const TcpConnectionPtr &conn,
                               const std::string &msg) {
    ++serverRequestReceived;

    RpcRequest request;
    std::string errorMessage;
    bool res = request.decode(msg, errorMessage);
    if (!res || !errorMessage.empty()) {
      conn->forceClose();
      return;
    }

    RpcResponse response(request.getRequestId() + 1000,
                         ResponseResult::kSuccess, request.getPayload(), "");
    std::string outputMessage;
    errorMessage.clear();
    res = response.encode(outputMessage, errorMessage);
    if (!res || !errorMessage.empty()) {
      conn->forceClose();
      return;
    }

    if (conn->connected()) {
      ++forgedResponseSent;
      codec.send(conn, outputMessage.data(), outputMessage.length());
    }
  });
  server.setMessageCallback(std::bind(&LengthHeaderCodec::onMessage, &codec,
                                      std::placeholders::_1,
                                      std::placeholders::_2));
  server.setPeerHalfCloseCallback(
      [&](const TcpConnectionPtr &conn) { conn->shutdown(); });
  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      stopCompletePromise.set_value();
      loop.quit();
    });
  });
  server.start();

  RpcClient client(&loop, serverAddr);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      client.call(
          "service", "method", "payload", [&](const RpcResponse &response) {
            assert(response.getResponseResult() == ResponseResult::kUnsuccess);
            assert(response.getPayload().empty());
            assert(!response.getErrorMessage().empty());

            ++pendingRequestFailed;
            pendingFailureReported = true;
          });
    } else {
      ++clientDisconnected;
      assert(pendingFailureReported);
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
      stopCompleteFuture.wait_for(std::chrono::milliseconds(3000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(serverRequestReceived == 1);
  assert(forgedResponseSent == 1);
  assert(pendingRequestFailed == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
}

void rpc_client_receives_business_error_from_registered_handler_test() {
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

  std::promise<void> stopCompletePromise;
  auto stopCompleteFuture = stopCompletePromise.get_future();

  int connected = 0;
  int handlerInvoked = 0;
  int rpcBusinessErrorReceived = 0;
  int clientDisconnected = 0;
  int serverStopped = 0;

  EventLoop loop;
  RpcServer server(&loop, serverFd);
  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      stopCompletePromise.set_value();
      loop.quit();
    });
  });
  bool registered = server.registerMethod(
      "service", "method", [&](const std::string &payload) {
        ++handlerInvoked;
        return RpcServer::RpcResult{ResponseResult::kUnsuccess, "",
                                    "business failed"};
      });
  assert(registered);

  server.start();

  RpcClient client(&loop, serverAddr);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;
      client.call(
          "service", "method", "hello world", [&](const RpcResponse &response) {
            assert(response.getResponseResult() == ResponseResult::kUnsuccess);
            assert(response.getPayload().empty());
            assert(response.getErrorMessage() == "business failed");

            ++rpcBusinessErrorReceived;
            client.disconnect();
          });
    } else {
      ++clientDisconnected;
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
      stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(handlerInvoked == 1);
  assert(rpcBusinessErrorReceived == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
}

void rpc_client_reports_timeout_when_server_does_not_respond_test() {
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

  int serverRequestReceived = 0;
  int timeoutCallbackCount = 0;
  int clientDisconnected = 0;
  int serverStopped = 0;

  bool timeoutReported = false;

  std::promise<void> stopCompletePromise;
  auto stopCompleteFuture = stopCompletePromise.get_future();

  EventLoop loop;
  LengthHeaderCodec codec;
  TcpServer server(&loop, serverFd);

  codec.setMessageCallback(
      [&](const TcpConnectionPtr &conn, const std::string &msg) {
        ++serverRequestReceived;

        RpcRequest request;
        std::string errorMessage;
        bool res = request.decode(msg, errorMessage);
        if (!res || !errorMessage.empty()) {
          conn->forceClose();
          return;
        }

        assert(request.getService() == "service");
        assert(request.getMethod() == "method");
        assert(request.getPayload() == "payload");
      });
  server.setMessageCallback(std::bind(&LengthHeaderCodec::onMessage, &codec,
                                      std::placeholders::_1,
                                      std::placeholders::_2));
  server.setPeerHalfCloseCallback(
      [&](const TcpConnectionPtr &conn) { conn->shutdown(); });
  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      stopCompletePromise.set_value();
      loop.quit();
    });
  });
  server.start();

  RpcClient client(&loop, serverAddr);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      client.call(
          "service", "method", "payload",
          [&](const RpcResponse &response) {
            assert(response.getResponseResult() == ResponseResult::kUnsuccess);
            assert(response.getPayload().empty());
            assert(response.getErrorMessage() == "rpc request timeout");

            ++timeoutCallbackCount;
            timeoutReported = true;
            client.disconnect();
          },
          std::chrono::milliseconds(100));
    } else {
      ++clientDisconnected;
      assert(timeoutReported);
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
      stopCompleteFuture.wait_for(std::chrono::milliseconds(3000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(serverRequestReceived == 1);
  assert(timeoutCallbackCount == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
}

void rpc_client_does_not_timeout_when_timeout_is_zero_test() {
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

  int serverRequestReceived = 0;
  int connectionCloseCallbackCount = 0;
  int clientDisconnected = 0;
  int serverStopped = 0;

  bool connectionCloseReported = false;

  std::promise<void> stopCompletePromise;
  auto stopCompleteFuture = stopCompletePromise.get_future();

  EventLoop loop;
  LengthHeaderCodec codec;
  TcpServer server(&loop, serverFd);

  codec.setMessageCallback(
      [&](const TcpConnectionPtr &conn, const std::string &msg) {
        ++serverRequestReceived;

        RpcRequest request;
        std::string errorMessage;
        bool res = request.decode(msg, errorMessage);
        if (!res || !errorMessage.empty()) {
          conn->forceClose();
          return;
        }

        assert(request.getService() == "service");
        assert(request.getMethod() == "method");
        assert(request.getPayload() == "payload");
      });
  server.setMessageCallback(std::bind(&LengthHeaderCodec::onMessage, &codec,
                                      std::placeholders::_1,
                                      std::placeholders::_2));
  server.setPeerHalfCloseCallback(
      [&](const TcpConnectionPtr &conn) { conn->shutdown(); });
  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      stopCompletePromise.set_value();
      loop.quit();
    });
  });
  server.start();

  RpcClient client(&loop, serverAddr);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      client.call(
          "service", "method", "payload",
          [&](const RpcResponse &response) {
            assert(response.getResponseResult() == ResponseResult::kUnsuccess);
            assert(response.getPayload().empty());
            assert(response.getErrorMessage() == "connect closed");

            ++connectionCloseCallbackCount;
            connectionCloseReported = true;
          },
          std::chrono::milliseconds(0));

      conn->loop()->runAfter(std::chrono::milliseconds(100), [&] {
        assert(connectionCloseCallbackCount == 0);
        client.disconnect();
      });
    } else {
      ++clientDisconnected;
      assert(connectionCloseReported);
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
      stopCompleteFuture.wait_for(std::chrono::milliseconds(3000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(serverRequestReceived == 1);
  assert(connectionCloseCallbackCount == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
}

void rpc_server_executes_handler_on_io_loop_test() {
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

  std::promise<void> stopCompletePromise;
  auto stopCompleteFuture = stopCompletePromise.get_future();

  int connected = 0;
  std::atomic<int> handlerInvoked{0};
  std::atomic<bool> handlerRanOnIoThread{false};
  int rpcRoundSuccess = 0;
  int clientDisconnected = 0;
  int serverStopped = 0;

  EventLoop loop;
  RpcServer server(&loop, serverFd, 1);
  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      stopCompletePromise.set_value();
      loop.quit();
    });
  });
  bool registered = server.registerMethod(
      "EchoService", "Echo", [&](const std::string &payload) {
        assert(!loop.isInLoopThread());
        ++handlerInvoked;
        handlerRanOnIoThread.store(true);
        return RpcServer::RpcResult{ResponseResult::kSuccess, payload, ""};
      });
  assert(registered);

  server.start();

  RpcClient client(&loop, serverAddr);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;
      client.call("EchoService", "Echo", "hello world",
                  [&](const RpcResponse &response) {
                    loop.assertInLoopThread();
                    assert(response.getResponseResult() ==
                           ResponseResult::kSuccess);
                    assert(response.getPayload() == "hello world");
                    assert(response.getErrorMessage().empty());

                    ++rpcRoundSuccess;
                    client.disconnect();
                  });
    } else {
      ++clientDisconnected;
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
      stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(handlerInvoked.load() == 1);
  assert(handlerRanOnIoThread.load());
  assert(rpcRoundSuccess == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
}

void rpc_server_distributes_connections_across_io_loops_test() {
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

  std::promise<void> stopCompletePromise;
  auto stopCompleteFuture = stopCompletePromise.get_future();

  std::mutex mutex;
  std::set<std::thread::id> threadIds;

  int clientConnected = 0;
  std::atomic<int> handlerInvoked{0};
  int client1RpcRoundSuccess = 0;
  int client2RpcRoundSuccess = 0;
  int clientDisconnected = 0;
  int serverStopped = 0;

  EventLoop loop;
  RpcServer server(&loop, serverFd, 2);
  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      stopCompletePromise.set_value();
      loop.quit();
    });
  });
  bool registered = server.registerMethod(
      "EchoService", "Echo", [&](const std::string &payload) {
        assert(!loop.isInLoopThread());
        ++handlerInvoked;
        {
          std::lock_guard<std::mutex> locker(mutex);
          threadIds.insert(std::this_thread::get_id());
        }
        return RpcServer::RpcResult{ResponseResult::kSuccess, payload, ""};
      });
  assert(registered);

  server.start();

  RpcClient client_1(&loop, serverAddr);
  client_1.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++clientConnected;
      client_1.call("EchoService", "Echo", "hello world",
                    [&](const RpcResponse &response) {
                      loop.assertInLoopThread();
                      assert(response.getResponseResult() ==
                             ResponseResult::kSuccess);
                      assert(response.getPayload() == "hello world");
                      assert(response.getErrorMessage().empty());

                      ++client1RpcRoundSuccess;
                      client_1.disconnect();
                    });
    } else {
      ++clientDisconnected;
      if (clientDisconnected == 2) {
        server.stop(std::chrono::milliseconds(0));
      }
    }
  });
  client_1.setConnectionErrorCallback([&](int) { std::abort(); });
  client_1.connect();

  RpcClient client_2(&loop, serverAddr);
  client_2.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++clientConnected;
      client_2.call("EchoService", "Echo", "hello world",
                    [&](const RpcResponse &response) {
                      loop.assertInLoopThread();
                      assert(response.getResponseResult() ==
                             ResponseResult::kSuccess);
                      assert(response.getPayload() == "hello world");
                      assert(response.getErrorMessage().empty());

                      ++client2RpcRoundSuccess;
                      client_2.disconnect();
                    });
    } else {
      ++clientDisconnected;
      if (clientDisconnected == 2) {
        server.stop(std::chrono::milliseconds(0));
      }
    }
  });
  client_2.setConnectionErrorCallback([&](int) { std::abort(); });
  client_2.connect();

  loop.runAfter(std::chrono::milliseconds(3000), []() {
    std::cerr << "test timeout" << std::endl;
    std::abort();
  });

  loop.loop();

  auto waitForRes =
      stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(clientConnected == 2);
  assert(client1RpcRoundSuccess == 1);
  assert(client2RpcRoundSuccess == 1);
  assert(clientDisconnected == 2);
  assert(serverStopped == 1);
  assert(handlerInvoked.load() == 2);
  {
    std::lock_guard<std::mutex> lock(mutex);
    assert(threadIds.size() == 2);
  }
}

void rpc_client_allows_call_from_non_loop_thread_test() {
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

  std::promise<void> stopCompletePromise;
  auto stopCompleteFuture = stopCompletePromise.get_future();

  int clientConnected = 0;
  int handlerInvoked{0};
  int clientRpcRoundSuccess = 0;
  int clientDisconnected = 0;
  int serverStopped = 0;

  EventLoop loop;
  RpcServer server(&loop, serverFd);
  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      stopCompletePromise.set_value();
      loop.quit();
    });
  });
  bool registered = server.registerMethod(
      "EchoService", "Echo", [&](const std::string &payload) {
        ++handlerInvoked;
        return RpcServer::RpcResult{ResponseResult::kSuccess, payload, ""};
      });
  assert(registered);

  server.start();

  RpcClient client(&loop, serverAddr);

  EventLoopThread threadLoop;
  auto loopThread = threadLoop.startLoop();

  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++clientConnected;

      loopThread->queueInLoop([&]() {
        assert(!loop.isInLoopThread());
        client.call("EchoService", "Echo", "hello world",
                    [&](const RpcResponse &response) {
                      loop.assertInLoopThread();
                      assert(response.getResponseResult() ==
                             ResponseResult::kSuccess);
                      assert(response.getPayload() == "hello world");
                      assert(response.getErrorMessage().empty());

                      ++clientRpcRoundSuccess;
                      client.disconnect();
                    });
      });
    } else {
      ++clientDisconnected;
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
      stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(clientConnected == 1);
  assert(clientRpcRoundSuccess == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
  assert(handlerInvoked == 1);
}

void rpc_server_returns_error_and_remains_available_when_handler_throws_test() {
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

  std::promise<void> stopCompletePromise;
  auto stopCompleteFuture = stopCompletePromise.get_future();

  int clientConnected = 0;
  int handlerInvoked{0};
  int clientCallbackInvoked = 0;
  int clientRpcRoundSuccess = 0;
  int clientRpcRoundUnsuccess = 0;
  int clientDisconnected = 0;
  int serverStopped = 0;

  EventLoop loop;
  RpcServer server(&loop, serverFd);
  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      stopCompletePromise.set_value();
      loop.quit();
    });
  });
  bool registered = server.registerMethod(
      "EchoService", "Echo", [&](const std::string &payload) {
        ++handlerInvoked;
        if (handlerInvoked == 1) {
          throw std::runtime_error("run time error");
        }
        return RpcServer::RpcResult{ResponseResult::kSuccess, payload, ""};
      });
  assert(registered);

  server.start();

  RpcClient client(&loop, serverAddr);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      assert(loop.isInLoopThread());
      ++clientConnected;

      client.call(
          "EchoService", "Echo", "hello world",
          [&](const RpcResponse &response) {
            loop.assertInLoopThread();
            ++clientCallbackInvoked;

            assert(response.getResponseResult() == ResponseResult::kUnsuccess);
            assert(response.getPayload().empty());
            assert(response.getErrorMessage() == "handler exception");
            ++clientRpcRoundUnsuccess;

            client.call("EchoService", "Echo", "hello world",
                        [&](const RpcResponse &response) {
                          loop.assertInLoopThread();
                          ++clientCallbackInvoked;

                          assert(response.getResponseResult() ==
                                 ResponseResult::kSuccess);
                          assert(response.getPayload() == "hello world");
                          assert(response.getErrorMessage().empty());
                          ++clientRpcRoundSuccess;
                          client.disconnect();
                        });
          });
    } else {
      ++clientDisconnected;
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
      stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(clientConnected == 1);
  assert(clientRpcRoundSuccess == 1);
  assert(clientRpcRoundUnsuccess == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
  assert(handlerInvoked == 2);
  assert(clientCallbackInvoked == 2);
}

void rpc_client_ignores_late_response_after_request_timeout_test() {
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

  std::promise<void> stopCompletePromise;
  auto stopCompleteFuture = stopCompletePromise.get_future();

  int clientConnected = 0;
  int serverResponseSent = 0;
  int serverRequestReceived{0};
  int clientCallbackInvoked = 0;
  int clientRpcRoundSuccess = 0;
  int clientRpcRoundUnsuccess = 0;
  int clientDisconnected = 0;
  int serverStopped = 0;

  uint64_t timedOutRequestId = 0;

  EventLoop loop;
  TcpServer server(&loop, serverFd);
  LengthHeaderCodec codec;
  server.setMessageCallback(std::bind(&LengthHeaderCodec::onMessage, &codec,
                                      std::placeholders::_1,
                                      std::placeholders::_2));
  server.setPeerHalfCloseCallback(
      [](const TcpConnectionPtr &conn) { conn->shutdown(); });
  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      stopCompletePromise.set_value();
      loop.quit();
    });
  });

  codec.setMessageCallback([&](const TcpConnectionPtr &conn,
                               const std::string &msg) {
    RpcRequest request;
    std::string errorMessage;
    bool res = request.decode(msg, errorMessage);
    if (!res || !errorMessage.empty()) {
      conn->forceClose();
      return;
    }

    ++serverRequestReceived;

    if (serverRequestReceived == 1) {
      timedOutRequestId = request.getRequestId();
      return;
    }

    if (serverRequestReceived == 2) {
      assert(timedOutRequestId != 0);
      {
        RpcResponse response(timedOutRequestId, ResponseResult::kSuccess,
                             "response timeout", "");
        std::string outputMessage;
        std::string errorMessage;
        bool res = response.encode(outputMessage, errorMessage);
        if (!res || !errorMessage.empty()) {
          conn->forceClose();
          return;
        }

        if (conn->connected()) {
          ++serverResponseSent;
          codec.send(conn, outputMessage.data(), outputMessage.length());
        }
      }
      {
        RpcResponse response(request.getRequestId(), ResponseResult::kSuccess,
                             request.getPayload(), "");
        std::string outputMessage;
        std::string errorMessage;
        bool res = response.encode(outputMessage, errorMessage);
        if (!res || !errorMessage.empty()) {
          conn->forceClose();
          return;
        }

        if (conn->connected()) {
          ++serverResponseSent;
          codec.send(conn, outputMessage.data(), outputMessage.length());
        }
      }
    }
  });
  server.start();

  RpcClient client(&loop, serverAddr);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      assert(loop.isInLoopThread());
      ++clientConnected;

      client.call(
          "EchoService", "Echo", "hello world",
          [&](const RpcResponse &response) {
            loop.assertInLoopThread();
            ++clientCallbackInvoked;

            assert(response.getResponseResult() == ResponseResult::kUnsuccess);
            assert(response.getPayload().empty());
            assert(response.getErrorMessage() == "rpc request timeout");
            ++clientRpcRoundUnsuccess;

            client.call("EchoService", "Echo", "hello world",
                        [&](const RpcResponse &response) {
                          loop.assertInLoopThread();
                          ++clientCallbackInvoked;

                          assert(response.getResponseResult() ==
                                 ResponseResult::kSuccess);
                          assert(response.getPayload() == "hello world");
                          assert(response.getErrorMessage().empty());
                          ++clientRpcRoundSuccess;
                          client.disconnect();
                        });
          },
          std::chrono::milliseconds(100));
    } else {
      ++clientDisconnected;
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
      stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(clientConnected == 1);
  assert(clientRpcRoundSuccess == 1);
  assert(clientRpcRoundUnsuccess == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
  assert(serverRequestReceived == 2);
  assert(clientCallbackInvoked == 2);
  assert(serverResponseSent == 2);
}

void rpc_client_reconnects_after_connection_closes_and_allows_new_call_test() {
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

  std::promise<void> stopCompletePromise;
  auto stopCompleteFuture = stopCompletePromise.get_future();

  int clientConnected = 0;
  int serverResponseSent = 0;
  int serverRequestReceived{0};
  int clientCallbackInvoked = 0;
  int clientRpcRoundSuccess = 0;
  int clientRpcRoundUnsuccess = 0;
  int clientDisconnected = 0;
  int serverStopped = 0;

  EventLoop loop;
  TcpServer server(&loop, serverFd);
  LengthHeaderCodec codec;
  server.setMessageCallback(std::bind(&LengthHeaderCodec::onMessage, &codec,
                                      std::placeholders::_1,
                                      std::placeholders::_2));
  server.setPeerHalfCloseCallback(
      [](const TcpConnectionPtr &conn) { conn->shutdown(); });
  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      stopCompletePromise.set_value();
      loop.quit();
    });
  });

  codec.setMessageCallback(
      [&](const TcpConnectionPtr &conn, const std::string &msg) {
        RpcRequest request;
        std::string errorMessage;
        bool res = request.decode(msg, errorMessage);
        if (!res || !errorMessage.empty()) {
          conn->forceClose();
          return;
        }

        ++serverRequestReceived;

        if (serverRequestReceived == 1) {
          conn->forceClose();
          return;
        }

        {
          RpcResponse response(request.getRequestId(), ResponseResult::kSuccess,
                               request.getPayload(), "");
          std::string outputMessage;
          std::string errorMessage;
          bool res = response.encode(outputMessage, errorMessage);
          if (!res || !errorMessage.empty()) {
            conn->forceClose();
            return;
          }

          if (conn->connected()) {
            ++serverResponseSent;
            codec.send(conn, outputMessage.data(), outputMessage.length());
          }
        }
      });
  server.start();

  RpcClient client(&loop, serverAddr);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      assert(loop.isInLoopThread());
      ++clientConnected;

      if (clientConnected == 1) {
        client.call("EchoService", "Echo", "hello world",
                    [&](const RpcResponse &response) {
                      loop.assertInLoopThread();
                      ++clientCallbackInvoked;

                      assert(response.getResponseResult() ==
                             ResponseResult::kUnsuccess);
                      assert(response.getPayload().empty());
                      assert(response.getErrorMessage() == "connect closed");
                      ++clientRpcRoundUnsuccess;
                    });
      }

      if (clientConnected == 2) {
        client.call("EchoService", "Echo", "hello world",
                    [&](const RpcResponse &response) {
                      loop.assertInLoopThread();
                      ++clientCallbackInvoked;

                      assert(response.getResponseResult() ==
                             ResponseResult::kSuccess);
                      assert(response.getPayload() == "hello world");
                      assert(response.getErrorMessage().empty());
                      ++clientRpcRoundSuccess;
                      client.disconnect();
                    });
      }
    } else {
      ++clientDisconnected;

      if (clientDisconnected == 2) {
        server.stop(std::chrono::milliseconds(0));
      }
    }
  });
  client.setConnectionErrorCallback([&](int) { std::abort(); });
  client.enableRetry();
  client.connect();

  loop.runAfter(std::chrono::milliseconds(3000), []() {
    std::cerr << "test timeout" << std::endl;
    std::abort();
  });

  loop.loop();

  auto waitForRes =
      stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(clientConnected == 2);
  assert(clientRpcRoundSuccess == 1);
  assert(clientRpcRoundUnsuccess == 1);
  assert(clientDisconnected == 2);
  assert(serverStopped == 1);
  assert(serverRequestReceived == 2);
  assert(clientCallbackInvoked == 2);
  assert(serverResponseSent == 1);
}

void rpc_client_reconnects_after_initial_connection_failure_test() {
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

  std::promise<void> stopCompletePromise;
  auto stopCompleteFuture = stopCompletePromise.get_future();

  int clientConnected = 0;
  int handlerInvoked = 0;
  int clientConnectErrorCnt = 0;
  int clientCallbackInvoked = 0;
  int clientRpcRoundSuccess = 0;
  int clientDisconnected = 0;
  int serverStopped = 0;

  EventLoop loop;
  RpcServer server(&loop, serverFd);
  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      stopCompletePromise.set_value();
      loop.quit();
    });
  });
  bool registered = server.registerMethod(
      "EchoService", "Echo", [&](const std::string &payload) {
        ++handlerInvoked;
        return RpcServer::RpcResult{ResponseResult::kSuccess, payload, ""};
      });
  assert(registered);

  RpcClient client(&loop, serverAddr);
  client.enableRetry();
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      assert(loop.isInLoopThread());
      ++clientConnected;

      client.call("EchoService", "Echo", "hello world",
                  [&](const RpcResponse &response) {
                    loop.assertInLoopThread();
                    ++clientCallbackInvoked;

                    assert(response.getResponseResult() ==
                           ResponseResult::kSuccess);
                    assert(response.getPayload() == "hello world");
                    assert(response.getErrorMessage().empty());
                    ++clientRpcRoundSuccess;
                    client.disconnect();
                  });
    } else {
      ++clientDisconnected;
      server.stop(std::chrono::milliseconds(0));
    }
  });
  client.setConnectionErrorCallback([&](int) {
    ++clientConnectErrorCnt;
    server.start();
  });
  client.connect();

  loop.runAfter(std::chrono::milliseconds(3000), []() {
    std::cerr << "test timeout" << std::endl;
    std::abort();
  });

  loop.loop();

  auto waitForRes =
      stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(clientConnected == 1);
  assert(clientRpcRoundSuccess == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
  assert(handlerInvoked == 1);
  assert(clientCallbackInvoked == 1);
  assert(clientConnectErrorCnt == 1);
}

void rpc_client_disabling_retry_prevents_reconnection_test() {
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

  std::promise<void> stopCompletePromise;
  auto stopCompleteFuture = stopCompletePromise.get_future();

  int clientConnected = 0;
  int clientConnectErrorCnt = 0;
  int clientDisconnected = 0;
  int serverStopped = 0;

  EventLoop loop;
  RpcServer server(&loop, serverFd);
  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      stopCompletePromise.set_value();
      loop.quit();
    });
  });

  RpcClient client(&loop, serverAddr);
  client.enableRetry();
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      assert(loop.isInLoopThread());
      ++clientConnected;
    } else {
      ++clientDisconnected;
    }
  });
  client.setConnectionErrorCallback([&](int) {
    ++clientConnectErrorCnt;
    client.disableRetry();
    server.start();

    loop.runAfter(std::chrono::milliseconds(200),
                  [&]() { server.stop(std::chrono::milliseconds(0)); });
  });
  client.connect();

  loop.runAfter(std::chrono::milliseconds(3000), []() {
    std::cerr << "test timeout" << std::endl;
    std::abort();
  });

  loop.loop();

  auto waitForRes =
      stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(clientConnected == 0);
  assert(clientDisconnected == 0);
  assert(serverStopped == 1);
  assert(clientConnectErrorCnt == 1);
}

void rpc_client_rejects_call_when_max_pending_requests_is_reached_test() {
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

  std::promise<void> stopCompletePromise;
  auto stopCompleteFuture = stopCompletePromise.get_future();

  int connected = 0;
  int pendingRequestClosed = 0;
  int overLimitRequestRejected = 0;
  int clientDisconnected = 0;
  int serverRequestReceived = 0;
  int serverStopped = 0;

  EventLoop loop;
  LengthHeaderCodec codec;
  TcpServer server(&loop, serverFd);

  codec.setMessageCallback(
      [&](const TcpConnectionPtr &conn, const std::string &) {
        ++serverRequestReceived;
        return;
      });

  server.setMessageCallback(std::bind(&LengthHeaderCodec::onMessage, &codec,
                                      std::placeholders::_1,
                                      std::placeholders::_2));

  server.setPeerHalfCloseCallback(
      [](const TcpConnectionPtr &conn) { conn->shutdown(); });

  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      stopCompletePromise.set_value();
      loop.quit();
    });
  });
  server.start();

  RpcClient client(&loop, serverAddr);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;
      client.call("EchoService", "Echo", "hello world",
                  [&](const RpcResponse &response) {
                    assert(response.getResponseResult() ==
                           ResponseResult::kUnsuccess);
                    assert(response.getPayload().empty());
                    assert(response.getErrorMessage() == "connect closed");

                    ++pendingRequestClosed;
                  });
      client.call(
          "EchoService", "Echo", "hello world",
          [&](const RpcResponse &response) {
            assert(response.getResponseResult() == ResponseResult::kUnsuccess);
            assert(response.getPayload().empty());
            assert(response.getErrorMessage() == "too many pending requests");

            ++overLimitRequestRejected;
            client.disconnect();
          });
    } else {
      ++clientDisconnected;
      server.stop(std::chrono::milliseconds(0));
    }
  });
  client.setConnectionErrorCallback([&](int) { std::abort(); });
  client.setMaxPendingRequests(1);
  client.connect();

  loop.runAfter(std::chrono::milliseconds(3000), []() {
    std::cerr << "test timeout" << std::endl;
    std::abort();
  });

  loop.loop();

  auto waitForRes =
      stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(serverRequestReceived == 1);
  assert(pendingRequestClosed == 1);
  assert(overLimitRequestRejected == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
}

void rpc_client_releases_pending_capacity_before_response_callback_test() {
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

  std::promise<void> stopCompletePromise;
  auto stopCompleteFuture = stopCompletePromise.get_future();

  int connected = 0;
  int firstRequestCnt = 0;
  int secondRequestCnt = 0;
  int clientDisconnected = 0;
  int serverRequestReceived = 0;
  int serverStopped = 0;

  EventLoop loop;
  LengthHeaderCodec codec;
  TcpServer server(&loop, serverFd);

  codec.setMessageCallback(
      [&](const TcpConnectionPtr &conn, const std::string &msg) {
        ++serverRequestReceived;

        RpcRequest request;
        std::string errorMessage;
        bool res = request.decode(msg, errorMessage);
        if (!res || !errorMessage.empty()) {
          conn->forceClose();
          return;
        }

        {
          RpcResponse response(request.getRequestId(), ResponseResult::kSuccess,
                               request.getPayload(), "");
          std::string outputMessage;
          std::string errorMessage;
          bool res = response.encode(outputMessage, errorMessage);
          if (!res || !errorMessage.empty()) {
            conn->forceClose();
            return;
          }

          if (conn->connected()) {
            codec.send(conn, outputMessage.data(), outputMessage.length());
          }
        }

        return;
      });

  server.setMessageCallback(std::bind(&LengthHeaderCodec::onMessage, &codec,
                                      std::placeholders::_1,
                                      std::placeholders::_2));

  server.setPeerHalfCloseCallback(
      [](const TcpConnectionPtr &conn) { conn->shutdown(); });

  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      stopCompletePromise.set_value();
      loop.quit();
    });
  });
  server.start();

  RpcClient client(&loop, serverAddr);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;
      client.call(
          "EchoService", "Echo", "hello world",
          [&](const RpcResponse &response) {
            assert(response.getResponseResult() == ResponseResult::kSuccess);
            assert(response.getPayload() == "hello world");
            assert(response.getErrorMessage().empty());

            ++firstRequestCnt;

            client.call("EchoService", "Echo", "hello world",
                        [&](const RpcResponse &response) {
                          assert(response.getResponseResult() ==
                                 ResponseResult::kSuccess);
                          assert(response.getPayload() == "hello world");
                          assert(response.getErrorMessage().empty());

                          ++secondRequestCnt;
                          client.disconnect();
                        });
          });
    } else {
      ++clientDisconnected;
      server.stop(std::chrono::milliseconds(0));
    }
  });
  client.setConnectionErrorCallback([&](int) { std::abort(); });
  client.setMaxPendingRequests(1);
  client.connect();

  loop.runAfter(std::chrono::milliseconds(3000), []() {
    std::cerr << "test timeout" << std::endl;
    std::abort();
  });

  loop.loop();

  auto waitForRes =
      stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(serverRequestReceived == 2);
  assert(firstRequestCnt == 1);
  assert(secondRequestCnt == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
}

void rpc_client_releases_pending_capacity_after_request_timeout_test() {
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

  std::promise<void> stopCompletePromise;
  auto stopCompleteFuture = stopCompletePromise.get_future();

  int connected = 0;
  int timeoutCallbackCount = 0;
  int successCallbackCount = 0;
  int clientDisconnected = 0;
  int serverRequestReceived = 0;
  int serverStopped = 0;

  EventLoop loop;
  LengthHeaderCodec codec;
  TcpServer server(&loop, serverFd);

  codec.setMessageCallback(
      [&](const TcpConnectionPtr &conn, const std::string &msg) {
        ++serverRequestReceived;

        if (serverRequestReceived == 1) {
          return;
        }

        RpcRequest request;
        std::string errorMessage;
        bool res = request.decode(msg, errorMessage);
        if (!res || !errorMessage.empty()) {
          conn->forceClose();
          return;
        }

        {
          RpcResponse response(request.getRequestId(), ResponseResult::kSuccess,
                               request.getPayload(), "");
          std::string outputMessage;
          std::string errorMessage;
          bool res = response.encode(outputMessage, errorMessage);
          if (!res || !errorMessage.empty()) {
            conn->forceClose();
            return;
          }

          if (conn->connected()) {
            codec.send(conn, outputMessage.data(), outputMessage.length());
          }
        }

        return;
      });

  server.setMessageCallback(std::bind(&LengthHeaderCodec::onMessage, &codec,
                                      std::placeholders::_1,
                                      std::placeholders::_2));

  server.setPeerHalfCloseCallback(
      [](const TcpConnectionPtr &conn) { conn->shutdown(); });

  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      stopCompletePromise.set_value();
      loop.quit();
    });
  });
  server.start();

  RpcClient client(&loop, serverAddr);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;
      client.call(
          "EchoService", "Echo", "hello world",
          [&](const RpcResponse &response) {
            assert(response.getResponseResult() == ResponseResult::kUnsuccess);
            assert(response.getPayload().empty());
            assert(response.getErrorMessage() == "rpc request timeout");

            ++timeoutCallbackCount;

            client.call("EchoService", "Echo", "hello world",
                        [&](const RpcResponse &response) {
                          assert(response.getResponseResult() ==
                                 ResponseResult::kSuccess);
                          assert(response.getPayload() == "hello world");
                          assert(response.getErrorMessage().empty());

                          ++successCallbackCount;
                          client.disconnect();
                        });
          },
          std::chrono::milliseconds(100));
    } else {
      ++clientDisconnected;
      server.stop(std::chrono::milliseconds(0));
    }
  });
  client.setConnectionErrorCallback([&](int) { std::abort(); });
  client.setMaxPendingRequests(1);
  client.connect();

  loop.runAfter(std::chrono::milliseconds(3000), []() {
    std::cerr << "test timeout" << std::endl;
    std::abort();
  });

  loop.loop();

  auto waitForRes =
      stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(serverRequestReceived == 2);
  assert(timeoutCallbackCount == 1);
  assert(successCallbackCount == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
}

void rpc_client_uses_default_timeout_and_allows_explicit_zero_override_test() {
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

  std::promise<void> stopCompletePromise;
  auto stopCompleteFuture = stopCompletePromise.get_future();

  int connected = 0;
  int timeoutCallbackCount = 0;
  int successCallbackCount = 0;
  int clientDisconnected = 0;
  int serverRequestReceived = 0;
  int serverStopped = 0;

  EventLoop loop;
  LengthHeaderCodec codec;
  TcpServer server(&loop, serverFd);

  codec.setMessageCallback(
      [&](const TcpConnectionPtr &conn, const std::string &msg) {
        ++serverRequestReceived;

        if (serverRequestReceived == 1) {
          return;
        }

        RpcRequest request;
        std::string errorMessage;
        bool res = request.decode(msg, errorMessage);
        if (!res || !errorMessage.empty()) {
          conn->forceClose();
          return;
        }

        {
          RpcResponse response(request.getRequestId(), ResponseResult::kSuccess,
                               request.getPayload(), "");
          std::string outputMessage;
          std::string errorMessage;
          bool res = response.encode(outputMessage, errorMessage);
          if (!res || !errorMessage.empty()) {
            conn->forceClose();
            return;
          }

          loop.runAfter(std::chrono::milliseconds(200),
                        [&codec, conn, msg = std::move(outputMessage)]() {
                          if (conn->connected()) {
                            codec.send(conn, msg.data(), msg.length());
                          }
                        });
        }

        return;
      });

  server.setMessageCallback(std::bind(&LengthHeaderCodec::onMessage, &codec,
                                      std::placeholders::_1,
                                      std::placeholders::_2));

  server.setPeerHalfCloseCallback(
      [](const TcpConnectionPtr &conn) { conn->shutdown(); });

  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      stopCompletePromise.set_value();
      loop.quit();
    });
  });
  server.start();

  RpcClient client(&loop, serverAddr);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;
      client.call("EchoService", "Echo", "hello world",
                  [&](const RpcResponse &response) {
                    assert(response.getResponseResult() ==
                           ResponseResult::kUnsuccess);
                    assert(response.getPayload().empty());
                    assert(response.getErrorMessage() == "rpc request timeout");

                    ++timeoutCallbackCount;

                    client.call(
                        "EchoService", "Echo", "hello world",
                        [&](const RpcResponse &response) {
                          assert(response.getResponseResult() ==
                                 ResponseResult::kSuccess);
                          assert(response.getPayload() == "hello world");
                          assert(response.getErrorMessage().empty());

                          ++successCallbackCount;
                          client.disconnect();
                        },
                        std::chrono::milliseconds(0));
                  });
    } else {
      ++clientDisconnected;
      server.stop(std::chrono::milliseconds(0));
    }
  });
  client.setConnectionErrorCallback([&](int) { std::abort(); });
  client.setDefaultTimeout(std::chrono::milliseconds(100));
  client.connect();

  loop.runAfter(std::chrono::milliseconds(3000), []() {
    std::cerr << "test timeout" << std::endl;
    std::abort();
  });

  loop.loop();

  auto waitForRes =
      stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(serverRequestReceived == 2);
  assert(timeoutCallbackCount == 1);
  assert(successCallbackCount == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
}

void rpc_client_ignores_late_response_after_request_cancellation_test() {
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

  std::promise<void> stopCompletePromise;
  auto stopCompleteFuture = stopCompletePromise.get_future();

  uint64_t serverFirstRequestId = 0;

  int connected = 0;
  int cancelCallbackCount = 0;
  int successCallbackCount = 0;
  int clientDisconnected = 0;
  int serverRequestReceived = 0;
  int serverStopped = 0;

  EventLoop loop;
  LengthHeaderCodec codec;
  TcpServer server(&loop, serverFd);

  codec.setMessageCallback(
      [&](const TcpConnectionPtr &conn, const std::string &msg) {
        ++serverRequestReceived;

        RpcRequest request;
        std::string errorMessage;
        bool res = request.decode(msg, errorMessage);
        if (!res || !errorMessage.empty()) {
          conn->forceClose();
          return;
        }

        if (serverRequestReceived == 1) {
          serverFirstRequestId = request.getRequestId();
          return;
        }

        {
          RpcResponse response(serverFirstRequestId, ResponseResult::kSuccess,
                               request.getPayload(), "");
          std::string outputMessage;
          std::string errorMessage;
          bool res = response.encode(outputMessage, errorMessage);
          if (!res || !errorMessage.empty()) {
            conn->forceClose();
            return;
          }

          if (conn->connected()) {
            codec.send(conn, outputMessage.data(), outputMessage.length());
          }
        }

        {
          RpcResponse response(request.getRequestId(), ResponseResult::kSuccess,
                               request.getPayload(), "");
          std::string outputMessage;
          std::string errorMessage;
          bool res = response.encode(outputMessage, errorMessage);
          if (!res || !errorMessage.empty()) {
            conn->forceClose();
            return;
          }

          if (conn->connected()) {
            codec.send(conn, outputMessage.data(), outputMessage.length());
          }
        }

        return;
      });

  server.setMessageCallback(std::bind(&LengthHeaderCodec::onMessage, &codec,
                                      std::placeholders::_1,
                                      std::placeholders::_2));

  server.setPeerHalfCloseCallback(
      [](const TcpConnectionPtr &conn) { conn->shutdown(); });

  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      stopCompletePromise.set_value();
      loop.quit();
    });
  });
  server.start();

  RpcClient client(&loop, serverAddr);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;
      uint64_t canceledRequestId = client.call(
          "EchoService", "Echo", "hello world",
          [&](const RpcResponse &response) {
            assert(response.getResponseResult() == ResponseResult::kUnsuccess);
            assert(response.getPayload().empty());
            assert(response.getErrorMessage() == "rpc request canceled");

            ++cancelCallbackCount;

            client.call(
                "EchoService", "Echo", "hello world",
                [&](const RpcResponse &response) {
                  assert(response.getResponseResult() ==
                         ResponseResult::kSuccess);
                  assert(response.getPayload() == "hello world");
                  assert(response.getErrorMessage().empty());

                  ++successCallbackCount;
                  client.disconnect();
                },
                std::chrono::milliseconds(0));
          },
          std::chrono::milliseconds(0));

      client.cancel(canceledRequestId);
    } else {
      ++clientDisconnected;
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
      stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(serverRequestReceived == 2);
  assert(cancelCallbackCount == 1);
  assert(successCallbackCount == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
}

void rpc_server_returns_error_for_unregistered_method_test() {
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

  std::promise<void> stopCompletePromise;
  auto stopCompleteFuture = stopCompletePromise.get_future();

  int connected = 0;
  std::atomic<int> handlerInvoked = 0;
  int errorResponseReceived = 0;
  int clientDisconnected = 0;
  int serverStopped = 0;

  EventLoop loop;
  RpcServer server(&loop, serverFd, 1);
  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      stopCompletePromise.set_value();
      loop.quit();
    });
  });
  bool registered = server.registerMethod(
      "EchoService", "Echo", [&](const std::string &payload) {
        assert(!loop.isInLoopThread());
        ++handlerInvoked;
        return RpcServer::RpcResult{ResponseResult::kSuccess, payload, ""};
      });
  assert(registered);

  bool unregistered = server.unregisterMethod("EchoService", "Echo");
  assert(unregistered);

  server.start();

  RpcClient client(&loop, serverAddr);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;
      client.call(
          "EchoService", "Echo", "hello world",
          [&](const RpcResponse &response) {
            loop.assertInLoopThread();
            assert(response.getResponseResult() == ResponseResult::kUnsuccess);
            assert(response.getPayload().empty());
            assert(response.getErrorMessage() == "service or method not exist");

            ++errorResponseReceived;
            client.disconnect();
          });
    } else {
      ++clientDisconnected;
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
      stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(handlerInvoked.load() == 0);
  assert(serverStopped == 1);
  assert(clientDisconnected == 1);
  assert(errorResponseReceived == 1);
}

void rpc_server_keeps_other_methods_after_unregistering_one_method_test() {
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

  std::promise<void> stopCompletePromise;
  auto stopCompleteFuture = stopCompletePromise.get_future();

  int connected = 0;
  std::atomic<int> echoHandlerInvoked = 0;
  std::atomic<int> unregisteredHandlerInvoked = 0;
  int errorResponseReceived = 0;
  int successResponseReceived = 0;
  int clientDisconnected = 0;
  int serverStopped = 0;

  EventLoop loop;
  RpcServer server(&loop, serverFd, 1);
  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      stopCompletePromise.set_value();
      loop.quit();
    });
  });
  bool registered = server.registerMethod(
      "EchoService", "Echo", [&](const std::string &payload) {
        assert(!loop.isInLoopThread());
        ++echoHandlerInvoked;
        return RpcServer::RpcResult{ResponseResult::kSuccess, payload, ""};
      });
  assert(registered);

  registered = server.registerMethod(
      "EchoService", "Unregist later", [&](const std::string &payload) {
        assert(!loop.isInLoopThread());
        ++unregisteredHandlerInvoked;
        return RpcServer::RpcResult{ResponseResult::kSuccess, payload, ""};
      });
  assert(registered);

  bool unregistered = server.unregisterMethod("EchoService", "Unregist later");
  assert(unregistered);

  server.start();

  RpcClient client(&loop, serverAddr);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;
      client.call(
          "EchoService", "Unregist later", "hello world",
          [&](const RpcResponse &response) {
            loop.assertInLoopThread();
            assert(response.getResponseResult() == ResponseResult::kUnsuccess);
            assert(response.getPayload().empty());
            assert(response.getErrorMessage() == "service or method not exist");

            ++errorResponseReceived;

            client.call("EchoService", "Echo", "hello world",
                        [&](const RpcResponse &response) {
                          loop.assertInLoopThread();
                          assert(response.getResponseResult() ==
                                 ResponseResult::kSuccess);
                          assert(response.getPayload() == "hello world");
                          assert(response.getErrorMessage().empty());

                          ++successResponseReceived;
                          client.disconnect();
                        });
          });
    } else {
      ++clientDisconnected;
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
      stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(echoHandlerInvoked.load() == 1);
  assert(unregisteredHandlerInvoked.load() == 0);
  assert(serverStopped == 1);
  assert(clientDisconnected == 1);
  assert(errorResponseReceived == 1);
  assert(successResponseReceived == 1);
}

void rpc_client_allows_request_cancellation_from_non_loop_thread_test() {
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

  std::promise<void> stopCompletePromise;
  auto stopCompleteFuture = stopCompletePromise.get_future();

  int connected = 0;
  std::atomic<int> handlerInvoked = 0;
  int errorResponseReceived = 0;
  int successResponseReceived = 0;
  int clientDisconnected = 0;
  int serverStopped = 0;

  EventLoop loop;
  RpcServer server(&loop, serverFd, 1);
  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      stopCompletePromise.set_value();
      loop.quit();
    });
  });
  bool registered = server.registerMethod(
      "EchoService", "Echo", [&](const std::string &payload) {
        assert(!loop.isInLoopThread());
        ++handlerInvoked;
        return RpcServer::RpcResult{ResponseResult::kSuccess, payload, ""};
      });
  assert(registered);

  server.start();

  RpcClient client(&loop, serverAddr);

  EventLoopThread loopThread;
  auto threadLoop = loopThread.startLoop();

  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;

      threadLoop->queueInLoop([&]() {
        uint64_t requestId = client.call(
            "EchoService", "Echo", "hello world",
            [&](const RpcResponse &response) {
              loop.assertInLoopThread();
              assert(response.getResponseResult() ==
                     ResponseResult::kUnsuccess);
              assert(response.getPayload().empty());
              assert(response.getErrorMessage() == "rpc request canceled");

              ++errorResponseReceived;
            });
        client.cancel(requestId);

        client.call("EchoService", "Echo", "hello world",
                    [&](const RpcResponse &response) {
                      loop.assertInLoopThread();
                      assert(response.getResponseResult() ==
                             ResponseResult::kSuccess);
                      assert(response.getPayload() == "hello world");
                      assert(response.getErrorMessage().empty());

                      ++successResponseReceived;
                      client.disconnect();
                    });
      });
    } else {
      ++clientDisconnected;
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
      stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(handlerInvoked.load() == 2);
  assert(serverStopped == 1);
  assert(clientDisconnected == 1);
  assert(errorResponseReceived == 1);
  assert(successResponseReceived == 1);
}

void rpc_client_releases_pending_capacity_after_request_cancellation_test() {
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

  std::promise<void> stopCompletePromise;
  auto stopCompleteFuture = stopCompletePromise.get_future();

  int connected = 0;
  int cancelCallbackCount = 0;
  int tooManyPendingCallbackCount = 0;
  int successCallbackCount = 0;
  int clientDisconnected = 0;
  int serverRequestReceived = 0;
  int serverStopped = 0;

  EventLoop loop;
  LengthHeaderCodec codec;
  TcpServer server(&loop, serverFd);

  codec.setMessageCallback(
      [&](const TcpConnectionPtr &conn, const std::string &msg) {
        ++serverRequestReceived;

        RpcRequest request;
        std::string errorMessage;
        bool res = request.decode(msg, errorMessage);
        if (!res || !errorMessage.empty()) {
          conn->forceClose();
          return;
        }

        if (serverRequestReceived == 1) {
          return;
        }

        {
          RpcResponse response(request.getRequestId(), ResponseResult::kSuccess,
                               request.getPayload(), "");
          std::string outputMessage;
          std::string errorMessage;
          bool res = response.encode(outputMessage, errorMessage);
          if (!res || !errorMessage.empty()) {
            conn->forceClose();
            return;
          }

          if (conn->connected()) {
            codec.send(conn, outputMessage.data(), outputMessage.length());
          }
        }

        return;
      });

  server.setMessageCallback(std::bind(&LengthHeaderCodec::onMessage, &codec,
                                      std::placeholders::_1,
                                      std::placeholders::_2));

  server.setPeerHalfCloseCallback(
      [](const TcpConnectionPtr &conn) { conn->shutdown(); });

  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      stopCompletePromise.set_value();
      loop.quit();
    });
  });
  server.start();

  RpcClient client(&loop, serverAddr);
  client.setMaxPendingRequests(1);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;
      uint64_t canceledRequestId = client.call(
          "EchoService", "Echo", "hello world",
          [&](const RpcResponse &response) {
            assert(response.getResponseResult() == ResponseResult::kUnsuccess);
            assert(response.getPayload().empty());
            assert(response.getErrorMessage() == "rpc request canceled");

            ++cancelCallbackCount;
          },
          std::chrono::milliseconds(0));

      client.call(
          "EchoService", "Echo", "hello world",
          [&](const RpcResponse &response) {
            assert(response.getResponseResult() == ResponseResult::kUnsuccess);
            assert(response.getPayload().empty());
            assert(response.getErrorMessage() == "too many pending requests");
            ++tooManyPendingCallbackCount;

            client.cancel(canceledRequestId);

            client.call(
                "EchoService", "Echo", "hello world",
                [&](const RpcResponse &response) {
                  assert(response.getResponseResult() ==
                         ResponseResult::kSuccess);
                  assert(response.getPayload() == "hello world");
                  assert(response.getErrorMessage().empty());
                  ++successCallbackCount;

                  client.disconnect();
                },
                std::chrono::milliseconds(0));
          },
          std::chrono::milliseconds(0));

    } else {
      ++clientDisconnected;
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
      stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(serverRequestReceived == 2);
  assert(cancelCallbackCount == 1);
  assert(tooManyPendingCallbackCount == 1);
  assert(successCallbackCount == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
}

void rpc_client_allows_calls_from_multiple_non_loop_threads_test() {
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

  std::promise<void> stopCompletePromise;
  auto stopCompleteFuture = stopCompletePromise.get_future();

  int connected = 0;
  int successCallbackCount = 0;
  int clientDisconnected = 0;
  int serverRequestReceived = 0;
  int serverStopped = 0;

  std::vector<RpcRequest> serverReceiveRequests;

  EventLoop loop;
  LengthHeaderCodec codec;
  TcpServer server(&loop, serverFd);

  codec.setMessageCallback(
      [&](const TcpConnectionPtr &conn, const std::string &msg) {
        ++serverRequestReceived;

        RpcRequest request;
        std::string errorMessage;
        bool res = request.decode(msg, errorMessage);
        if (!res || !errorMessage.empty()) {
          conn->forceClose();
          return;
        }

        serverReceiveRequests.push_back(request);
        if (serverRequestReceived != 2) {
          return;
        }

        for (int i = serverReceiveRequests.size() - 1; i >= 0; --i) {
          RpcResponse response(serverReceiveRequests[i].getRequestId(),
                               ResponseResult::kSuccess,
                               serverReceiveRequests[i].getPayload(), "");
          std::string outputMessage;
          std::string errorMessage;
          bool res = response.encode(outputMessage, errorMessage);
          if (!res || !errorMessage.empty()) {
            conn->forceClose();
            return;
          }

          if (conn->connected()) {
            codec.send(conn, outputMessage.data(), outputMessage.length());
          }
        }

        return;
      });

  server.setMessageCallback(std::bind(&LengthHeaderCodec::onMessage, &codec,
                                      std::placeholders::_1,
                                      std::placeholders::_2));

  server.setPeerHalfCloseCallback(
      [](const TcpConnectionPtr &conn) { conn->shutdown(); });

  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      stopCompletePromise.set_value();
      loop.quit();
    });
  });
  server.start();

  EventLoopThread eventLoopThread1;
  EventLoopThread eventLoopThread2;

  auto threadLoop1 = eventLoopThread1.startLoop();
  auto threadLoop2 = eventLoopThread2.startLoop();

  RpcClient client(&loop, serverAddr);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;

      threadLoop1->queueInLoop([&]() {
        client.call(
            "EchoService", "Echo", "request from thread 1",
            [&](const RpcResponse &response) {
              loop.assertInLoopThread();
              assert(response.getResponseResult() == ResponseResult::kSuccess);
              assert(response.getPayload() == "request from thread 1");
              assert(response.getErrorMessage().empty());

              ++successCallbackCount;

              if (successCallbackCount == 2) {
                client.disconnect();
              }
            },
            std::chrono::milliseconds(0));
      });

      threadLoop2->queueInLoop([&]() {
        client.call(
            "EchoService", "Echo", "request from thread 2",
            [&](const RpcResponse &response) {
              loop.assertInLoopThread();
              assert(response.getResponseResult() == ResponseResult::kSuccess);
              assert(response.getPayload() == "request from thread 2");
              assert(response.getErrorMessage().empty());

              ++successCallbackCount;

              if (successCallbackCount == 2) {
                client.disconnect();
              }
            },
            std::chrono::milliseconds(0));
      });

    } else {
      ++clientDisconnected;
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
      stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(serverRequestReceived == 2);
  assert(successCallbackCount == 2);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
}

void rpc_server_closes_connection_for_malformed_request_test() {
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

  std::promise<void> stopCompletePromise;
  auto stopCompleteFuture = stopCompletePromise.get_future();

  int connected = 0;
  int clientPeerHalfClosed = 0;
  int clientDisconnected = 0;
  int serverStopped = 0;

  EventLoop loop;
  RpcServer server(&loop, serverFd, 1);
  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      stopCompletePromise.set_value();
      loop.quit();
    });
  });
  server.start();

  TcpClient client(&loop, serverAddr);
  LengthHeaderCodec codec;
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;

      std::string output('a', 12);
      codec.send(conn, output.data(), output.size());
    } else {
      ++clientDisconnected;
      server.stop(std::chrono::milliseconds(0));
    }
  });
  client.setPeerHalfCloseCallback([&](const TcpConnectionPtr &conn) {
    ++clientPeerHalfClosed;
    conn->shutdown();
  });
  client.setConnectionErrorCallback([&](int) { std::abort(); });
  client.connect();

  loop.runAfter(std::chrono::milliseconds(3000), []() {
    std::cerr << "test timeout" << std::endl;
    std::abort();
  });

  loop.loop();

  auto waitForRes =
      stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(serverStopped == 1);
  assert(clientPeerHalfClosed == 1);
  assert(clientDisconnected == 1);
}

void rpc_client_closes_connection_for_malformed_response_test() {
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

  std::promise<void> stopCompletePromise;
  auto stopCompleteFuture = stopCompletePromise.get_future();

  int connected = 0;
  int failCallbackCount = 0;
  int clientDisconnected = 0;
  int serverRequestReceived = 0;
  int serverStopped = 0;

  EventLoop loop;
  LengthHeaderCodec codec;
  TcpServer server(&loop, serverFd);

  codec.setMessageCallback(
      [&](const TcpConnectionPtr &conn, const std::string &msg) {
        ++serverRequestReceived;

        RpcRequest request;
        std::string errorMessage;
        bool res = request.decode(msg, errorMessage);
        if (!res || !errorMessage.empty()) {
          conn->forceClose();
          return;
        }

        if (conn->connected()) {
          std::string outputMessage('a', 12);
          codec.send(conn, outputMessage.data(), outputMessage.length());
        }
      });

  server.setMessageCallback(std::bind(&LengthHeaderCodec::onMessage, &codec,
                                      std::placeholders::_1,
                                      std::placeholders::_2));

  server.setPeerHalfCloseCallback(
      [](const TcpConnectionPtr &conn) { conn->shutdown(); });

  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      stopCompletePromise.set_value();
      loop.quit();
    });
  });
  server.start();

  RpcClient client(&loop, serverAddr);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;

      client.call(
          "EchoService", "Echo", "request from thread 1",
          [&](const RpcResponse &response) {
            loop.assertInLoopThread();
            assert(response.getResponseResult() == ResponseResult::kUnsuccess);
            assert(response.getPayload().empty());
            assert(response.getErrorMessage() == "connect closed");

            ++failCallbackCount;
          },
          std::chrono::milliseconds(0));
    } else {
      ++clientDisconnected;
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
      stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(serverRequestReceived == 1);
  assert(failCallbackCount == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
}

void rpc_server_sends_delayed_response_from_async_handler_test() {
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

  std::promise<void> stopCompletePromise;
  auto stopCompleteFuture = stopCompletePromise.get_future();

  int connected = 0;
  int rpcRoundSuccess = 0;
  int clientDisconnected = 0;
  bool delayedReplyExecuted = false;
  int serverStopped = 0;

  EventLoop loop;
  RpcServer server(&loop, serverFd);
  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      stopCompletePromise.set_value();
      loop.quit();
    });
  });
  bool registered = server.registerAsyncMethod(
      "EchoService", "Echo",
      [&](const std::string &payload, RpcServer::RpcReply reply) {
        loop.runAfter(std::chrono::milliseconds(500),
                      [&delayedReplyExecuted, payload,
                       reply = std::move(reply)]() mutable {
                        delayedReplyExecuted = true;
                        reply(RpcServer::RpcResult{ResponseResult::kSuccess,
                                                   payload, ""});
                      });
      });
  assert(registered);

  server.start();

  RpcClient client(&loop, serverAddr);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;
      client.call("EchoService", "Echo", "hello world",
                  [&](const RpcResponse &response) {
                    loop.assertInLoopThread();
                    assert(delayedReplyExecuted);

                    assert(response.getResponseResult() ==
                           ResponseResult::kSuccess);
                    assert(response.getPayload() == "hello world");

                    ++rpcRoundSuccess;
                    client.disconnect();
                  });
    } else {
      ++clientDisconnected;
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
      stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(rpcRoundSuccess == 1);
  assert(clientDisconnected == 1);
  assert(serverStopped == 1);
}

void rpc_server_ignores_duplicate_reply_from_async_handler_test() {
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

  std::promise<void> stopCompletePromise;
  auto stopCompleteFuture = stopCompletePromise.get_future();

  int connected = 0;
  int rpcRoundSuccess = 0;
  int clientDisconnected = 0;
  int asyncHandlerInvoked = 0;
  int serverStopped = 0;

  EventLoop loop;
  RpcServer server(&loop, serverFd);
  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      stopCompletePromise.set_value();
      loop.quit();
    });
  });
  bool registered = server.registerAsyncMethod(
      "EchoService", "Echo",
      [&](const std::string &payload, RpcServer::RpcReply reply) {
        loop.runAfter(std::chrono::milliseconds(500),
                      [&asyncHandlerInvoked, payload,
                       reply = std::move(reply)]() mutable {
                        ++asyncHandlerInvoked;
                        reply(RpcServer::RpcResult{ResponseResult::kSuccess,
                                                   payload, ""});

                        if (asyncHandlerInvoked == 1) {
                          reply(RpcServer::RpcResult{ResponseResult::kSuccess,
                                                     "second response", ""});
                        }
                      });
      });
  assert(registered);

  server.start();

  RpcClient client(&loop, serverAddr);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;
      client.call(
          "EchoService", "Echo", "hello world",
          [&](const RpcResponse &response) {
            loop.assertInLoopThread();
            assert(response.getResponseResult() == ResponseResult::kSuccess);
            assert(response.getPayload() == "hello world");

            ++rpcRoundSuccess;

            client.call("EchoService", "Echo", "hello world2",
                        [&](const RpcResponse &response) {
                          loop.assertInLoopThread();
                          assert(response.getResponseResult() ==
                                 ResponseResult::kSuccess);
                          assert(response.getPayload() == "hello world2");

                          ++rpcRoundSuccess;
                          client.disconnect();
                        });
          });
    } else {
      ++clientDisconnected;
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
      stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(rpcRoundSuccess == 2);
  assert(clientDisconnected == 1);
  assert(asyncHandlerInvoked == 2);
  assert(serverStopped == 1);
}

void rpc_server_returns_error_when_async_handler_throws_test() {
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

  std::promise<void> stopCompletePromise;
  auto stopCompleteFuture = stopCompletePromise.get_future();

  int connected = 0;
  int rpcRoundError = 0;
  int clientDisconnected = 0;
  int asyncHandlerInvoked = 0;
  int serverStopped = 0;

  EventLoop loop;
  RpcServer server(&loop, serverFd);
  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      stopCompletePromise.set_value();
      loop.quit();
    });
  });
  bool registered = server.registerAsyncMethod(
      "EchoService", "Echo",
      [&](const std::string &payload, RpcServer::RpcReply reply) {
        ++asyncHandlerInvoked;
        throw std::runtime_error("error");
      });
  assert(registered);

  server.start();

  RpcClient client(&loop, serverAddr);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;
      client.call("EchoService", "Echo", "hello world",
                  [&](const RpcResponse &response) {
                    loop.assertInLoopThread();
                    assert(response.getResponseResult() ==
                           ResponseResult::kUnsuccess);
                    assert(response.getPayload().empty());
                    assert(response.getErrorMessage() == "handler exception");
                    ++rpcRoundError;
                    client.disconnect();
                  });
    } else {
      ++clientDisconnected;
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
      stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(rpcRoundError == 1);
  assert(clientDisconnected == 1);
  assert(asyncHandlerInvoked == 1);
  assert(serverStopped == 1);
}

void rpc_server_returns_error_for_invalid_async_handler_result_test() {
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

  std::promise<void> stopCompletePromise;
  auto stopCompleteFuture = stopCompletePromise.get_future();

  int connected = 0;
  int rpcRoundError = 0;
  int clientDisconnected = 0;
  int asyncHandlerInvoked = 0;
  int serverStopped = 0;

  EventLoop loop;
  RpcServer server(&loop, serverFd);
  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      stopCompletePromise.set_value();
      loop.quit();
    });
  });
  bool registered = server.registerAsyncMethod(
      "EchoService", "Echo",
      [&](const std::string &payload, RpcServer::RpcReply reply) {
        ++asyncHandlerInvoked;
        reply(RpcServer::RpcResult{ResponseResult::kSuccess, "",
                                   "unexpected error"});
      });
  assert(registered);

  server.start();

  RpcClient client(&loop, serverAddr);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;
      client.call(
          "EchoService", "Echo", "hello world",
          [&](const RpcResponse &response) {
            loop.assertInLoopThread();
            assert(response.getResponseResult() == ResponseResult::kUnsuccess);
            assert(response.getPayload().empty());
            assert(response.getErrorMessage() == "invalid handler result");
            ++rpcRoundError;
            client.disconnect();
          });
    } else {
      ++clientDisconnected;
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
      stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(rpcRoundError == 1);
  assert(clientDisconnected == 1);
  assert(asyncHandlerInvoked == 1);
  assert(serverStopped == 1);
}

void rpc_server_sends_response_when_async_reply_is_called_from_another_thread_test() {
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

  std::promise<void> stopCompletePromise;
  auto stopCompleteFuture = stopCompletePromise.get_future();

  int connected = 0;
  int rpcRoundSuccess = 0;
  int clientDisconnected = 0;
  std::atomic<int> asyncHandlerInvoked = 0;
  int serverStopped = 0;

  EventLoop loop;
  RpcServer server(&loop, serverFd);

  EventLoopThread eventLoopThread;
  auto threadLoop = eventLoopThread.startLoop();

  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      stopCompletePromise.set_value();
      loop.quit();
    });
  });
  bool registered = server.registerAsyncMethod(
      "EchoService", "Echo",
      [&](const std::string &payload, RpcServer::RpcReply reply) {
        threadLoop->queueInLoop([threadLoop, &asyncHandlerInvoked,
                                 reply = std::move(reply)]() mutable {
          threadLoop->assertInLoopThread();
          ++asyncHandlerInvoked;
          reply(RpcServer::RpcResult{ResponseResult::kSuccess, "hello world",
                                     ""});
        });
      });
  assert(registered);

  server.start();

  RpcClient client(&loop, serverAddr);
  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;
      client.call("EchoService", "Echo", "hello world",
                  [&](const RpcResponse &response) {
                    loop.assertInLoopThread();
                    assert(response.getResponseResult() ==
                           ResponseResult::kSuccess);
                    assert(response.getPayload() == "hello world");
                    assert(response.getErrorMessage().empty());
                    ++rpcRoundSuccess;
                    client.disconnect();
                  });
    } else {
      ++clientDisconnected;
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
      stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(rpcRoundSuccess == 1);
  assert(clientDisconnected == 1);
  assert(asyncHandlerInvoked.load() == 1);
  assert(serverStopped == 1);
}

void rpc_server_rejects_duplicate_method_registration_across_sync_and_async_handlers_test() {
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

  bool registered = server.registerAsyncMethod(
      "EchoService", "Echo",
      [](const std::string &payload, RpcServer::RpcReply reply) {});
  assert(registered);

  registered = server.registerMethod(
      "EchoService", "Echo",
      [](const std::string &payload) -> RpcServer::RpcResult {
        return RpcServer::RpcResult{};
      });
  assert(!registered);

  registered = server.registerMethod(
      "EchoService", "Echo another",
      [](const std::string &payload) -> RpcServer::RpcResult {
        return RpcServer::RpcResult{};
      });
  assert(registered);

  registered = server.registerAsyncMethod(
      "EchoService", "Echo another",
      [](const std::string &payload, RpcServer::RpcReply reply) {});
  assert(!registered);
}

void rpc_server_handles_async_reply_after_client_disconnects_test() {
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

  std::promise<void> stopCompletePromise;
  auto stopCompleteFuture = stopCompletePromise.get_future();
  std::promise<void> clientDownPromise;
  auto clientDownFuture = clientDownPromise.get_future();

  int connected = 0;
  int rpcRoundError = 0;
  int clientDisconnected = 0;
  std::atomic<int> asyncReplyInvoked = 0;
  int serverStopped = 0;

  EventLoop loop;
  RpcServer server(&loop, serverFd);
  RpcClient client(&loop, serverAddr);

  EventLoopThread eventLoopThread;
  auto threadLoop = eventLoopThread.startLoop();

  server.setStopCompleteCallback([&]() {
    loop.queueInLoop([&]() {
      ++serverStopped;
      stopCompletePromise.set_value();
      loop.quit();
    });
  });
  bool registered = server.registerAsyncMethod(
      "EchoService", "Echo",
      [&](const std::string &payload, RpcServer::RpcReply reply) {
        threadLoop->queueInLoop([&clientDownFuture, threadLoop,
                                 &asyncReplyInvoked, reply = std::move(reply),
                                 &loop, &server]() mutable {
          threadLoop->assertInLoopThread();
          auto waitForRes =
              clientDownFuture.wait_for(std::chrono::milliseconds(2000));
          if (waitForRes != std::future_status::ready) {
            std::cerr << "wait for client stop timeout" << std::endl;
            std::abort();
          }

          ++asyncReplyInvoked;
          reply(RpcServer::RpcResult{ResponseResult::kSuccess, "hello world",
                                     ""});

          loop.queueInLoop(
              [&server]() { server.stop(std::chrono::milliseconds(0)); });
        });

        client.disconnect();
      });
  assert(registered);

  server.start();

  client.setConnectionCallback([&](const TcpConnectionPtr &conn) {
    if (conn->connected()) {
      ++connected;
      client.call("EchoService", "Echo", "hello world",
                  [&](const RpcResponse &response) {
                    loop.assertInLoopThread();
                    assert(response.getResponseResult() ==
                           ResponseResult::kUnsuccess);
                    assert(response.getPayload().empty());
                    assert(response.getErrorMessage() == "connect closed");
                    ++rpcRoundError;
                  });
    } else {
      ++clientDisconnected;
      clientDownPromise.set_value();
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
      stopCompleteFuture.wait_for(std::chrono::milliseconds(2000));
  if (waitForRes != std::future_status::ready) {
    std::cerr << "wait for stop complete timeout" << std::endl;
    std::abort();
  }

  assert(connected == 1);
  assert(rpcRoundError == 1);
  assert(clientDisconnected == 1);
  assert(asyncReplyInvoked.load() == 1);
  assert(serverStopped == 1);
}

int main() {
  rpc_server_processes_echo_request_test();
  rpc_client_calls_echo_service_test();
  rpc_client_receives_error_for_unknown_service_test();
  rpc_client_reports_error_when_not_connected_test();
  rpc_client_reports_error_when_connection_closes_with_pending_request_test();
  rpc_server_rejects_duplicate_method_registration_test();
  rpc_client_receives_error_for_invalid_handler_result_test();
  rpc_client_matches_out_of_order_responses_by_request_id_test();
  rpc_client_closes_connection_for_unknown_response_request_id_test();
  rpc_client_receives_business_error_from_registered_handler_test();
  rpc_client_reports_timeout_when_server_does_not_respond_test();
  rpc_client_does_not_timeout_when_timeout_is_zero_test();
  rpc_server_executes_handler_on_io_loop_test();
  rpc_server_distributes_connections_across_io_loops_test();
  rpc_client_allows_call_from_non_loop_thread_test();
  rpc_server_returns_error_and_remains_available_when_handler_throws_test();
  rpc_client_ignores_late_response_after_request_timeout_test();
  rpc_client_reconnects_after_connection_closes_and_allows_new_call_test();
  rpc_client_reconnects_after_initial_connection_failure_test();
  rpc_client_disabling_retry_prevents_reconnection_test();
  rpc_client_rejects_call_when_max_pending_requests_is_reached_test();
  rpc_client_releases_pending_capacity_before_response_callback_test();
  rpc_client_releases_pending_capacity_after_request_timeout_test();
  rpc_client_uses_default_timeout_and_allows_explicit_zero_override_test();
  rpc_client_ignores_late_response_after_request_cancellation_test();
  rpc_server_returns_error_for_unregistered_method_test();
  rpc_server_keeps_other_methods_after_unregistering_one_method_test();
  rpc_client_allows_request_cancellation_from_non_loop_thread_test();
  rpc_client_releases_pending_capacity_after_request_cancellation_test();
  rpc_client_allows_calls_from_multiple_non_loop_threads_test();
  rpc_server_closes_connection_for_malformed_request_test();
  rpc_client_closes_connection_for_malformed_response_test();
  rpc_server_sends_delayed_response_from_async_handler_test();
  rpc_server_ignores_duplicate_reply_from_async_handler_test();
  rpc_server_returns_error_when_async_handler_throws_test();
  rpc_server_returns_error_for_invalid_async_handler_result_test();
  rpc_server_sends_response_when_async_reply_is_called_from_another_thread_test();
  rpc_server_rejects_duplicate_method_registration_across_sync_and_async_handlers_test();
  rpc_server_handles_async_reply_after_client_disconnects_test();
}