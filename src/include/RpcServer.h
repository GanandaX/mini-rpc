#ifndef MINI_RPC_RPC_SERVER_H
#define MINI_RPC_RPC_SERVER_H

#include <chrono>
#include <functional>
#include <memory>
#include <unordered_map>

#include "EventLoop.h"
#include "LengthHeaderCodec.h"
#include "RpcMessage.h"
#include "TcpServer.h"

class RpcServer {
public:
  struct RpcResult {
    ResponseResult result;
    std::string payload;
    std::string errorMessage;
  };

public:
  using StopCompleteCallback = std::function<void()>;
  using RpcHandler = std::function<RpcResult(const std::string &payload)>;

  explicit RpcServer(EventLoop *loop, int listenFd, int threadNum = 0);

  void start();
  void stop(std::chrono::milliseconds gracePeriod);

  void setStopCompleteCallback(StopCompleteCallback cb);

  bool registerMethod(const std::string &service, const std::string &method,
                      RpcHandler handler);

private:
  void onRpcMessage(const TcpConnectionPtr &, const std::string &);

  void sendResponse(const TcpConnectionPtr &conn, uint64_t requestId,
                    ResponseResult responseResult, std::string payload,
                    std::string errorMessage);

private:
  enum class Status { kNotStarted, kRunning, kStopping, kStopped };

  EventLoop *loop_;
  Status status_;
  LengthHeaderCodec lengthHeaderCodec_;
  std::unique_ptr<TcpServer> server_;
  std::unordered_map<std::string, std::unordered_map<std::string, RpcHandler>>
      handlers_;

  StopCompleteCallback stopCompleteCallback_;
};

#endif