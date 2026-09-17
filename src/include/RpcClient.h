#ifndef MINI_RPC_RPC_CLIENT_H
#define MINI_RPC_RPC_CLIENT_H

#include "EventLoop.h"
#include "InetAddress.h"
#include "LengthHeaderCodec.h"
#include "RpcMessage.h"
#include "TcpClient.h"
#include "TcpConnection.h"

#include <chrono>
#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <unordered_map>

using RpcCallback = std::function<void(const RpcResponse &)>;

class RpcClient {
public:
  explicit RpcClient(EventLoop *loop, const InetAddress &serverAddr);
  ~RpcClient();

  void setConnectionCallback(ConnectionCallback cb);
  void setConnectionErrorCallback(ConnectionErrorCallback cb);

  void connect();
  void disconnect();

  void call(const std::string &service, const std::string &method,
            const std::string &payload, RpcCallback cb,
            std::chrono::milliseconds timeout = std::chrono::milliseconds{0});

  void enableRetry();
  void disableRetry();

private:
  static EventLoop *checkedLoop(EventLoop *loop);
  void onConnection(const TcpConnectionPtr &conn);
  void onRpcMessage(const TcpConnectionPtr &conn, const std::string &rpcBytes);
  void onConnectionError(int errorCode);
  void failAllPendingRequests(std::string errorMessage);
  void onRequestTimeout(uint64_t requestId);

  void callInLoop(const std::string &service, const std::string &method,
                  const std::string &payload, RpcCallback cb,
                  std::chrono::milliseconds timeout);

  void timeoutCron();

  void clearPendingRequests();
  void clearTimedOutRequests();

private:
  enum class Status { kDisconnected, kConnecting, kDisconnecting, kConnected };

  struct PendingRequest {
    RpcCallback callback;
    std::optional<TimerId> timerId;
  };

  bool hasConnectAttempted_;
  uint64_t nextRequestId_ = 1;
  EventLoop *loop_;
  Status status_;
  LengthHeaderCodec codec_;
  TcpClient tcpClient_;
  TcpConnectionPtr conn_;
  ConnectionCallback connectionCallback_;
  ConnectionErrorCallback connectionErrorCallback_;
  std::unordered_map<uint64_t, PendingRequest> pendingRequests_;
  std::optional<TimerId> timeoutCronTimerId;
  std::unordered_map<uint64_t, std::chrono::steady_clock::time_point>
      timeoutRequests_;
};

#endif