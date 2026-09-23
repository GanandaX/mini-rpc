#ifndef MINI_RPC_RPC_CLIENT_H
#define MINI_RPC_RPC_CLIENT_H

#include "EventLoop.h"
#include "InetAddress.h"
#include "LengthHeaderCodec.h"
#include "RpcMessage.h"
#include "TcpClient.h"
#include "TcpConnection.h"

#include <atomic>
#include <chrono>
#include <cstddef>
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

  uint64_t call(const std::string &service, const std::string &method,
                const std::string &payload, RpcCallback cb);

  uint64_t call(const std::string &service, const std::string &method,
                const std::string &payload, RpcCallback cb,
                std::chrono::milliseconds timeout);

  void enableRetry();
  void disableRetry();

  void setMaxPendingRequests(size_t maxPendingRequests);

  /**
   * RpcClient 默认超时为 0ms，表示不启用超时；调用 setDefaultTimeout 可在首次
   * connect() 前修改它。
   */
  void setDefaultTimeout(std::chrono::milliseconds defaultTimeout);

  void cancel(uint64_t requestId);

private:
  static EventLoop *checkedLoop(EventLoop *loop);
  void onConnection(const TcpConnectionPtr &conn);
  void onRpcMessage(const TcpConnectionPtr &conn, const std::string &rpcBytes);
  void onConnectionError(int errorCode);
  void failAllPendingRequests(std::string errorMessage);
  void onRequestTimeout(uint64_t requestId);

  void callInLoop(uint64_t requestId, const std::string &service,
                  const std::string &method, const std::string &payload,
                  RpcCallback cb, std::chrono::milliseconds timeout);

  void clearExpiredIgnoredResponseRequests();

  void clearPendingRequests();
  void clearIgnoredResponseRequests();

  void cancelInLoop(uint64_t requestId);

private:
  enum class Status { kDisconnected, kConnecting, kDisconnecting, kConnected };

  struct PendingRequest {
    RpcCallback callback;
    std::optional<TimerId> timerId;
  };

  bool hasConnectAttempted_;
  std::atomic<uint64_t> nextRequestId_ = 1;
  std::optional<size_t> maxPendingRequests_;
  std::chrono::milliseconds defaultTimeout_;
  EventLoop *loop_;
  Status status_;
  LengthHeaderCodec codec_;
  TcpClient tcpClient_;
  TcpConnectionPtr conn_;
  ConnectionCallback connectionCallback_;
  ConnectionErrorCallback connectionErrorCallback_;
  std::unordered_map<uint64_t, PendingRequest> pendingRequests_;
  std::optional<TimerId> clearIgnoredResponseRequestsTimerId_;
  std::unordered_map<uint64_t, std::chrono::steady_clock::time_point>
      ignoredResponseRequests_;
};

#endif