#ifndef MINI_RPC_RPC_SERVER_H
#define MINI_RPC_RPC_SERVER_H

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <unordered_map>

#include "EventLoop.h"
#include "LengthHeaderCodec.h"
#include "RpcMessage.h"
#include "TcpServer.h"

class CancellationRegistry;

class RpcCancellationToken {
public:
  bool isCanceled() const;

private:
  explicit RpcCancellationToken(std::shared_ptr<std::atomic<bool>> canceled);

  std::shared_ptr<std::atomic<bool>> canceled_;

  friend class RpcServer;
  friend class CancellationRegistry;
};

class RpcServer {
public:
  struct RpcResult {
    ResponseResult result;
    std::string payload;
    std::string errorMessage;
  };

public:
  using StopCompleteCallback = std::function<void()>;
  using RpcHandler = std::function<RpcResult(const std::string &)>;
  using RpcReply = std::function<void(RpcResult)>;
  using RpcAsyncHandler =
      std::function<void(const std::string &payload,
                         RpcCancellationToken cancellation, RpcReply reply)>;

  explicit RpcServer(EventLoop *loop, int listenFd, int threadNum = 0);

  void start();
  void stop(std::chrono::milliseconds gracePeriod);

  void setStopCompleteCallback(StopCompleteCallback cb);

  bool registerMethod(const std::string &service, const std::string &method,
                      RpcHandler handler);

  bool registerAsyncMethod(const std::string &service,
                           const std::string &method,
                           RpcAsyncHandler asyncHandler);

  bool unregisterMethod(const std::string &service, const std::string &method);

  void setMaxActiveRequests(size_t maxActiveRequests);

  void setMaxActiveRequestsPerConnection(size_t maxActiveRequests);

private:
  void onRpcMessage(const TcpConnectionPtr &, const std::string &);

  void sendResponse(const TcpConnectionPtr &conn, uint64_t requestId,
                    ResponseResult responseResult, std::string payload,
                    std::string errorMessage);

  bool isRegistered(const std::string &service, const std::string &method);

  bool isRegisteredInSync(const std::string &service,
                          const std::string &method);

  bool isRegisteredInAsync(const std::string &service,
                           const std::string &method);

  void invokeSyncHandler(const TcpConnectionPtr &conn, RpcRequest request);

  void invokeAsyncHandler(const TcpConnectionPtr &conn, RpcRequest request);

  void cancelAsync(const TcpConnectionPtr &conn, uint64_t requestId);

private:
  enum class Status { kNotStarted, kRunning, kStopping, kStopped };

  EventLoop *loop_;
  Status status_;
  LengthHeaderCodec lengthHeaderCodec_;
  std::unique_ptr<TcpServer> server_;
  std::unordered_map<std::string, std::unordered_map<std::string, RpcHandler>>
      syncHandlers_;
  std::unordered_map<std::string,
                     std::unordered_map<std::string, RpcAsyncHandler>>
      asyncHandlers_;

  std::shared_ptr<CancellationRegistry> cancellationRegistry_;

  StopCompleteCallback stopCompleteCallback_;
};

#endif