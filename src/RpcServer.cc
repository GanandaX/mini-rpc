#include "RpcServer.h"
#include <assert.h>
#include <atomic>
#include <map>
#include <mutex>
#include <optional>

enum class RegisterTokenStatus {
  kRegistered,
  kDuplicateRequestId,
  kGlobalLimitReached,
  kConnectionLimitReached
};

class CancellationRegistry {
public:
  explicit CancellationRegistry() = default;

  std::optional<RpcCancellationToken> take(const TcpConnectionPtr &conn,
                                           uint64_t requestId);
  RegisterTokenStatus registerToken(const TcpConnectionPtr &conn,
                                    uint64_t requestId,
                                    RpcCancellationToken token);

  void cancel(const TcpConnectionPtr &conn, uint64_t requestId);

  void clearConnectionCancellations(const TcpConnectionPtr &conn);

private:
  std::mutex mutex_;
  std::optional<uint64_t> maxActiveRequests_;
  std::optional<uint64_t> maxActiveRequestsPerConnection_;
  std::map<TcpConnectionPtr, std::unordered_map<uint64_t, RpcCancellationToken>>
      cancellations_;
  friend class RpcServer;
};

namespace {
void sendFramedData(const TcpConnectionPtr &conn, uint64_t requestId,
                    ResponseResult responseResult, std::string payload,
                    std::string errorMessage, size_t maxMessageSize) {

  size_t outputMessageLen =
      RpcResponse::OutputMessageLen(payload.length(), errorMessage.length());
  if (outputMessageLen > maxMessageSize) {
    if (maxMessageSize <
        RpcResponse::OutputMessageLen(0, strlen("rpc message too large"))) {
      conn->forceClose();
      return;
    }

    RpcResponse response(requestId, ResponseResult::kUnsuccess, "",
                         "rpc message too large");
    std::string outputMessage;
    errorMessage.clear();
    bool res = response.encode(outputMessage, errorMessage);
    if (!res || !errorMessage.empty()) {
      conn->forceClose();
      return;
    }

    if (conn->connected()) {
      LengthHeaderCodec codec;
      codec.send(conn, outputMessage.data(), outputMessage.length());
    }
    return;
  }

  RpcResponse response(requestId, responseResult, payload, errorMessage);
  std::string outputMessage;
  errorMessage.clear();
  bool res = response.encode(outputMessage, errorMessage);
  if (!res || !errorMessage.empty()) {
    conn->forceClose();
    return;
  }

  if (conn->connected()) {
    LengthHeaderCodec codec;
    codec.send(conn, outputMessage.data(), outputMessage.length());
  }
}
} // namespace

bool RpcCancellationToken::isCanceled() const { return canceled_->load(); }

RpcCancellationToken::RpcCancellationToken(
    std::shared_ptr<std::atomic<bool>> canceled) {
  canceled_ = std::move(canceled);
}

std::optional<RpcCancellationToken>
CancellationRegistry::take(const TcpConnectionPtr &conn, uint64_t requestId) {
  std::lock_guard<std::mutex> locker(mutex_);

  std::optional<RpcCancellationToken> value;

  auto connFindRes = cancellations_.find(conn);
  if (connFindRes == cancellations_.end()) {
    return value;
  }

  auto requestFindRes = connFindRes->second.find(requestId);
  if (requestFindRes == connFindRes->second.end()) {
    return value;
  }

  value.emplace(requestFindRes->second);
  connFindRes->second.erase(requestFindRes);

  if (connFindRes->second.size() == 0) {
    cancellations_.erase(connFindRes);
  }

  return value;
}

RegisterTokenStatus
CancellationRegistry::registerToken(const TcpConnectionPtr &conn,
                                    uint64_t requestId,
                                    RpcCancellationToken token) {
  std::lock_guard<std::mutex> locker(mutex_);

  auto connFindRes = cancellations_.find(conn);

  // 已有连接时，先识别重复 requestId。
  if (connFindRes != cancellations_.end()) {
    auto requestFindRes = connFindRes->second.find(requestId);
    if (requestFindRes != connFindRes->second.end()) {
      return RegisterTokenStatus::kDuplicateRequestId;
    }
  }

  // 检查全局活动异步请求上限。
  if (maxActiveRequests_.has_value()) {
    uint64_t activeRequests = 0;
    for (const auto &item : cancellations_) {
      activeRequests += item.second.size();
    }

    if (activeRequests >= maxActiveRequests_.value()) {
      return RegisterTokenStatus::kGlobalLimitReached;
    }
  }

  // 仅已有连接项才需要检查其当前请求数。
  if (connFindRes != cancellations_.end() &&
      maxActiveRequestsPerConnection_.has_value() &&
      connFindRes->second.size() >= maxActiveRequestsPerConnection_.value()) {
    return RegisterTokenStatus::kConnectionLimitReached;
  }

  // 所有检查通过后，才为新连接创建内层 map。
  if (connFindRes == cancellations_.end()) {
    connFindRes =
        cancellations_
            .emplace(conn, std::unordered_map<uint64_t, RpcCancellationToken>{})
            .first;
  }

  connFindRes->second.emplace(requestId, std::move(token));
  return RegisterTokenStatus::kRegistered;
}

void CancellationRegistry::cancel(const TcpConnectionPtr &conn,
                                  uint64_t requestId) {
  std::lock_guard<std::mutex> locker(mutex_);

  auto connFindRes = cancellations_.find(conn);
  if (connFindRes == cancellations_.end()) {
    return;
  }

  auto requestFindRes = connFindRes->second.find(requestId);
  if (requestFindRes == connFindRes->second.end()) {
    return;
  }

  requestFindRes->second.canceled_->store(true);

  connFindRes->second.erase(requestFindRes);
  if (connFindRes->second.size() == 0) {
    cancellations_.erase(connFindRes);
  }
}

void CancellationRegistry::clearConnectionCancellations(
    const TcpConnectionPtr &conn) {
  std::lock_guard<std::mutex> locker(mutex_);

  auto connFindRes = cancellations_.find(conn);

  if (connFindRes == cancellations_.end()) {
    return;
  }

  for (auto it = connFindRes->second.begin(); it != connFindRes->second.end();
       ++it) {
    it->second.canceled_->store(true);
  }

  cancellations_.erase(conn);
}

RpcServer::RpcServer(EventLoop *loop, int listenFd, int threadNum)
    : loop_(loop), status_(Status::kNotStarted) {
  assert(loop != nullptr);
  assert(listenFd >= 0);
  assert(threadNum >= 0);
  loop_->assertInLoopThread();

  server_ = std::make_unique<TcpServer>(loop, listenFd, threadNum);
  cancellationRegistry_ = std::make_shared<CancellationRegistry>();
  maxMessageSize_ = std::make_shared<std::optional<size_t>>();

  lengthHeaderCodec_.setMessageCallback(std::bind(&RpcServer::onRpcMessage,
                                                  this, std::placeholders::_1,
                                                  std::placeholders::_2));
  server_->setMessageCallback(
      std::bind(&LengthHeaderCodec::onMessage, &lengthHeaderCodec_,
                std::placeholders::_1, std::placeholders::_2));

  auto registry = cancellationRegistry_;
  server_->setConnectionCallback([registry](const TcpConnectionPtr &conn) {
    if (!conn->connected()) {
      registry->clearConnectionCancellations(conn);
    }
  });

  server_->setPeerHalfCloseCallback(
      [](const TcpConnectionPtr &conn) { conn->shutdown(); });

  server_->setStopCompleteCallback([this]() {
    status_ = Status::kStopped;
    if (stopCompleteCallback_) {
      stopCompleteCallback_();
    }
  });
}

void RpcServer::start() {
  loop_->assertInLoopThread();
  if (status_ != Status::kNotStarted) {
    return;
  }
  status_ = Status::kRunning;
  server_->start();
}

void RpcServer::stop(std::chrono::milliseconds gracePeriod) {
  loop_->assertInLoopThread();

  if (status_ != Status::kRunning) {
    return;
  }
  status_ = Status::kStopping;
  server_->stop(gracePeriod);
}

void RpcServer::setStopCompleteCallback(StopCompleteCallback cb) {
  loop_->assertInLoopThread();
  assert(status_ == Status::kNotStarted);

  stopCompleteCallback_ = std::move(cb);
}

bool RpcServer::registerMethod(const std::string &service,
                               const std::string &method, RpcHandler handler) {

  loop_->assertInLoopThread();
  assert(status_ == Status::kNotStarted);

  if (service.empty() || method.empty() || !handler) {
    return false;
  }

  if (isRegistered(service, method)) {
    return false;
  }

  auto serviceItr = syncHandlers_.find(service);
  if (serviceItr == syncHandlers_.end()) {
    std::unordered_map<std::string, RpcHandler> methodMap;
    methodMap[method] = std::move(handler);
    syncHandlers_[service] = std::move(methodMap);
    return true;
  }

  auto handlerItr = serviceItr->second.find(method);
  if (handlerItr == serviceItr->second.end()) {
    (serviceItr->second)[method] = std::move(handler);
    return true;
  }

  return false;
}

bool RpcServer::registerAsyncMethod(const std::string &service,
                                    const std::string &method,
                                    RpcAsyncHandler asyncHandler) {
  loop_->assertInLoopThread();
  assert(status_ == Status::kNotStarted);

  if (service.empty() || method.empty() || !asyncHandler) {
    return false;
  }

  if (isRegistered(service, method)) {
    return false;
  }

  auto serviceItr = asyncHandlers_.find(service);
  if (serviceItr == asyncHandlers_.end()) {
    std::unordered_map<std::string, RpcAsyncHandler> methodMap;
    methodMap[method] = std::move(asyncHandler);
    asyncHandlers_[service] = std::move(methodMap);
    return true;
  }

  auto handlerItr = serviceItr->second.find(method);
  if (handlerItr == serviceItr->second.end()) {
    (serviceItr->second)[method] = std::move(asyncHandler);
    return true;
  }

  return false;
}

bool RpcServer::unregisterMethod(const std::string &service,
                                 const std::string &method) {
  loop_->assertInLoopThread();
  assert(status_ == Status::kNotStarted);

  if (service.empty() || method.empty()) {
    return false;
  }

  if (isRegisteredInSync(service, method)) {
    auto serviceItr = syncHandlers_.find(service);
    if (serviceItr == syncHandlers_.end()) {
      return false;
    }

    auto handlerItr = serviceItr->second.find(method);
    if (handlerItr == serviceItr->second.end()) {
      return false;
    }

    serviceItr->second.erase(method);

    if (serviceItr->second.empty()) {
      syncHandlers_.erase(serviceItr);
    }

    return true;
  }

  if (isRegisteredInAsync(service, method)) {
    auto serviceItr = asyncHandlers_.find(service);
    if (serviceItr == asyncHandlers_.end()) {
      return false;
    }

    auto handlerItr = serviceItr->second.find(method);
    if (handlerItr == serviceItr->second.end()) {
      return false;
    }

    serviceItr->second.erase(method);

    if (serviceItr->second.empty()) {
      asyncHandlers_.erase(serviceItr);
    }
    return true;
  }

  return false;
}

void RpcServer::setMaxActiveRequests(size_t maxActiveRequests) {
  loop_->assertInLoopThread();
  assert(status_ == Status::kNotStarted);
  assert(maxActiveRequests > 0);

  cancellationRegistry_->maxActiveRequests_ = maxActiveRequests;
}

void RpcServer::setMaxActiveRequestsPerConnection(size_t maxActiveRequests) {
  loop_->assertInLoopThread();
  assert(status_ == Status::kNotStarted);
  assert(maxActiveRequests > 0);

  cancellationRegistry_->maxActiveRequestsPerConnection_ = maxActiveRequests;
}

void RpcServer::setMaxMessageSize(size_t maxMessageSize) {
  loop_->assertInLoopThread();
  assert(status_ == Status::kNotStarted);
  assert(maxMessageSize > 0);
  assert(maxMessageSize <= LengthHeaderCodec::kMaxMessageSize);

  *maxMessageSize_ = maxMessageSize;
}

void RpcServer::onRpcMessage(const TcpConnectionPtr &conn,
                             const std::string &msg) {
  if (msg.empty()) {
    conn->forceClose();
    return;
  }

  size_t effectiveMaxMessageSize =
      maxMessageSize_->value_or(LengthHeaderCodec::kMaxMessageSize);

  if (msg.length() > effectiveMaxMessageSize) {
    conn->forceClose();
    return;
  }

  RpcMessageType messageType;
  bool decodeRes = decodeHead(msg, messageType);
  if (!decodeRes) {
    conn->forceClose();
    return;
  }

  if (messageType == RpcMessageType::kCancel) {
    RpcCancel cancel;
    std::string errorMessage;

    bool res = cancel.decode(msg, errorMessage);
    if (!res || !errorMessage.empty()) {
      conn->forceClose();
      return;
    }

    cancelAsync(conn, cancel.getRequestId());
    return;
  }

  if (messageType != RpcMessageType::kRequest) {
    conn->forceClose();
    return;
  }

  RpcRequest request;
  std::string errorMessage;
  bool res = request.decode(msg, errorMessage);
  if (!res || !errorMessage.empty()) {
    conn->forceClose();
    return;
  }

  if (isRegisteredInSync(request.getService(), request.getMethod())) {
    invokeSyncHandler(conn, request);
    return;
  }

  if (isRegisteredInAsync(request.getService(), request.getMethod())) {
    invokeAsyncHandler(conn, request);
    return;
  }

  sendResponse(conn, request.getRequestId(), ResponseResult::kUnsuccess, "",
               "service or method not exist");
  return;
}

void RpcServer::sendResponse(const TcpConnectionPtr &conn, uint64_t requestId,
                             ResponseResult responseResult, std::string payload,
                             std::string errorMessage) {

  size_t outputMessageLen =
      RpcResponse::OutputMessageLen(payload.length(), errorMessage.length());
  size_t effectiveMaxMessageSize =
      maxMessageSize_->value_or(LengthHeaderCodec::kMaxMessageSize);
  if (outputMessageLen > effectiveMaxMessageSize) {
    if (effectiveMaxMessageSize <
        RpcResponse::OutputMessageLen(0, strlen("rpc message too large"))) {
      conn->forceClose();
      return;
    }

    RpcResponse response(requestId, ResponseResult::kUnsuccess, "",
                         "rpc message too large");
    std::string outputMessage;
    errorMessage.clear();
    bool res = response.encode(outputMessage, errorMessage);
    if (!res || !errorMessage.empty()) {
      conn->forceClose();
      return;
    }

    if (conn->connected()) {
      lengthHeaderCodec_.send(conn, outputMessage.data(),
                              outputMessage.length());
    }
    return;
  }

  RpcResponse response(requestId, responseResult, payload, errorMessage);
  std::string outputMessage;
  errorMessage.clear();
  bool res = response.encode(outputMessage, errorMessage);
  if (!res || !errorMessage.empty()) {
    conn->forceClose();
    return;
  }

  if (conn->connected()) {
    lengthHeaderCodec_.send(conn, outputMessage.data(), outputMessage.length());
  }
}

bool RpcServer::isRegistered(const std::string &service,
                             const std::string &method) {
  return isRegisteredInSync(service, method) ||
         isRegisteredInAsync(service, method);
}

bool RpcServer::isRegisteredInSync(const std::string &service,
                                   const std::string &method) {
  auto syncServiceItr = syncHandlers_.find(service);

  if (syncServiceItr == syncHandlers_.end()) {
    return false;
  }

  auto handlerItr = syncServiceItr->second.find(method);
  if (handlerItr != syncServiceItr->second.end()) {
    return true;
  }
  return false;
}

bool RpcServer::isRegisteredInAsync(const std::string &service,
                                    const std::string &method) {
  auto asyncServiceItr = asyncHandlers_.find(service);
  if (asyncServiceItr == asyncHandlers_.end()) {
    return false;
  }

  auto handlerItr = asyncServiceItr->second.find(method);
  if (handlerItr != asyncServiceItr->second.end()) {
    return true;
  }
  return false;
}

void RpcServer::invokeSyncHandler(const TcpConnectionPtr &conn,
                                  RpcRequest request) {
  auto serviceItr = syncHandlers_.find(request.getService());
  auto handler = serviceItr->second.find(request.getMethod())->second;

  RpcResult rpcResult;
  try {
    rpcResult = handler(request.getPayload());
  } catch (...) {
    sendResponse(conn, request.getRequestId(), ResponseResult::kUnsuccess, "",
                 "handler exception");
    return;
  }

  if ((rpcResult.result == ResponseResult::kUnsuccess &&
       !rpcResult.payload.empty()) ||
      (rpcResult.result == ResponseResult::kSuccess &&
       !rpcResult.errorMessage.empty()) ||
      (rpcResult.result != ResponseResult::kSuccess &&
       rpcResult.result != ResponseResult::kUnsuccess)) {
    sendResponse(conn, request.getRequestId(), ResponseResult::kUnsuccess, "",
                 "invalid handler result");
    return;
  }

  sendResponse(conn, request.getRequestId(), rpcResult.result,
               std::move(rpcResult.payload), std::move(rpcResult.errorMessage));
}

void RpcServer::invokeAsyncHandler(const TcpConnectionPtr &conn,
                                   RpcRequest request) {
  auto serviceItr = asyncHandlers_.find(request.getService());
  auto handler = serviceItr->second.find(request.getMethod())->second;

  std::weak_ptr<TcpConnection> weakPtr = conn;
  uint64_t requestId = request.getRequestId();

  RpcCancellationToken cancellationToken(
      std::make_shared<std::atomic<bool>>(false));
  RegisterTokenStatus res =
      cancellationRegistry_->registerToken(conn, requestId, cancellationToken);

  if (res != RegisterTokenStatus::kRegistered) {
    if (res == RegisterTokenStatus::kDuplicateRequestId) {
      conn->forceClose();
    } else if (res == RegisterTokenStatus::kConnectionLimitReached ||
               res == RegisterTokenStatus::kGlobalLimitReached) {
      sendResponse(conn, request.getRequestId(), ResponseResult::kUnsuccess, "",
                   "server request limit reached");
    }
    return;
  }

  std::weak_ptr<CancellationRegistry> weakCancellationRegistryPtr =
      cancellationRegistry_;
  std::weak_ptr<std::optional<size_t>> weakMaxMessageSizePtr = maxMessageSize_;

  std::shared_ptr<std::atomic<bool>> repliedFlag =
      std::make_shared<std::atomic<bool>>(false);
  std::function<void(RpcResult)> reply =
      [weakPtr, requestId, repliedFlag, weakCancellationRegistryPtr,
       weakMaxMessageSizePtr](RpcResult rpcResult) -> void {
    if (repliedFlag->exchange(true)) {
      return;
    }

    auto conn = weakPtr.lock();
    if (!conn) {
      return;
    }

    if ((rpcResult.result == ResponseResult::kUnsuccess &&
         !rpcResult.payload.empty()) ||
        (rpcResult.result == ResponseResult::kSuccess &&
         !rpcResult.errorMessage.empty()) ||
        (rpcResult.result != ResponseResult::kSuccess &&
         rpcResult.result != ResponseResult::kUnsuccess)) {
      rpcResult = {ResponseResult::kUnsuccess, "", "invalid handler result"};
    }

    conn->loop()->queueInLoop(
        [conn, requestId, rpcResult = std::move(rpcResult),
         weakCancellationRegistryPtr, weakMaxMessageSizePtr]() mutable {
          auto registry = weakCancellationRegistryPtr.lock();
          if (!registry) {
            return;
          }
          auto maxMessageSize = weakMaxMessageSizePtr.lock();
          if (!maxMessageSize) {
            return;
          }

          auto token = registry->take(conn, requestId);

          if (!token.has_value() || token->isCanceled()) {
            return;
          }

          size_t effectiveMaxMessageSize =
              maxMessageSize->value_or(LengthHeaderCodec::kMaxMessageSize);

          sendFramedData(
              conn, requestId, rpcResult.result, std::move(rpcResult.payload),
              std::move(rpcResult.errorMessage), effectiveMaxMessageSize);
        });
  };

  conn->loop()->runInLoop(
      [request, handler, reply, token = cancellationToken]() {
        try {
          handler(request.getPayload(), token, reply);
        } catch (...) {
          reply(RpcServer::RpcResult{ResponseResult::kUnsuccess, "",
                                     "handler exception"});
          return;
        }
      });
}

void RpcServer::cancelAsync(const TcpConnectionPtr &conn, uint64_t requestId) {
  cancellationRegistry_->cancel(conn, requestId);
}