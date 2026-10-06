#include "RpcServer.h"
#include <assert.h>
#include <atomic>
#include <map>
#include <mutex>
#include <optional>

class CancellationRegistry {
public:
  explicit CancellationRegistry() = default;

  std::optional<RpcCancellationToken> take(const TcpConnectionPtr &ptr,
                                           uint64_t requestId);
  bool registerToken(const TcpConnectionPtr &ptr, uint64_t requestId,
                     RpcCancellationToken token);

  void cancel(const TcpConnectionPtr &conn, uint64_t requestId);

  void clearConnectionCancellations(const TcpConnectionPtr &conn);

private:
  std::mutex mutex_;
  std::map<std::pair<TcpConnectionPtr, uint64_t>, RpcCancellationToken>
      cancellations;
  friend class RpcServer;
};

namespace {
void sendFramedData(const TcpConnectionPtr &conn, uint64_t requestId,
                    ResponseResult responseResult, std::string payload,
                    std::string errorMessage) {
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
CancellationRegistry::take(const TcpConnectionPtr &ptr, uint64_t requestId) {
  std::lock_guard<std::mutex> locker(mutex_);

  std::optional<RpcCancellationToken> value;

  auto findRes = cancellations.find({ptr, requestId});
  if (findRes == cancellations.end()) {
    return value;
  }

  value.emplace(findRes->second);
  cancellations.erase(findRes);

  return value;
}

bool CancellationRegistry::registerToken(const TcpConnectionPtr &ptr,
                                         uint64_t requestId,
                                         RpcCancellationToken token) {
  std::lock_guard<std::mutex> locker(mutex_);

  auto findRes = cancellations.find({ptr, requestId});
  if (findRes != cancellations.end()) {
    return false;
  }

  cancellations.emplace(std::make_pair(ptr, requestId), std::move(token));
  return true;
}

void CancellationRegistry::cancel(const TcpConnectionPtr &conn,
                                  uint64_t requestId) {
  std::lock_guard<std::mutex> locker(mutex_);

  auto findRes = cancellations.find({conn, requestId});
  if (findRes == cancellations.end()) {
    return;
  }

  findRes->second.canceled_->store(true);
  cancellations.erase(findRes);
}

void CancellationRegistry::clearConnectionCancellations(
    const TcpConnectionPtr &conn) {
  std::lock_guard<std::mutex> locker(mutex_);
  for (auto it = cancellations.begin(); it != cancellations.end();) {
    if (it->first.first == conn) {
      it->second.canceled_->store(true);
      it = cancellations.erase(it);
    } else {
      ++it;
    }
  }
}

RpcServer::RpcServer(EventLoop *loop, int listenFd, int threadNum)
    : loop_(loop), status_(Status::kNotStarted) {
  assert(loop != nullptr);
  assert(listenFd >= 0);
  assert(threadNum >= 0);
  loop_->assertInLoopThread();

  server_ = std::make_unique<TcpServer>(loop, listenFd, threadNum);
  cancellationRegistry_ = std::make_shared<CancellationRegistry>();

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

void RpcServer::onRpcMessage(const TcpConnectionPtr &conn,
                             const std::string &msg) {
  if (msg.empty()) {
    conn->forceClose();
    return;
  }

  if (msg[0] == RpcMessageType::kCancel) {
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
  bool res =
      cancellationRegistry_->registerToken(conn, requestId, cancellationToken);
  if (!res) {
    conn->forceClose();
    return;
  }

  std::weak_ptr<CancellationRegistry> weakCancellationRegistryPtr =
      cancellationRegistry_;

  std::shared_ptr<std::atomic<bool>> repliedFlag =
      std::make_shared<std::atomic<bool>>(false);
  std::function<void(RpcResult)> reply =
      [weakPtr, requestId, repliedFlag,
       weakCancellationRegistryPtr](RpcResult rpcResult) -> void {
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

    conn->loop()->queueInLoop([conn, requestId,
                               rpcResult = std::move(rpcResult),
                               weakCancellationRegistryPtr]() mutable {
      auto registry = weakCancellationRegistryPtr.lock();
      if (!registry) {
        return;
      }

      auto token = registry->take(conn, requestId);

      if (!token.has_value() || token->isCanceled()) {
        return;
      }

      sendFramedData(conn, requestId, rpcResult.result,
                     std::move(rpcResult.payload),
                     std::move(rpcResult.errorMessage));
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