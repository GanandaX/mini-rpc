#include "RpcClient.h"

#include <assert.h>

RpcClient::RpcClient(EventLoop *loop, const InetAddress &serverAddr)
    : hasConnectAttempted_(false), maxPendingRequests_(std::nullopt),
      defaultTimeout_(std::chrono::milliseconds(0)), loop_(checkedLoop(loop)),
      status_(Status::kDisconnected), tcpClient_(loop_, serverAddr),
      clearIgnoredResponseRequestsTimerId_(std::nullopt) {
  loop_->assertInLoopThread();

  codec_.setMessageCallback(std::bind(&RpcClient::onRpcMessage, this,
                                      std::placeholders::_1,
                                      std::placeholders::_2));

  tcpClient_.setConnectionCallback(
      std::bind(&RpcClient::onConnection, this, std::placeholders::_1));
  tcpClient_.setConnectionErrorCallback(
      std::bind(&RpcClient::onConnectionError, this, std::placeholders::_1));
  tcpClient_.setMessageCallback(std::bind(&LengthHeaderCodec::onMessage,
                                          &codec_, std::placeholders::_1,
                                          std::placeholders::_2));
  tcpClient_.setPeerHalfCloseCallback(
      [](const TcpConnectionPtr &conn) { conn->shutdown(); });

  clearIgnoredResponseRequestsTimerId_ = loop_->runEvery(
      std::chrono::milliseconds(1000),
      std::bind(&RpcClient::clearExpiredIgnoredResponseRequests, this));
}

RpcClient::~RpcClient() {
  loop_->assertInLoopThread();

  clearPendingRequests();
  clearIgnoredResponseRequests();

  if (clearIgnoredResponseRequestsTimerId_.has_value()) {
    loop_->cancel(clearIgnoredResponseRequestsTimerId_.value());
  }
}

void RpcClient::setConnectionCallback(ConnectionCallback cb) {
  loop_->assertInLoopThread();
  assert(hasConnectAttempted_ == false);

  connectionCallback_ = std::move(cb);
}
void RpcClient::setConnectionErrorCallback(ConnectionErrorCallback cb) {
  loop_->assertInLoopThread();
  assert(hasConnectAttempted_ == false);

  connectionErrorCallback_ = std::move(cb);
}

void RpcClient::connect() {
  loop_->assertInLoopThread();
  if (status_ != Status::kDisconnected) {
    return;
  }

  status_ = Status::kConnecting;
  hasConnectAttempted_ = true;
  tcpClient_.connect();
}

void RpcClient::disconnect() {
  loop_->assertInLoopThread();
  if (status_ != Status::kConnected && status_ != Status::kConnecting) {
    return;
  }

  status_ = Status::kDisconnecting;
  tcpClient_.disconnect();
}

uint64_t RpcClient::call(const std::string &service, const std::string &method,
                         const std::string &payload, RpcCallback cb) {
  assert(cb != nullptr);

  uint64_t requestId = nextRequestId_.fetch_add(1);

  if (loop_->isInLoopThread()) {
    callInLoop(requestId, service, method, payload, std::move(cb),
               defaultTimeout_);
  } else {
    loop_->queueInLoop([this, requestId, service, method, payload,
                        callback = std::move(cb)]() mutable {
      callInLoop(requestId, service, method, payload, std::move(callback),
                 defaultTimeout_);
    });
  }
  return requestId;
}

uint64_t RpcClient::call(const std::string &service, const std::string &method,
                         const std::string &payload, RpcCallback cb,
                         std::chrono::milliseconds timeout) {
  assert(cb != nullptr);
  assert(timeout >= std::chrono::milliseconds{0});

  uint64_t requestId = nextRequestId_.fetch_add(1);

  if (loop_->isInLoopThread()) {
    callInLoop(requestId, service, method, payload, std::move(cb), timeout);
  } else {
    loop_->queueInLoop([this, requestId, service, method, payload,
                        callback = std::move(cb), timeout]() mutable {
      callInLoop(requestId, service, method, payload, std::move(callback),
                 timeout);
    });
  }
  return requestId;
}

void RpcClient::enableRetry() {
  loop_->assertInLoopThread();
  tcpClient_.enableRetry();
}

void RpcClient::disableRetry() {
  loop_->assertInLoopThread();
  tcpClient_.disableRetry();
}

void RpcClient::setMaxPendingRequests(size_t maxPendingRequests) {
  assert(maxPendingRequests > 0);
  loop_->assertInLoopThread();
  assert(!hasConnectAttempted_);

  maxPendingRequests_ = maxPendingRequests;
}

void RpcClient::setDefaultTimeout(std::chrono::milliseconds defaultTimeout) {
  assert(defaultTimeout >= std::chrono::milliseconds(0));
  loop_->assertInLoopThread();
  assert(!hasConnectAttempted_);

  defaultTimeout_ = defaultTimeout;
}

void RpcClient::cancel(uint64_t requestId) {
  if (loop_->isInLoopThread()) {
    cancelInLoop(requestId);
  } else {
    loop_->queueInLoop([this, requestId]() { cancelInLoop(requestId); });
  }
}

EventLoop *RpcClient::checkedLoop(EventLoop *loop) {
  assert(loop != nullptr);
  return loop;
}

void RpcClient::onConnection(const TcpConnectionPtr &conn) {
  loop_->assertInLoopThread();

  if (conn->connected()) {
    conn_ = conn;
    status_ = Status::kConnected;
  } else {
    conn_.reset();
    status_ = Status::kDisconnected;
    failAllPendingRequests("connect closed");
    clearIgnoredResponseRequests();
  }

  if (connectionCallback_) {
    connectionCallback_(conn);
  }
}

void RpcClient::onRpcMessage(const TcpConnectionPtr &conn,
                             const std::string &rpcBytes) {
  loop_->assertInLoopThread();

  RpcResponse response;
  std::string errorMsg;
  bool res = response.decode(rpcBytes, errorMsg);
  if (!res || !errorMsg.empty()) {
    conn->forceClose();
    return;
  }

  auto findRes = pendingRequests_.find(response.getRequestId());
  if (findRes == pendingRequests_.end()) {
    auto timeoutFindRes =
        ignoredResponseRequests_.find(response.getRequestId());
    if (timeoutFindRes != ignoredResponseRequests_.end()) {
      ignoredResponseRequests_.erase(timeoutFindRes);
      return;
    }
    conn->forceClose();
    return;
  }

  if (findRes->second.timerId.has_value()) {
    loop_->cancel(findRes->second.timerId.value());
  }

  RpcCallback cb = std::move(findRes->second.callback);
  pendingRequests_.erase(findRes->first);
  cb(response);
}

void RpcClient::onConnectionError(int errorCode) {
  loop_->assertInLoopThread();

  conn_.reset();
  status_ = Status::kDisconnected;
  failAllPendingRequests("connection error");
  clearIgnoredResponseRequests();
  if (connectionErrorCallback_) {
    connectionErrorCallback_(errorCode);
  }
}

void RpcClient::failAllPendingRequests(std::string errorMessage) {
  loop_->assertInLoopThread();

  std::unordered_map<uint64_t, PendingRequest> temp;
  temp.swap(pendingRequests_);

  for (auto &item : temp) {
    if (item.second.timerId.has_value()) {
      loop_->cancel(item.second.timerId.value());
    }
  }

  for (auto &item : temp) {
    RpcResponse response(item.first, ResponseResult::kUnsuccess, "",
                         errorMessage);
    (item.second.callback)(response);
  }
}

void RpcClient::onRequestTimeout(uint64_t requestId) {
  loop_->assertInLoopThread();

  auto findRes = pendingRequests_.find(requestId);
  if (findRes == pendingRequests_.end()) {
    return;
  }

  PendingRequest pendingRequest = std::move(findRes->second);
  pendingRequests_.erase(findRes);

  auto oneMinuteLater =
      std::chrono::steady_clock::now() + std::chrono::minutes(1);
  ignoredResponseRequests_[requestId] = oneMinuteLater;

  RpcResponse response(requestId, ResponseResult::kUnsuccess, "",
                       "rpc request timeout");
  (pendingRequest.callback)(response);
}

void RpcClient::callInLoop(uint64_t requestId, const std::string &service,
                           const std::string &method,
                           const std::string &payload, RpcCallback cb,
                           std::chrono::milliseconds timeout) {
  loop_->assertInLoopThread();
  assert(timeout >= std::chrono::milliseconds{0});
  assert(cb != nullptr);

  do {
    if ((status_ == Status::kConnected) && (conn_ != nullptr) &&
        (conn_->connected())) {

      if (maxPendingRequests_.has_value() &&
          pendingRequests_.size() >= maxPendingRequests_) {
        RpcResponse response(requestId, ResponseResult::kUnsuccess, "",
                             "too many pending requests");
        cb(response);
        return;
      }

      RpcRequest request(requestId, service, method, payload);
      std::string output;
      std::string errorMsg;
      bool res = request.encode(output, errorMsg);
      if (!res || !errorMsg.empty()) {
        break;
      }

      pendingRequests_[requestId] = {std::move(cb), std::nullopt};
      if (timeout > std::chrono::milliseconds(0)) {
        TimerId timerId = loop_->runAfter(
            timeout, std::bind(&RpcClient::onRequestTimeout, this, requestId));
        pendingRequests_[requestId].timerId = timerId;
      }

      codec_.send(conn_, output.data(), output.length());
      return;
    }
  } while (false);

  RpcResponse response(requestId, ResponseResult::kUnsuccess, "",
                       "connection not connected or encode error");
  cb(response);
}

void RpcClient::clearExpiredIgnoredResponseRequests() {
  loop_->assertInLoopThread();

  auto now = std::chrono::steady_clock::now();

  for (auto it = ignoredResponseRequests_.begin();
       it != ignoredResponseRequests_.end();) {
    if (it->second <= now) {
      it = ignoredResponseRequests_.erase(it);
    } else {
      ++it;
    }
  }
}

void RpcClient::clearPendingRequests() {
  loop_->assertInLoopThread();

  std::unordered_map<uint64_t, PendingRequest> temp;
  temp.swap(pendingRequests_);

  for (auto &item : temp) {
    if (item.second.timerId.has_value()) {
      loop_->cancel(item.second.timerId.value());
    }
  }
}

void RpcClient::clearIgnoredResponseRequests() {
  loop_->assertInLoopThread();
  ignoredResponseRequests_.clear();
}

void RpcClient::cancelInLoop(uint64_t requestId) {
  loop_->assertInLoopThread();

  auto findRes = pendingRequests_.find(requestId);
  if (findRes == pendingRequests_.end()) {
    return;
  }

  auto callback = std::move(findRes->second.callback);
  if (findRes->second.timerId.has_value()) {
    loop_->cancel(findRes->second.timerId.value());
  }
  pendingRequests_.erase(findRes);

  auto oneMinuteLater =
      std::chrono::steady_clock::now() + std::chrono::minutes(1);
  ignoredResponseRequests_[requestId] = oneMinuteLater;

  RpcResponse response{requestId, ResponseResult::kUnsuccess, "",
                       "rpc request canceled"};
  callback(response);
}