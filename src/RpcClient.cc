#include "RpcClient.h"

#include <assert.h>

RpcClient::RpcClient(EventLoop *loop, const InetAddress &serverAddr)
    : hasConnectAttempted_(false), loop_(checkedLoop(loop)),
      status_(Status::kDisconnected), tcpClient_(loop_, serverAddr),
      timeoutCronTimerId(std::nullopt) {
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

  timeoutCronTimerId =
      loop_->runEvery(std::chrono::milliseconds(1000),
                      std::bind(&RpcClient::timeoutCron, this));
}

RpcClient::~RpcClient() {
  loop_->assertInLoopThread();

  clearPendingRequests();
  clearTimedOutRequests();

  if (timeoutCronTimerId.has_value()) {
    loop_->cancel(timeoutCronTimerId.value());
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

void RpcClient::call(const std::string &service, const std::string &method,
                     const std::string &payload, RpcCallback cb,
                     std::chrono::milliseconds timeout) {
  assert(cb != nullptr);
  assert(timeout >= std::chrono::milliseconds{0});

  if (loop_->isInLoopThread()) {
    callInLoop(service, method, payload, std::move(cb), timeout);
  } else {
    loop_->queueInLoop([this, service, method, payload,
                        callback = std::move(cb), timeout]() mutable {
      callInLoop(service, method, payload, std::move(callback), timeout);
    });
  }
}

void RpcClient::enableRetry() {
  loop_->assertInLoopThread();
  tcpClient_.enableRetry();
}

void RpcClient::disableRetry() {
  loop_->assertInLoopThread();
  tcpClient_.disableRetry();
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
    clearTimedOutRequests();
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
    auto timeoutFindRes = timeoutRequests_.find(response.getRequestId());
    if (timeoutFindRes != timeoutRequests_.end()) {
      timeoutRequests_.erase(timeoutFindRes);
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
  clearTimedOutRequests();
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
  timeoutRequests_[requestId] = oneMinuteLater;

  RpcResponse response(requestId, ResponseResult::kUnsuccess, "",
                       "rpc request timeout");
  (pendingRequest.callback)(response);
}

void RpcClient::callInLoop(const std::string &service,
                           const std::string &method,
                           const std::string &payload, RpcCallback cb,
                           std::chrono::milliseconds timeout) {
  loop_->assertInLoopThread();
  assert(timeout >= std::chrono::milliseconds{0});
  assert(cb != nullptr);

  uint64_t requestId = nextRequestId_++;

  do {
    if ((status_ == Status::kConnected) && (conn_ != nullptr) &&
        (conn_->connected())) {
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

void RpcClient::timeoutCron() {
  loop_->assertInLoopThread();

  auto now = std::chrono::steady_clock::now();

  for (auto it = timeoutRequests_.begin(); it != timeoutRequests_.end();) {
    if (it->second <= now) {
      it = timeoutRequests_.erase(it);
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

void RpcClient::clearTimedOutRequests() {
  loop_->assertInLoopThread();
  timeoutRequests_.clear();
}
