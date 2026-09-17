#include "RpcServer.h"
#include <assert.h>

RpcServer::RpcServer(EventLoop *loop, int listenFd, int threadNum)
    : loop_(loop), status_(Status::kNotStarted) {
  assert(loop != nullptr);
  assert(listenFd >= 0);
  assert(threadNum >= 0);
  loop_->assertInLoopThread();

  server_ = std::make_unique<TcpServer>(loop, listenFd, threadNum);

  lengthHeaderCodec_.setMessageCallback(std::bind(&RpcServer::onRpcMessage,
                                                  this, std::placeholders::_1,
                                                  std::placeholders::_2));
  server_->setMessageCallback(
      std::bind(&LengthHeaderCodec::onMessage, &lengthHeaderCodec_,
                std::placeholders::_1, std::placeholders::_2));
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

  auto serviceItr = handlers_.find(service);
  if (serviceItr == handlers_.end()) {
    std::unordered_map<std::string, RpcHandler> methodMap;
    methodMap[method] = std::move(handler);
    handlers_[service] = std::move(methodMap);
    return true;
  }

  auto handlerItr = serviceItr->second.find(method);
  if (handlerItr == serviceItr->second.end()) {
    (serviceItr->second)[method] = std::move(handler);
    return true;
  }

  return false;
}

void RpcServer::onRpcMessage(const TcpConnectionPtr &conn,
                             const std::string &msg) {
  RpcRequest request;
  std::string errorMessage;
  bool res = request.decode(msg, errorMessage);
  if (!res || !errorMessage.empty()) {
    conn->forceClose();
    return;
  }

  auto serviceItr = handlers_.find(request.getService());
  if (serviceItr == handlers_.end() ||
      serviceItr->second.find(request.getMethod()) ==
          serviceItr->second.end()) {
    sendResponse(conn, request.getRequestId(), ResponseResult::kUnsuccess, "",
                 "service or method not exist");
    return;
  }

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