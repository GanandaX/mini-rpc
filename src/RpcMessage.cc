#include "RpcMessage.h"

namespace {

bool isLittleEnd() {
  size_t i = 1;
  return *(char *)(&i) == 1;
}

template <class T> T hton(T t) {
  T netValue = t;
  if (isLittleEnd()) {
    size_t i = 0;
    char *hostValPoint = reinterpret_cast<char *>(&t) + sizeof(T);
    char *netValPoint = reinterpret_cast<char *>(&netValue);
    for (i = 0; i < sizeof(T); ++i) {
      netValPoint[i] = *(--hostValPoint);
    }
  }

  return netValue;
}

template <class T> T ntoh(T t) {
  /*  T hostValue = t;
   if (!isLittleEnd()) {
     size_t i = 0;
     char *netValPoint = reinterpret_cast<char *>(&t) + sizeof(T);
     char *hostValPoint = reinterpret_cast<char *>(&hostValue);
     for (i = 0; i < sizeof(T); ++i) {
       hostValPoint[i] = *(--netValPoint);
     }
   }

   return hostValue;*/

  return hton(t);
}

bool loadString(size_t &index, size_t &leftLen, char *data,
                std::string &target) {
  uint32_t netLen = 0;
  uint32_t hostLen = 0;

  if (leftLen < sizeof(netLen)) {
    std::cerr << "left len error" << std::endl;
    return false;
  }
  ::memcpy(&netLen, data + index, sizeof(netLen));
  hostLen = ntoh(netLen);
  leftLen -= sizeof(netLen);
  index += sizeof(netLen);

  if (leftLen < hostLen) {
    return false;
  }
  target.append(data + index, hostLen);
  leftLen -= hostLen;
  index += hostLen;
  return true;
}

} // namespace

RpcRequest::RpcRequest() : requestId_(0), service_(), method_(), payload_() {}

RpcRequest::RpcRequest(uint64_t requestId, std::string service,
                       std::string method, std::string paylod)
    : requestId_(requestId), service_(std::move(service)),
      method_(std::move(method)), payload_(std::move(paylod)) {}

bool RpcRequest::encode(std::string &output, std::string &errorMessage) const {
  if (service_.length() > UINT32_MAX || method_.length() > UINT32_MAX ||
      payload_.length() > UINT32_MAX) {
    errorMessage.append("service_ or method or payload out of capacity!");
    return false;
  }

  size_t msgLen = 21 + service_.length() + method_.length() + payload_.length();

  output.resize(msgLen);
  char *data = output.data();

  size_t index = 0;
  uint64_t requestIdNet = requestId_;
  uint32_t serviceLenNet = service_.length();
  uint32_t methodLenNet = method_.length();
  uint32_t payloadLenNet = payload_.length();

  requestIdNet = hton(requestIdNet);
  serviceLenNet = hton(serviceLenNet);
  methodLenNet = hton(methodLenNet);
  payloadLenNet = hton(payloadLenNet);

  uint8_t type = RpcMessageType::kRequest;
  ::memcpy(data + index, &type, 1);
  index += 1;
  ::memcpy(data + index, &requestIdNet, sizeof(requestIdNet));
  index += sizeof(requestIdNet);
  ::memcpy(data + index, &serviceLenNet, sizeof(serviceLenNet));
  index += sizeof(serviceLenNet);
  ::memcpy(data + index, service_.data(), service_.length());
  index += service_.length();
  ::memcpy(data + index, &methodLenNet, sizeof(methodLenNet));
  index += sizeof(methodLenNet);
  ::memcpy(data + index, method_.data(), method_.length());
  index += method_.length();
  ::memcpy(data + index, &payloadLenNet, sizeof(payloadLenNet));
  index += sizeof(payloadLenNet);
  ::memcpy(data + index, payload_.data(), payload_.length());
  index += payload_.length();

  return true;
}

bool RpcRequest::decode(std::string requestMsg, std::string &errorMsg) {
  size_t leftLen = requestMsg.length();

  if (leftLen <= 0) {
    errorMsg.append("length <= 0");
    return false;
  }

  size_t index = 0;

  char *data = requestMsg.data();
  uint8_t type = data[index];
  --leftLen;
  ++index;

  if (type != RpcMessageType::kRequest) {
    errorMsg.append("illegal request type");
    return false;
  }

  uint64_t requestIdNet;
  uint64_t requestIdHost;

  if (leftLen < sizeof(requestIdNet)) {
    errorMsg.append("request id length error");
    return false;
  }
  ::memcpy(&requestIdNet, data + index, sizeof(requestIdNet));
  requestIdHost = ntoh(requestIdNet);
  leftLen -= sizeof(requestIdNet);
  index += sizeof(requestIdNet);

  std::string service;
  std::string method;
  std::string payload;
  if (!loadString(index, leftLen, data, service)) {
    errorMsg.append("service load error");
    return false;
  }
  if (!loadString(index, leftLen, data, method)) {
    errorMsg.append("method load error");
    return false;
  }
  if (!loadString(index, leftLen, data, payload)) {
    errorMsg.append("payload load error");
    return false;
  }

  if (leftLen != 0) {
    errorMsg.append("msg length too much");
    return false;
  }

  requestId_ = requestIdHost;
  service_ = std::move(service);
  method_ = std::move(method);
  payload_ = std::move(payload);

  return true;
}

uint64_t RpcRequest::getRequestId() const { return requestId_; }

std::string RpcRequest::getService() const { return service_; }

std::string RpcRequest::getMethod() const { return method_; }

std::string RpcRequest::getPayload() const { return payload_; }

RpcResponse::RpcResponse()
    : requestId_(0), responseResult_(ResponseResult::kUnsuccess), payload_(),
      errorMessage_() {}

RpcResponse::RpcResponse(uint64_t requestId, ResponseResult responseResult,
                         std::string paylod, std::string errorMessage)
    : requestId_(requestId), responseResult_(responseResult),
      payload_(std::move(paylod)), errorMessage_(std::move(errorMessage)) {}

bool RpcResponse::encode(std::string &output, std::string &errorMessage) const {
  if (payload_.length() > UINT32_MAX || errorMessage_.length() > UINT32_MAX) {
    errorMessage.append("payload or error message out of capacity!");
    return false;
  }

  if (((responseResult_ == ResponseResult::kSuccess) &&
       !errorMessage_.empty()) ||
      (((responseResult_ == ResponseResult::kUnsuccess) &&
        !payload_.empty()))) {
    errorMessage.append("traffic error!");
    return false;
  }

  if (responseResult_ != ResponseResult::kSuccess &&
      responseResult_ != ResponseResult::kUnsuccess) {
    errorMessage.append("illegal response result");
    return false;
  }

  size_t msgLen = 18 + payload_.length() + errorMessage_.length();

  output.resize(msgLen);
  char *data = output.data();

  size_t index = 0;
  uint64_t requestIdNet = requestId_;
  uint32_t payloadLenNet = payload_.length();
  uint32_t errorMessageLenNet = errorMessage_.length();

  requestIdNet = hton(requestIdNet);
  payloadLenNet = hton(payloadLenNet);
  errorMessageLenNet = hton(errorMessageLenNet);

  uint8_t type = RpcMessageType::kResponse;
  uint8_t responseResult = responseResult_;
  ::memcpy(data + index, &type, 1);
  index += 1;
  ::memcpy(data + index, &requestIdNet, sizeof(requestIdNet));
  index += sizeof(requestIdNet);
  ::memcpy(data + index, &responseResult, sizeof(responseResult));
  index += sizeof(responseResult);
  ::memcpy(data + index, &payloadLenNet, sizeof(payloadLenNet));
  index += sizeof(payloadLenNet);
  ::memcpy(data + index, payload_.data(), payload_.length());
  index += payload_.length();
  ::memcpy(data + index, &errorMessageLenNet, sizeof(errorMessageLenNet));
  index += sizeof(errorMessageLenNet);
  ::memcpy(data + index, errorMessage_.data(), errorMessage_.length());
  index += errorMessage_.length();

  return true;
}

bool RpcResponse::decode(std::string requestMsg, std::string &errorMsg) {
  size_t leftLen = requestMsg.length();

  if (leftLen <= 0) {
    errorMsg.append("length <= 0");
    return false;
  }

  size_t index = 0;

  char *data = requestMsg.data();
  uint8_t type = data[index];
  --leftLen;
  ++index;

  if (type != RpcMessageType::kResponse) {
    errorMsg.append("illegal response type");
    return false;
  }

  uint64_t requestIdNet;
  uint64_t requestIdHost;

  if (leftLen < sizeof(requestIdNet)) {
    errorMsg.append("request id length error");
    return false;
  }
  ::memcpy(&requestIdNet, data + index, sizeof(requestIdNet));
  requestIdHost = ntoh(requestIdNet);
  leftLen -= sizeof(requestIdNet);
  index += sizeof(requestIdNet);

  if (leftLen < 1) {
    errorMsg.append("response result length error");
    return false;
  }

  uint8_t success = data[index];
  --leftLen;
  ++index;
  if (success != ResponseResult::kSuccess &&
      success != ResponseResult::kUnsuccess) {
    errorMsg.append("illegal response result");
    return false;
  }

  std::string payload;
  std::string errorMessage;

  if (!loadString(index, leftLen, data, payload)) {
    errorMsg.append("payload load error");
    return false;
  }

  if (!loadString(index, leftLen, data, errorMessage)) {
    errorMsg.append("error message load error");
    return false;
  }

  if (leftLen != 0) {
    errorMsg.append("msg length too much");
    return false;
  }

  if ((success == ResponseResult::kSuccess && !errorMessage.empty()) ||
      (success == ResponseResult::kUnsuccess && !payload.empty())) {
    errorMsg.append("数据格式出错");
    return false;
  }

  requestId_ = requestIdHost;
  responseResult_ = (ResponseResult)success;
  payload_ = std::move(payload);
  errorMessage_ = std::move(errorMessage);

  return true;
}

uint64_t RpcResponse::getRequestId() const { return requestId_; }

ResponseResult RpcResponse::getResponseResult() const {
  return responseResult_;
}

std::string RpcResponse::getPayload() const { return payload_; }

std::string RpcResponse::getErrorMessage() const { return errorMessage_; }
