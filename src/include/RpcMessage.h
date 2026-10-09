#ifndef MINI_RPC_RPC_MESSAGE_H
#define MINI_RPC_RPC_MESSAGE_H

#include <arpa/inet.h>
#include <assert.h>
#include <cstdint>
#include <iostream>
#include <string.h>
#include <string>

constexpr uint16_t kRpcProtocolVersion = 1;

constexpr uint16_t VERSION_LEN = sizeof(uint16_t);
constexpr uint16_t REQUEST_TYPE_LEN = sizeof(uint8_t);
constexpr uint16_t REQUEST_ID_LEN = sizeof(uint64_t);
constexpr uint16_t SERVICE_NAME_LEN = sizeof(uint32_t);
constexpr uint16_t METHOD_NAME_LEN = sizeof(uint32_t);
constexpr uint16_t RESPONSE_RESULT_LEN = sizeof(uint8_t);
constexpr uint16_t PALOAD_MESSAGE_LEN = sizeof(uint32_t);
constexpr uint16_t ERROR_MESSAGE_LEN = sizeof(uint32_t);

enum RpcMessageType { kRequest, kResponse, kCancel };
enum ResponseResult { kSuccess, kUnsuccess };

bool decodeHead(const std::string &msg, RpcMessageType &messageType);

class RpcRequest {
public:
  explicit RpcRequest();
  explicit RpcRequest(uint64_t requestId, std::string service,
                      std::string method, std::string paylod);

  bool encode(std::string &output, std::string &errorMessage) const;
  bool decode(std::string requestMsg, std::string &errorMsg);

  uint64_t getRequestId() const;
  std::string getService() const;
  std::string getMethod() const;
  std::string getPayload() const;

private:
  uint64_t requestId_;
  std::string service_;
  std::string method_;
  std::string payload_;
};

class RpcResponse {
public:
  explicit RpcResponse();
  explicit RpcResponse(uint64_t requestId, ResponseResult responseResult,
                       std::string paylod, std::string errorMessage);

  bool encode(std::string &output, std::string &errorMessage) const;
  bool decode(std::string requestMsg, std::string &errorMsg);

  uint64_t getRequestId() const;
  ResponseResult getResponseResult() const;
  std::string getPayload() const;
  std::string getErrorMessage() const;

private:
  uint64_t requestId_;
  ResponseResult responseResult_;
  std::string payload_;
  std::string errorMessage_;
};

class RpcCancel {
public:
  explicit RpcCancel();
  explicit RpcCancel(uint64_t requestId);

  bool encode(std::string &output, std::string &errorMessage) const;
  bool decode(std::string message, std::string &errorMessage);

  uint64_t getRequestId() const;

private:
  uint64_t requestId_;
};

#endif