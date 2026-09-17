#ifndef MINI_RPC_RPC_MESSAGE_H
#define MINI_RPC_RPC_MESSAGE_H

#include <arpa/inet.h>
#include <assert.h>
#include <cstdint>
#include <iostream>
#include <string.h>
#include <string>

enum RpcMessageType { kRequest, kResponse };
enum ResponseResult { kSuccess, kUnsuccess };

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

#endif