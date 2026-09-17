#include <assert.h>
#include <iomanip>
#include <iostream>

#include "RpcMessage.h"

void rpc_request_round_trip_preserves_embedded_nul_test() {
  std::string expected("hello\0 world", 12);
  RpcRequest request(0x112233, "EchoService", "echo", expected);
  std::string output;
  std::string errorMessage;
  assert(true == request.encode(output, errorMessage));
  assert(errorMessage.empty());

  RpcRequest requestResolve;
  std::string errorMsg;
  bool res = requestResolve.decode(output, errorMsg);

  assert(res == true);
  assert(errorMsg.empty());
  assert(requestResolve.getRequestId() == 0x112233);
  assert(requestResolve.getService() == "EchoService");
  assert(requestResolve.getMethod() == "echo");
  assert(requestResolve.getPayload() == expected);
  assert(requestResolve.getPayload().size() == 12);
}

void rpc_request_round_trip_preserves_empty_payload_test() {
  RpcRequest request(0x112233, "EchoService", "echo", "");
  std::string output;
  std::string errorMessage;
  assert(true == request.encode(output, errorMessage));
  assert(errorMessage.empty());

  RpcRequest requestResolve;
  std::string errorMsg;
  bool res = requestResolve.decode(output, errorMsg);

  assert(res == true);
  assert(errorMsg.empty());
  assert(requestResolve.getRequestId() == 0x112233);
  assert(requestResolve.getService() == "EchoService");
  assert(requestResolve.getMethod() == "echo");
  assert(requestResolve.getPayload().empty());
}

void rpc_response_round_trip_preserves_success_payload_test() {
  std::string expected("hello\0 world", 12);
  RpcResponse response(0x112233, ResponseResult::kSuccess, expected, "");
  std::string output;
  std::string errorMessage;
  assert(true == response.encode(output, errorMessage));
  assert(errorMessage.empty());

  RpcResponse responseResolve;
  std::string errorMsg;
  bool res = responseResolve.decode(output, errorMsg);

  assert(res == true);
  assert(errorMsg.empty());
  assert(responseResolve.getRequestId() == 0x112233);
  assert(responseResolve.getResponseResult() == ResponseResult::kSuccess);
  assert(responseResolve.getPayload() == expected);
  assert(responseResolve.getPayload().size() == 12);
  assert(responseResolve.getErrorMessage().empty());
}

void rpc_response_round_trip_preserves_error_message_test() {
  RpcResponse response(0x112233, ResponseResult::kUnsuccess, "", "error");

  std::string output;
  std::string errorMessage;
  assert(true == response.encode(output, errorMessage));
  assert(errorMessage.empty());

  RpcResponse responseResolve;
  std::string errorMsg;
  bool res = responseResolve.decode(output, errorMsg);

  assert(res == true);
  assert(errorMsg.empty());
  assert(responseResolve.getRequestId() == 0x112233);
  assert(responseResolve.getResponseResult() == ResponseResult::kUnsuccess);
  assert(responseResolve.getPayload().empty());
  assert(responseResolve.getErrorMessage() == "error");
}

void rpc_response_rejects_truncated_message_test() {
  RpcResponse response(0x112233, ResponseResult::kUnsuccess, "", "error");
  std::string output;
  std::string errorMessage;
  assert(true == response.encode(output, errorMessage));
  assert(errorMessage.empty());

  RpcResponse responseResolve;
  std::string errorMsg;
  bool res =
      responseResolve.decode(output.substr(0, output.length() - 1), errorMsg);

  assert(res == false);
}

void rpc_request_rejects_trailing_bytes_test() {
  RpcRequest request(0x112233, "EchoService", "echo", "");
  std::string output;
  std::string errorMessage;
  assert(true == request.encode(output, errorMessage));
  assert(errorMessage.empty());
  output.push_back('H');

  RpcRequest requestResolve;
  std::string errorMsg;
  bool res = requestResolve.decode(output, errorMsg);

  assert(res == false);
  assert(!errorMsg.empty());
  assert(requestResolve.getMethod().empty());
  assert(requestResolve.getPayload().empty());
  assert(requestResolve.getRequestId() == 0);
  assert(requestResolve.getService().empty());
}

void rpc_response_decode_failure_preserves_existing_value_test() {
  RpcResponse response(7, ResponseResult::kSuccess, "old", "");
  std::string output;
  std::string errorMessage;
  assert(true == response.encode(output, errorMessage));
  assert(errorMessage.empty());

  RpcResponse responseResolve;
  std::string errorMsg;
  bool res =
      responseResolve.decode(output.substr(0, output.length()), errorMsg);
  assert(res == true);
  assert(errorMsg.empty());

  errorMsg.clear();
  res = responseResolve.decode(output.substr(0, output.length() - 1), errorMsg);

  assert(res == false);
  assert(responseResolve.getRequestId() == 7);
  assert(responseResolve.getResponseResult() == ResponseResult::kSuccess);
  assert(responseResolve.getPayload() == "old");
  assert(responseResolve.getErrorMessage().empty());
}

void rpc_request_rejects_empty_message_test() {

  RpcRequest request(0x112233, "EchoService", "echo", "");
  std::string output;
  std::string errorMessage;
  assert(true == request.encode(output, errorMessage));
  assert(errorMessage.empty());

  RpcRequest requestResolve;
  std::string errorMsg;
  bool res = requestResolve.decode("", errorMsg);

  assert(res == false);
  assert(!errorMsg.empty());
  assert(requestResolve.getRequestId() == 0);
  assert(requestResolve.getService().empty());
  assert(requestResolve.getMethod().empty());
  assert(requestResolve.getPayload().empty());
}

void rpc_response_rejects_empty_message_test() {
  RpcResponse response(0x112233, ResponseResult::kUnsuccess, "", "");
  std::string output;
  std::string errorMessage;
  assert(true == response.encode(output, errorMessage));
  assert(errorMessage.empty());

  RpcResponse responseResolve;
  std::string errorMsg;
  bool res = responseResolve.decode("", errorMsg);

  assert(res == false);
  assert(!errorMsg.empty());
  assert(responseResolve.getRequestId() == 0);
  assert(responseResolve.getResponseResult() == ResponseResult::kUnsuccess);
  assert(responseResolve.getPayload().empty());
  assert(responseResolve.getErrorMessage().empty());
}

void rpc_response_encode_rejects_inconsistent_result_test() {
  RpcResponse response(0x112233, ResponseResult::kUnsuccess, "hello", "");
  std::string output;
  std::string errorMessage;
  assert(false == response.encode(output, errorMessage));
  assert(!errorMessage.empty());
}

int main() {
  rpc_request_round_trip_preserves_embedded_nul_test();
  rpc_request_round_trip_preserves_empty_payload_test();
  rpc_response_round_trip_preserves_success_payload_test();
  rpc_response_round_trip_preserves_error_message_test();
  rpc_response_rejects_truncated_message_test();
  rpc_request_rejects_trailing_bytes_test();
  rpc_response_decode_failure_preserves_existing_value_test();
  rpc_request_rejects_empty_message_test();
  rpc_response_rejects_empty_message_test();
  rpc_response_encode_rejects_inconsistent_result_test();
}