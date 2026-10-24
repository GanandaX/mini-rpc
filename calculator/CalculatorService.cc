#include "CalculatorService.h"
#include "CalculatorProtocol.h"
#include "RpcServer.h"

bool CalculatorService::registerCalculatorService(RpcServer &server) {

  return server.registerMethod(
      "Calculator", "Add",
      [](const std::string &payload) -> RpcServer::RpcResult {
        std::string errorMessage;
        CalculatorProtocol calculatorAdd;
        bool res = calculatorAdd.requestDecode(payload, errorMessage);

        if (!res || !errorMessage.empty()) {
          return RpcServer::RpcResult{ResponseResult::kUnsuccess, "",
                                      "invalid add request"};
        }

        int64_t result =
            static_cast<int64_t>(calculatorAdd.getA()) + calculatorAdd.getB();
        if (result > INT32_MAX || result < INT32_MIN) {
          return RpcServer::RpcResult{ResponseResult::kUnsuccess, "",
                                      "integer overflow"};
        }

        calculatorAdd.setResult(result);
        std::string msg = calculatorAdd.resultEncode();
        return RpcServer::RpcResult{ResponseResult::kSuccess, msg, ""};
      });
}