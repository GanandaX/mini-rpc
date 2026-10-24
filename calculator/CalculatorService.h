#ifndef CALCULATOR_SERVICE_H
#define CALCULATOR_SERVICE_H

class RpcServer;

class CalculatorService {
public:
  explicit CalculatorService() = default;

  bool registerCalculatorService(RpcServer &server);
};

#endif