#include "CalculatorProtocol.h"
#include <string.h>

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
} // namespace

CalculatorProtocol::CalculatorProtocol() : CalculatorProtocol(0, 0) {}

CalculatorProtocol::CalculatorProtocol(int32_t a, int32_t b) : a_(a), b_(b), result_(0) {}

void CalculatorProtocol::setA(int32_t a) { a_ = a; }

void CalculatorProtocol::setB(int32_t b) { b_ = b; }

void CalculatorProtocol::setResult(int32_t result) { result_ = result; }

int32_t CalculatorProtocol::getA() const { return a_; }

int32_t CalculatorProtocol::getB() const { return b_; }

int32_t CalculatorProtocol::getResult() const { return result_; }

std::string CalculatorProtocol::requestEncode() const {
  std::string msg;
  msg.resize(8);

  char *data = msg.data();

  int32_t aNet = a_;
  int32_t bNet = b_;

  if (isLittleEnd()) {
    aNet = hton(aNet);
    bNet = hton(bNet);
  }

  ::memcpy(data, &aNet, sizeof(int32_t));
  ::memcpy(data + sizeof(int32_t), &bNet, sizeof(int32_t));

  return msg;
}

bool CalculatorProtocol::requestDecode(const std::string &msg,
                                       std::string &errorMessage) {
  errorMessage.clear();
  if (msg.length() != 8) {
    errorMessage.append("invalid add request");
    return false;
  }

  const char *data = msg.data();

  int32_t aNet;
  int32_t bNet;

  ::memcpy(&aNet, data, sizeof(int32_t));
  ::memcpy(&bNet, data + sizeof(int32_t), sizeof(int32_t));

  if (isLittleEnd()) {
    aNet = hton(aNet);
    bNet = hton(bNet);
  }

  a_ = aNet;
  b_ = bNet;

  return true;
}

std::string CalculatorProtocol::resultEncode() const {
  std::string msg;
  msg.resize(4);

  char *data = msg.data();

  int32_t resultNet = result_;

  if (isLittleEnd()) {
    resultNet = hton(resultNet);
  }

  ::memcpy(data, &resultNet, sizeof(int32_t));

  return msg;
}

bool CalculatorProtocol::resultDecode(const std::string &msg,
                                      std::string &errorMessage) {
  errorMessage.clear();
  if (msg.length() != 4) {
    errorMessage.append("invalid result response");
    return false;
  }

  const char *data = msg.data();

  int32_t resultNet;

  ::memcpy(&resultNet, data, sizeof(int32_t));

  if (isLittleEnd()) {
    resultNet = hton(resultNet);
  }

  result_ = resultNet;

  return true;
}