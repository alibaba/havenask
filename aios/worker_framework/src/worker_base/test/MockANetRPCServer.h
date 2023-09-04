#pragma once

#include "gmock/gmock.h"

#include "aios/network/arpc/arpc/ANetRPCServer.h"
#include "unittest/unittest.h"

namespace worker_framework {

class MockANetRPCServer : public arpc::ANetRPCServer {
public:
    MOCK_METHOD4(Listen, bool(const std::string &, int, int, int));
    MOCK_METHOD0(Close, bool());
    MOCK_METHOD1(doRegisterService, bool(RPCService *));
};

}; // namespace worker_framework
