#pragma once

#include "gmock/gmock.h"

#include "aios/network/arpc/arpc/ANetRPCServer.h"
#include "aios/network/http_arpc/HTTPRPCServer.h"
#include "unittest/unittest.h"

namespace worker_framework {

class MockHTTPRPCServer : public http_arpc::HTTPRPCServer {
public:
    MockHTTPRPCServer(arpc::ANetRPCServer *rpcServer) : HTTPRPCServer(rpcServer) {}
    MOCK_METHOD4(Listen, bool(const std::string &, int, int, int));
    MOCK_METHOD0(Close, void());
    MOCK_METHOD0(StartThreads, bool());
};

}; // namespace worker_framework
