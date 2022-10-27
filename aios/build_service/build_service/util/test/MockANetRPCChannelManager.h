#ifndef ISEARCH_BS_MOCKANETRPCCHANNELMANAGER_H
#define ISEARCH_BS_MOCKANETRPCCHANNELMANAGER_H

#include "build_service/test/unittest.h"
#include "build_service/test/test.h"
#include "build_service/util/Log.h"
#include <arpc/ANetRPCChannelManager.h>

namespace arpc {

class MockANetRPCChannelManager : public ANetRPCChannelManager
{
public:
    MockANetRPCChannelManager() {}
    ~MockANetRPCChannelManager() {}

public:
    MOCK_METHOD6(OpenChannel, RPCChannel*(const std::string&, bool, size_t, int, bool, anet::CONNPRIORITY));
    MOCK_METHOD0(Close, void());
};

typedef ::testing::StrictMock<MockANetRPCChannelManager> StrictMockANetRPCChannelManager;
typedef ::testing::NiceMock<MockANetRPCChannelManager> NiceMockANetRPCChannelManager;

}

#endif //ISEARCH_BS_MOCKANETRPCCHANNELMANAGER_H
