#ifndef ISEARCH_BS_MOCKPOOLEDCHANNELMANAGER_H
#define ISEARCH_BS_MOCKPOOLEDCHANNELMANAGER_H

#include "build_service/test/unittest.h"
#include "build_service/test/test.h"
#include "build_service/util/Log.h"
#include "build_service/util/PooledChannelManager.h"

namespace build_service {
namespace util {

class MockPooledChannelManager : public PooledChannelManager
{
public:
    MockPooledChannelManager() {}
    ~MockPooledChannelManager() {}

public:
    MOCK_METHOD1(getRpcChannel, RPCChannelPtr(const std::string&));
};

typedef ::testing::StrictMock<MockPooledChannelManager> StrictMockPooledChannelManager;
typedef ::testing::NiceMock<MockPooledChannelManager> NiceMockPooledChannelManager;

}
}

#endif //ISEARCH_BS_MOCKPOOLEDCHANNELMANAGER_H
