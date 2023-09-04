#include "swift/network/SwiftRpcChannelManager.h"

#include <cstddef>
#include <map>
#include <string>
#include <unistd.h>

#include "arpc/ANetRPCChannel.h"
#include "autil/TimeUtility.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace arpc;
using namespace autil;
namespace swift {
namespace network {

class SwiftRpcChannelManagerTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
};

void SwiftRpcChannelManagerTest::setUp() {}

void SwiftRpcChannelManagerTest::tearDown() {}

TEST_F(SwiftRpcChannelManagerTest, testClearUselessChannel) {
    SwiftRpcChannelManager manager;
    CacheChannelItem item1;
    item1.channel.reset(new ANetRPCChannel(NULL, NULL));
    item1.lastAccessTime = TimeUtility::currentTime() - 1000 * 1000;

    CacheChannelItem item2;
    item2.channel.reset(new ANetRPCChannel(NULL, NULL));
    item2.lastAccessTime = TimeUtility::currentTime() - 3000 * 1000;
    manager._channelCache["item1"] = item1;
    manager._channelCache["item2"] = item2;
    manager._reserveTime = 2000 * 1000;
    manager.clearUselessChannel();
    ASSERT_EQ(size_t(2), manager._channelCache.size());
    item1.channel.reset();
    item2.channel.reset();
    manager.clearUselessChannel();
    ASSERT_EQ(size_t(1), manager._channelCache.size());
    ASSERT_TRUE(manager._channelCache.find("item1") != manager._channelCache.end());
    usleep(1200000);
    manager.clearUselessChannel();
    ASSERT_EQ(size_t(0), manager._channelCache.size());
}

TEST_F(SwiftRpcChannelManagerTest, testChannelTimeout) {
    SwiftRpcChannelManager manager;
    CacheChannelItem item1;
    item1.channel.reset(new ANetRPCChannel(NULL, NULL));
    item1.lastAccessTime = TimeUtility::currentTime() - 1000 * 1000;
    item1.timeoutErrorCount = 0;
    CacheChannelItem item2;
    item2.channel.reset(new ANetRPCChannel(NULL, NULL));
    item2.lastAccessTime = TimeUtility::currentTime() - 3000 * 1000;
    item2.timeoutErrorCount = 0;
    manager._channelCache["item1"] = item1;
    manager._channelCache["item2"] = item2;

    manager.channelTimeout("a");
    ASSERT_EQ(size_t(2), manager._channelCache.size());

    manager.channelTimeout("item1");
    ASSERT_EQ(size_t(2), manager._channelCache.size());
    manager.channelTimeout("item1");
    ASSERT_EQ(size_t(2), manager._channelCache.size());
    manager.channelTimeout("item1");
    ASSERT_EQ(size_t(1), manager._channelCache.size());

    manager.channelTimeout("item2");
    ASSERT_EQ(size_t(1), manager._channelCache.size());
    manager.channelTimeout("item2");
    ASSERT_EQ(size_t(1), manager._channelCache.size());
    auto &itemCache2 = manager._channelCache["item2"];
    itemCache2.lastAccessTime = TimeUtility::currentTime() - 9000000;
    manager.channelTimeout("item2");
    ASSERT_EQ(size_t(1), manager._channelCache.size());
    itemCache2.lastAccessTime = TimeUtility::currentTime();
    manager.channelTimeout("item2");
    ASSERT_EQ(size_t(0), manager._channelCache.size());
}
}; // namespace network
}; // namespace swift
