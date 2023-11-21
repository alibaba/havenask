#include "build_service/util/SwiftClientCreator.h"

#include <functional>
#include <iostream>
#include <map>
#include <string>

#include "autil/Span.h"
#include "build_service/test/unittest.h"
#include "build_service/util/test/FakeSwiftClientCreator.h"
#include "unittest/unittest.h"

using namespace std;
using namespace std::placeholders;
using namespace autil;
using namespace testing;

namespace build_service { namespace util {

class SwiftClientCreatorTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp() {}
    void tearDown() {}
};

TEST_F(SwiftClientCreatorTest, testInitConfigStr)
{
    string config1 = SwiftClientCreator::getInitConfigStr("zfs1", "");
    ASSERT_EQ("zkPath=zfs1", config1);

    string config2 = SwiftClientCreator::getInitConfigStr("zfs1", "ioThreadNum=3");
    ASSERT_EQ("zkPath=zfs1;ioThreadNum=3", config2);

    string config3 = SwiftClientCreator::getInitConfigStr("zfs1##zfs2", "");
    ASSERT_EQ("zkPath=zfs1##zkPath=zfs2", config3);

    string config4 = SwiftClientCreator::getInitConfigStr("zfs1##zfs2", "ioThreadNum=3");
    ASSERT_EQ("zkPath=zfs1;ioThreadNum=3##zkPath=zfs2;ioThreadNum=3", config4);
}

TEST_F(SwiftClientCreatorTest, testCreateSwiftClient)
{
    FakeSwiftClientCreator creator(false, false);
    string zk = "zfs://1111";
    string config = "clientIdentity=1";
    auto client1 = creator.createSwiftClient(zk, config);
    auto client2 = creator.createSwiftClient(zk, config);
    auto client3 = creator.createSwiftClient(zk, "");
    ASSERT_TRUE(client1 != nullptr);
    ASSERT_TRUE(client2 != nullptr);
    ASSERT_TRUE(client3 != nullptr);
    ASSERT_TRUE(client1 == client2);
    ASSERT_TRUE(client1 != client3);
    ASSERT_EQ(2, creator._swiftClientMap.size());
}

}} // namespace build_service::util
