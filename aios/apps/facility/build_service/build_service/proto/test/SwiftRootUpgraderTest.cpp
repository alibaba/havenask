#include "build_service/proto/SwiftRootUpgrader.h"

#include <iosfwd>
#include <map>
#include <stdlib.h>
#include <string>
#include <vector>

#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/proto/DataDescription.h"
#include "build_service/proto/DataDescriptions.h"
#include "build_service/test/unittest.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;

namespace build_service { namespace proto {

class SwiftRootUpgraderTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp();
    void tearDown();
};

void SwiftRootUpgraderTest::setUp() {}

void SwiftRootUpgraderTest::tearDown() {}

TEST_F(SwiftRootUpgraderTest, testNeedUpgrade)
{
    {
        SwiftRootUpgrader upgrader;
        ;
        ASSERT_FALSE(upgrader.needUpgrade(1234));
    }

    setenv("upgrade_swift_root_from", "zfs://abcd/1234", 1);
    setenv("upgrade_swift_root_to", "http://efgh/5678", 1);

    {
        SwiftRootUpgrader upgrader;
        ;
        ASSERT_TRUE(upgrader.needUpgrade(6789));
    }

    setenv("upgrade_swift_root_target_generations", "1234,789,346", 1);
    {
        SwiftRootUpgrader upgrader;
        ;
        ASSERT_FALSE(upgrader.needUpgrade(6789));
        ASSERT_TRUE(upgrader.needUpgrade(789));
        ASSERT_TRUE(upgrader.needUpgrade(1234));
    }

    setenv("upgrade_swift_root_from", "zfs://abcd/1234#zfs://bd", 1);
    setenv("upgrade_swift_root_to", "http://efgh/5678", 1);
    {
        // ignore upgrade for size not equal
        SwiftRootUpgrader upgrader;
        ;
        ASSERT_FALSE(upgrader.needUpgrade(789));
        ASSERT_FALSE(upgrader.needUpgrade(1234));
    }

    setenv("upgrade_swift_root_from", "zfs://abcd#zfs://abcd/1234", 1);
    setenv("upgrade_swift_root_to", "http://efgh/5678#http://asdf", 1);
    {
        // ignore upgrade for root ambiguous
        SwiftRootUpgrader upgrader;
        ;
        ASSERT_FALSE(upgrader.needUpgrade(789));
        ASSERT_FALSE(upgrader.needUpgrade(1234));
    }

    unsetenv("upgrade_swift_root_target_generations");
    unsetenv("upgrade_swift_root_from");
    unsetenv("upgrade_swift_root_to");
}

TEST_F(SwiftRootUpgraderTest, testSimple)
{
    setenv("upgrade_swift_root_from", "zfs://abcd/1234", 1);
    setenv("upgrade_swift_root_to", "http://efgh/5678", 1);

    proto::DataDescriptions dataDescriptions;
    proto::DataDescription ds;

    ds["swift_root"] = "zfs://abcd/1234/os_data";
    ds["src"] = "file";
    dataDescriptions.push_back(ds);

    ds["src"] = "swift";
    dataDescriptions.push_back(ds);

    SwiftRootUpgrader upgrader;
    ;
    upgrader.upgrade(ds);
    ASSERT_EQ(string("http://efgh/5678/os_data"), ds["swift_root"]);

    string str = ToJsonString(dataDescriptions.toVector(), true);
    upgrader.upgrade(dataDescriptions);
    ASSERT_EQ(string("http://efgh/5678/os_data"), dataDescriptions[1]["swift_root"]);
    string expect = ToJsonString(dataDescriptions.toVector(), true);
    ASSERT_NE(expect, str);

    string res = upgrader.upgradeDataDescriptions(str);
    ASSERT_EQ(expect, res);

    unsetenv("upgrade_swift_root_from");
    unsetenv("upgrade_swift_root_to");
}

TEST_F(SwiftRootUpgraderTest, testCombinedRoot)
{
    setenv("upgrade_swift_root_from",
           "zfs://search-zk-ops-swift-et2.vip.tbsite.net:12181/opensearch_swift#zfs://"
           "search-zk-ops-swift-et2.vip.tbsite.net:12181/ops_online_zk_root/swift",
           1);
    setenv("upgrade_swift_root_to",
           "http://fs-proxy.vip.tbsite.net:3066/opensearch_ea119_swift#http://fs-proxy.vip.tbsite.net:3066/"
           "opensearch_new_ea119_swift",
           1);

    proto::DataDescriptions dataDescriptions;
    proto::DataDescription ds;

    ds["swift_root"] = "zfs://search-zk-ops-swift-et2.vip.tbsite.net:12181/opensearch_swift#zfs://"
                       "search-zk-ops-swift-et2.vip.tbsite.net:12181/ops_online_zk_root/swift#zfs://"
                       "search-zk-ops-swift-et2.vip.tbsite.net:12181/opensearch_swift#zfs://"
                       "search-zk-ops-swift-et2.vip.tbsite.net:12181/opensearch_swift#zfs://"
                       "search-zk-ops-swift-et2.vip.tbsite.net:12181/ops_online_zk_root/swift#zfs://"
                       "search-zk-ops-swift-et2.vip.tbsite.net:12181/ops_online_zk_root/swift#zfs://"
                       "search-zk-ops-swift-et2.vip.tbsite.net:12181/opensearch_swift";
    ds["src"] = "swift";
    dataDescriptions.push_back(ds);

    SwiftRootUpgrader upgrader;
    ;
    upgrader.upgrade(ds);

    string expect = "http://fs-proxy.vip.tbsite.net:3066/opensearch_ea119_swift#http://fs-proxy.vip.tbsite.net:3066/"
                    "opensearch_new_ea119_swift#http://fs-proxy.vip.tbsite.net:3066/opensearch_ea119_swift#http://"
                    "fs-proxy.vip.tbsite.net:3066/opensearch_ea119_swift#http://fs-proxy.vip.tbsite.net:3066/"
                    "opensearch_new_ea119_swift#http://fs-proxy.vip.tbsite.net:3066/opensearch_new_ea119_swift#http://"
                    "fs-proxy.vip.tbsite.net:3066/opensearch_ea119_swift";
    ASSERT_EQ(expect, ds["swift_root"]);
    unsetenv("upgrade_swift_root_from");
    unsetenv("upgrade_swift_root_to");
}

}} // namespace build_service::proto
