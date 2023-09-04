#include "swift/auth/TopicAclDataSyncher.h"

#include <functional>
#include <map>
#include <ostream>
#include <string>
#include <unistd.h>

#include "aios/apps/facility/cm2/cm_basic/util/zk_wrapper.h"
#include "autil/result/Result.h"
#include "fslib/fs/zookeeper/test/ZkServer.h"
#include "fslib/util/FileUtil.h"
#include "swift/common/Common.h"
#include "swift/common/PathDefine.h"
#include "swift/protocol/AdminRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace swift::protocol;
using namespace swift::common;

namespace swift {
namespace auth {

class TopicAclDataSyncherTest : public TESTBASE {
public:
    void setUp();
    void tearDown();

protected:
    zookeeper::ZkServer *_zkServer = nullptr;
    string _zkRoot;
};

void TopicAclDataSyncherTest::setUp() {
    _zkServer = zookeeper::ZkServer::getZkServer();
    std::ostringstream oss;
    oss << "127.0.0.1:" << _zkServer->port();
    _zkRoot = "zfs://" + oss.str() + "/";
}

void TopicAclDataSyncherTest::tearDown() { _zkServer->stop(); }

TEST_F(TopicAclDataSyncherTest, testInit) {
    {
        // zkWrapper exist, will not init
        TopicAclDataSyncher dataSyncher(nullptr);
        dataSyncher._zkWrapper = new cm_basic::ZkWrapper();
        ASSERT_TRUE(dataSyncher.init("not_exist_path", true, 0));
        EXPECT_EQ("", dataSyncher._zkPath);
    }
    {
        // zkRoot invalid init failed
        TopicAclDataSyncher dataSyncher(nullptr);
        ASSERT_FALSE(dataSyncher.init("not_exist_path", true, 0));
        EXPECT_EQ("", dataSyncher._zkPath);
    }
    {
        // deserialize success, access info file not exists
        map<string, TopicAclData> topicAclDataMap;
        function<void(const map<string, TopicAclData> &)> func1 =
            [&topicAclDataMap](const map<string, TopicAclData> &map) { topicAclDataMap = map; };
        TopicAclDataSyncher dataSyncher(func1);
        bool ret = dataSyncher.init(_zkRoot, false, 1234);
        EXPECT_TRUE(ret);
        EXPECT_EQ(0, topicAclDataMap.size());
        EXPECT_EQ(nullptr, dataSyncher._loopThread);
    }
    {
        // deserialize success, access info file not exists
        map<string, TopicAclData> topicAclDataMap;
        function<void(const map<string, TopicAclData> &)> func1 =
            [&topicAclDataMap](const map<string, TopicAclData> &map) { topicAclDataMap = map; };
        TopicAclDataSyncher dataSyncher(func1);
        bool ret = dataSyncher.init(_zkRoot, true, 1234);
        EXPECT_TRUE(ret);
        EXPECT_EQ(0, topicAclDataMap.size());
        EXPECT_NE(nullptr, dataSyncher._loopThread);
    }
}

TEST_F(TopicAclDataSyncherTest, testDoWatchSync) {
    map<string, TopicAclData> topicAclDataMap;
    function<void(const map<string, TopicAclData> &)> func1 = [&topicAclDataMap](const map<string, TopicAclData> &map) {
        topicAclDataMap = map;
    };
    TopicAclDataSyncher dataSyncherFollower(func1);
    ASSERT_TRUE(dataSyncherFollower.init(_zkRoot, true, 1000000000));
    EXPECT_EQ(0, topicAclDataMap.size());

    TopicAclDataSyncher trigger(nullptr);
    EXPECT_TRUE(trigger.init(_zkRoot, false, 1234));
    map<string, TopicAclData> tadMap;
    TopicAclData &tad1 = tadMap["test1"];
    tad1.set_topicname("test1");
    tad1.set_checkstatus(TOPIC_AUTH_CHECK_ON);
    auto ret = trigger.serialized(tadMap);
    ASSERT_TRUE(ret.is_ok());
    usleep(500 * 1000);
    EXPECT_EQ(1, topicAclDataMap.size());
    tadMap.clear();
    ret = trigger.serialized(tadMap);
    ASSERT_TRUE(ret.is_ok());
    usleep(500 * 1000);
    EXPECT_EQ(0, topicAclDataMap.size());
}

TEST_F(TopicAclDataSyncherTest, testDoBackupSync) {
    map<string, TopicAclData> topicAclDataMap;
    function<void(const map<string, TopicAclData> &)> func1 = [&topicAclDataMap](const map<string, TopicAclData> &map) {
        topicAclDataMap = map;
    };
    TopicAclDataSyncher dataSyncherFollower(func1);
    ASSERT_TRUE(dataSyncherFollower.init(_zkRoot, true, 1000000));
    dataSyncherFollower.unregisterSyncCallback();
    EXPECT_EQ(0, topicAclDataMap.size());

    TopicAclDataSyncher trigger(nullptr);
    EXPECT_TRUE(trigger.init(_zkRoot, false, 1234));
    map<string, TopicAclData> tadMap;
    TopicAclData &tad1 = tadMap["test1"];
    tad1.set_topicname("test1");
    tad1.set_checkstatus(TOPIC_AUTH_CHECK_ON);
    auto ret = trigger.serialized(tadMap);
    ASSERT_TRUE(ret.is_ok());
    sleep(3);
    EXPECT_EQ(1, topicAclDataMap.size());
    tadMap.clear();
    ret = trigger.serialized(tadMap);
    ASSERT_TRUE(ret.is_ok());
    sleep(3);
    EXPECT_EQ(0, topicAclDataMap.size());
}

TEST_F(TopicAclDataSyncherTest, testSerializeAndDeserialize) {
    {
        // test empty acl data map
        TopicAclDataSyncher dataSyncher(nullptr);
        EXPECT_TRUE(dataSyncher.init(_zkRoot, false, 1234));
        map<string, TopicAclData> tadMap;
        auto ret = dataSyncher.serialized(tadMap);
        ASSERT_TRUE(ret.is_ok());
        map<string, TopicAclData> topicAclDataMap;
        ret = dataSyncher.deserialized(topicAclDataMap);
        ASSERT_TRUE(ret.is_ok());
        EXPECT_EQ(0, topicAclDataMap.size());
    }
    {
        TopicAclDataSyncher dataSyncher(nullptr);
        EXPECT_TRUE(dataSyncher.init(_zkRoot, false, 1234));

        map<string, TopicAclData> tadMap;
        TopicAclData &tad1 = tadMap["test1"];
        tad1.set_topicname("test1");
        tad1.set_checkstatus(TOPIC_AUTH_CHECK_ON);
        TopicAccessInfo topicAccessInfo;
        topicAccessInfo.mutable_accessauthinfo()->set_accessid("id1");
        topicAccessInfo.mutable_accessauthinfo()->set_accesskey("key1");
        topicAccessInfo.set_accesspriority(ACCESS_PRIORITY_P4);
        topicAccessInfo.set_accesstype(TOPIC_ACCESS_READ_WRITE);
        *(tad1.add_topicaccessinfos()) = topicAccessInfo;

        TopicAclData &tad2 = tadMap["test2"];
        tad2.set_topicname("test2");
        tad2.set_checkstatus(TOPIC_AUTH_CHECK_OFF);
        TopicAccessInfo topicAccessInfo2;
        topicAccessInfo2.mutable_accessauthinfo()->set_accessid("id2");
        topicAccessInfo2.mutable_accessauthinfo()->set_accesskey("key2");
        topicAccessInfo2.set_accesspriority(ACCESS_PRIORITY_P3);
        topicAccessInfo2.set_accesstype(TOPIC_ACCESS_READ);
        *(tad2.add_topicaccessinfos()) = topicAccessInfo2;
        auto ret = dataSyncher.serialized(tadMap);
        ASSERT_TRUE(ret.is_ok());
        map<string, TopicAclData> topicAclDataMap;
        ret = dataSyncher.deserialized(topicAclDataMap);
        ASSERT_TRUE(ret.is_ok());

        EXPECT_EQ(2, topicAclDataMap.size());
        EXPECT_EQ("id1", topicAclDataMap["test1"].topicaccessinfos(0).accessauthinfo().accessid());
        EXPECT_EQ("key1", topicAclDataMap["test1"].topicaccessinfos(0).accessauthinfo().accesskey());
        EXPECT_EQ(ACCESS_PRIORITY_P4, topicAclDataMap["test1"].topicaccessinfos(0).accesspriority());
        EXPECT_EQ(TOPIC_ACCESS_READ_WRITE, topicAclDataMap["test1"].topicaccessinfos(0).accesstype());
        EXPECT_EQ(TOPIC_AUTH_CHECK_ON, topicAclDataMap["test1"].checkstatus());
        EXPECT_EQ("id2", topicAclDataMap["test2"].topicaccessinfos(0).accessauthinfo().accessid());
        EXPECT_EQ("key2", topicAclDataMap["test2"].topicaccessinfos(0).accessauthinfo().accesskey());
        EXPECT_EQ(ACCESS_PRIORITY_P3, topicAclDataMap["test2"].topicaccessinfos(0).accesspriority());
        EXPECT_EQ(TOPIC_ACCESS_READ, topicAclDataMap["test2"].topicaccessinfos(0).accesstype());
        EXPECT_EQ(TOPIC_AUTH_CHECK_OFF, topicAclDataMap["test2"].checkstatus());
    }
}

TEST_F(TopicAclDataSyncherTest, testDeserializeAbnormal) {
    string zkHost = fslib::util::FileUtil::getHostFromZkPath(_zkRoot);
    cm_basic::ZkWrapper zkWrapper(zkHost);
    ASSERT_TRUE(zkWrapper.open()) << "open zkRoot:" << zkHost << " failed ";

    TopicAclDataSyncher dataSyncher(nullptr);
    EXPECT_TRUE(dataSyncher.init(_zkRoot, false, 0));
    string topicAclDataFile = PathDefine::getTopicAclDataFile(PathDefine::getPathFromZkPath(_zkRoot));

    ASSERT_TRUE(zkWrapper.touch(topicAclDataFile, "", true));
    map<string, TopicAclData> topicAclDataMap;
    auto ret = dataSyncher.deserialized(topicAclDataMap);
    ASSERT_TRUE(ret.is_ok());

    ASSERT_TRUE(zkWrapper.touch(topicAclDataFile, "xxx", true));
    ret = dataSyncher.deserialized(topicAclDataMap);
    ASSERT_TRUE(ret.is_err());
    EXPECT_EQ("topic acl data is invalid [xxx], len [3]", ret.get_error().message());
    {
        TopicAclDataHeader header;
        header.version = 0;
        header.compress = 0;
        header.len = 4;
        header.checksum = TopicAclDataHeader::calcChecksum("1233");
        string content((char *)&header, sizeof(header));
        content += "1234";
        ASSERT_TRUE(zkWrapper.touch(topicAclDataFile, content, true));
        ret = dataSyncher.deserialized(topicAclDataMap);
        ASSERT_TRUE(ret.is_err());
        EXPECT_EQ("topic acl data checksum is invalid expect [2354313772], accual [300799483]",
                  ret.get_error().message());
    }
    {
        string aclData = "xxxaaxxxxxxxxxx";
        TopicAclDataHeader header;
        header.version = 0;
        header.compress = 0;
        header.topicCount = 1;
        header.len = aclData.size();
        header.checksum = TopicAclDataHeader::calcChecksum(aclData);
        string content((char *)&header, sizeof(header));
        content += aclData;
        ASSERT_TRUE(zkWrapper.touch(topicAclDataFile, content, true));
        ret = dataSyncher.deserialized(topicAclDataMap);
        ASSERT_TRUE(ret.is_err());
        EXPECT_EQ("deserialized topic count not equal, 1 vs 0", ret.get_error().message());
    }
}
}; // namespace auth
}; // namespace swift
