#include "swift/auth/TopicAclDataManager.h"

#include <algorithm>
#include <iostream>
#include <map>
#include <string>
#include <vector>

#include "autil/result/Result.h"
#include "fslib/fs/zookeeper/test/ZkServer.h"
#include "swift/auth/TopicAclDataSyncher.h"
#include "swift/common/Common.h"
#include "swift/protocol/AdminRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace swift::protocol;
using namespace swift::common;

namespace swift {
namespace auth {

class TopicAclDataManagerTest : public TESTBASE {
public:
    void setUp();
    void tearDown();

protected:
    zookeeper::ZkServer *_zkServer = nullptr;
    string _zkRoot;
};

void TopicAclDataManagerTest::setUp() {
    _zkServer = zookeeper::ZkServer::getZkServer();
    std::ostringstream oss;
    oss << "127.0.0.1:" << _zkServer->port();
    _zkRoot = "zfs://" + oss.str() + "/";
    cout << "zkRoot: " << _zkRoot << endl;
}

void TopicAclDataManagerTest::tearDown() { _zkServer->stop(); }

TEST_F(TopicAclDataManagerTest, testInit) {
    {
        TopicAclDataManager dataManager;
        ASSERT_FALSE(dataManager.init("", false, 1234));
        EXPECT_FALSE(dataManager._readOnly);
    }
    {
        TopicAclDataManager dataManager;
        ASSERT_TRUE(dataManager.init(_zkRoot, false, 1234));
        EXPECT_FALSE(dataManager._readOnly);
    }
}

TEST_F(TopicAclDataManagerTest, testUpdateTopicAuthCheckStatus) {
    TopicAclDataManager dataManager;
    dataManager._readOnly = true;
    autil::Result<bool> ret = dataManager.updateTopicAuthCheckStatus("test1", TOPIC_AUTH_CHECK_ON);
    ASSERT_TRUE(ret.is_err());
    EXPECT_EQ("readOnly mode, can't update topic check status", ret.get_error().message());

    // prepare acl data map
    dataManager.init(_zkRoot, false, 1234);
    TopicAclData tad1;
    tad1.set_topicname("test1");
    tad1.set_checkstatus(TOPIC_AUTH_CHECK_ON);
    TopicAccessInfo topicAccessInfo;
    topicAccessInfo.mutable_accessauthinfo()->set_accessid("id1");
    topicAccessInfo.mutable_accessauthinfo()->set_accesskey("key1");
    topicAccessInfo.set_accesspriority(ACCESS_PRIORITY_P4);
    topicAccessInfo.set_accesstype(TOPIC_ACCESS_READ_WRITE);
    *(tad1.add_topicaccessinfos()) = topicAccessInfo;
    dataManager._topicAclDataMap["test1"] = tad1;

    // topic not exists
    ret = dataManager.updateTopicAuthCheckStatus("test2", TOPIC_AUTH_CHECK_OFF);
    ASSERT_TRUE(ret.is_err());
    EXPECT_EQ("topic not exists[test2]", ret.get_error().message());

    // topic exists
    EXPECT_EQ(TOPIC_AUTH_CHECK_ON, dataManager._topicAclDataMap["test1"].checkstatus());
    ret = dataManager.updateTopicAuthCheckStatus("test1", TOPIC_AUTH_CHECK_OFF);
    ASSERT_TRUE(ret.is_ok());
    EXPECT_EQ(TOPIC_AUTH_CHECK_OFF, dataManager._topicAclDataMap["test1"].checkstatus());
}

TEST_F(TopicAclDataManagerTest, testAddTopicAccess) {
    TopicAccessInfo tai;
    tai.mutable_accessauthinfo()->set_accessid("id0");
    tai.mutable_accessauthinfo()->set_accesskey("key0");
    tai.set_accesspriority(ACCESS_PRIORITY_P3);
    tai.set_accesstype(TOPIC_ACCESS_READ);
    TopicAclDataManager dataManager;
    dataManager._readOnly = true;
    autil::Result<bool> ret = dataManager.addTopicAccess("test1", tai);
    ASSERT_TRUE(ret.is_err());
    EXPECT_EQ("readOnly mode, can't add topic access", ret.get_error().message());

    // prepare acl data map
    dataManager.init(_zkRoot, false, 1234);
    TopicAclData tad1;
    tad1.set_topicname("test1");
    tad1.set_checkstatus(TOPIC_AUTH_CHECK_ON);
    TopicAccessInfo topicAccessInfo;
    topicAccessInfo.mutable_accessauthinfo()->set_accessid("id1");
    topicAccessInfo.mutable_accessauthinfo()->set_accesskey("key1");
    topicAccessInfo.set_accesspriority(ACCESS_PRIORITY_P4);
    topicAccessInfo.set_accesstype(TOPIC_ACCESS_READ_WRITE);
    *(tad1.add_topicaccessinfos()) = topicAccessInfo;
    dataManager._topicAclDataMap["test1"] = tad1;

    // topic not exists
    ret = dataManager.addTopicAccess("test2", tai);
    ASSERT_TRUE(ret.is_err());
    EXPECT_EQ("topic not exists[test2]", ret.get_error().message());

    // topic exists, accessid not exists
    EXPECT_EQ(1, dataManager._topicAclDataMap["test1"].topicaccessinfos_size());
    EXPECT_EQ("id1", dataManager._topicAclDataMap["test1"].topicaccessinfos(0).accessauthinfo().accessid());
    EXPECT_EQ("key1", dataManager._topicAclDataMap["test1"].topicaccessinfos(0).accessauthinfo().accesskey());
    EXPECT_EQ(ACCESS_PRIORITY_P4, dataManager._topicAclDataMap["test1"].topicaccessinfos(0).accesspriority());
    EXPECT_EQ(TOPIC_ACCESS_READ_WRITE, dataManager._topicAclDataMap["test1"].topicaccessinfos(0).accesstype());
    ret = dataManager.addTopicAccess("test1", tai);
    ASSERT_TRUE(ret.is_ok());
    EXPECT_EQ(2, dataManager._topicAclDataMap["test1"].topicaccessinfos_size());
    EXPECT_EQ("id0", dataManager._topicAclDataMap["test1"].topicaccessinfos(1).accessauthinfo().accessid());
    EXPECT_EQ("key0", dataManager._topicAclDataMap["test1"].topicaccessinfos(1).accessauthinfo().accesskey());
    EXPECT_EQ(ACCESS_PRIORITY_P3, dataManager._topicAclDataMap["test1"].topicaccessinfos(1).accesspriority());
    EXPECT_EQ(TOPIC_ACCESS_READ, dataManager._topicAclDataMap["test1"].topicaccessinfos(1).accesstype());

    // topic exists, accessid exists
    ret = dataManager.addTopicAccess("test1", tai);
    ASSERT_TRUE(ret.is_err());
    EXPECT_EQ("access id [id0] already in topic [test1]", ret.get_error().message());
}

TEST_F(TopicAclDataManagerTest, testUpdateTopicAccess) {
    TopicAccessInfo tai;
    tai.mutable_accessauthinfo()->set_accessid("id0");
    tai.mutable_accessauthinfo()->set_accesskey("key0");
    tai.set_accesspriority(ACCESS_PRIORITY_P3);
    tai.set_accesstype(TOPIC_ACCESS_READ);
    TopicAclDataManager dataManager;
    dataManager._readOnly = true;
    autil::Result<bool> ret = dataManager.updateTopicAccess("test1", tai);
    ASSERT_TRUE(ret.is_err());
    EXPECT_EQ("readOnly mode, can't update topic access", ret.get_error().message());

    // prepare acl data map
    dataManager.init(_zkRoot, false, 1234);
    TopicAclData tad1;
    tad1.set_topicname("test1");
    tad1.set_checkstatus(TOPIC_AUTH_CHECK_ON);
    TopicAccessInfo topicAccessInfo;
    topicAccessInfo.mutable_accessauthinfo()->set_accessid("id1");
    topicAccessInfo.mutable_accessauthinfo()->set_accesskey("key1");
    topicAccessInfo.set_accesspriority(ACCESS_PRIORITY_P4);
    topicAccessInfo.set_accesstype(TOPIC_ACCESS_READ_WRITE);
    *(tad1.add_topicaccessinfos()) = topicAccessInfo;
    dataManager._topicAclDataMap["test1"] = tad1;

    // topic not exists
    ret = dataManager.updateTopicAccess("test2", tai);
    ASSERT_TRUE(ret.is_err());
    EXPECT_EQ("topic not exists[test2]", ret.get_error().message());

    // topic exists, accessid not exists
    ret = dataManager.updateTopicAccess("test1", tai);
    ASSERT_TRUE(ret.is_err());
    EXPECT_EQ("topic accessid not exist, update failed [test1, id0]", ret.get_error().message());

    // topic exists, accessid exists
    tai.mutable_accessauthinfo()->set_accessid("id1");
    EXPECT_EQ(1, dataManager._topicAclDataMap["test1"].topicaccessinfos_size());
    EXPECT_EQ("id1", dataManager._topicAclDataMap["test1"].topicaccessinfos(0).accessauthinfo().accessid());
    EXPECT_EQ("key1", dataManager._topicAclDataMap["test1"].topicaccessinfos(0).accessauthinfo().accesskey());
    EXPECT_EQ(ACCESS_PRIORITY_P4, dataManager._topicAclDataMap["test1"].topicaccessinfos(0).accesspriority());
    EXPECT_EQ(TOPIC_ACCESS_READ_WRITE, dataManager._topicAclDataMap["test1"].topicaccessinfos(0).accesstype());
    ret = dataManager.updateTopicAccess("test1", tai);
    ASSERT_TRUE(ret.is_ok());
    EXPECT_EQ(1, dataManager._topicAclDataMap["test1"].topicaccessinfos_size());
    EXPECT_EQ("id1", dataManager._topicAclDataMap["test1"].topicaccessinfos(0).accessauthinfo().accessid());
    EXPECT_EQ("key0", dataManager._topicAclDataMap["test1"].topicaccessinfos(0).accessauthinfo().accesskey());
    EXPECT_EQ(ACCESS_PRIORITY_P3, dataManager._topicAclDataMap["test1"].topicaccessinfos(0).accesspriority());
    EXPECT_EQ(TOPIC_ACCESS_READ, dataManager._topicAclDataMap["test1"].topicaccessinfos(0).accesstype());
}

TEST_F(TopicAclDataManagerTest, testUpdateTopicAccessKey) {
    TopicAccessInfo tai;
    tai.mutable_accessauthinfo()->set_accessid("id0");
    tai.mutable_accessauthinfo()->set_accesskey("key0");
    TopicAclDataManager dataManager;
    dataManager._readOnly = true;
    autil::Result<bool> ret = dataManager.updateTopicAccessKey("test1", tai);
    ASSERT_TRUE(ret.is_err());
    EXPECT_EQ("readOnly mode, can't update topic access", ret.get_error().message());

    // prepare acl data map
    dataManager.init(_zkRoot, false, 1234);
    TopicAclData tad1;
    tad1.set_topicname("test1");
    tad1.set_checkstatus(TOPIC_AUTH_CHECK_ON);
    TopicAccessInfo topicAccessInfo;
    topicAccessInfo.mutable_accessauthinfo()->set_accessid("id1");
    topicAccessInfo.mutable_accessauthinfo()->set_accesskey("key1");
    *(tad1.add_topicaccessinfos()) = topicAccessInfo;
    dataManager._topicAclDataMap["test1"] = tad1;

    // topic not exists
    ret = dataManager.updateTopicAccessKey("test2", tai);
    ASSERT_TRUE(ret.is_err());
    EXPECT_EQ("topic not exists[test2]", ret.get_error().message());

    // topic exists, accessid not exists
    ret = dataManager.updateTopicAccessKey("test1", tai);
    ASSERT_TRUE(ret.is_err());
    EXPECT_EQ("topic accessid not exist, update failed [test1, id0]", ret.get_error().message());

    // topic exists, accessid exists
    tai.mutable_accessauthinfo()->set_accessid("id1");
    EXPECT_EQ(1, dataManager._topicAclDataMap["test1"].topicaccessinfos_size());
    EXPECT_EQ("id1", dataManager._topicAclDataMap["test1"].topicaccessinfos(0).accessauthinfo().accessid());
    EXPECT_EQ("key1", dataManager._topicAclDataMap["test1"].topicaccessinfos(0).accessauthinfo().accesskey());
    ret = dataManager.updateTopicAccessKey("test1", tai);
    ASSERT_TRUE(ret.is_ok());
    EXPECT_EQ(1, dataManager._topicAclDataMap["test1"].topicaccessinfos_size());
    EXPECT_EQ("id1", dataManager._topicAclDataMap["test1"].topicaccessinfos(0).accessauthinfo().accessid());
    EXPECT_EQ("key0", dataManager._topicAclDataMap["test1"].topicaccessinfos(0).accessauthinfo().accesskey());
}

TEST_F(TopicAclDataManagerTest, testUpdateTopicAccessPriority) {
    TopicAccessInfo tai;
    tai.mutable_accessauthinfo()->set_accessid("id0");
    tai.set_accesspriority(ACCESS_PRIORITY_P3);
    TopicAclDataManager dataManager;
    dataManager._readOnly = true;
    autil::Result<bool> ret = dataManager.updateTopicAccessPriority("test1", tai);
    ASSERT_TRUE(ret.is_err());
    EXPECT_EQ("readOnly mode, can't update topic access", ret.get_error().message());

    // prepare acl data map
    dataManager.init(_zkRoot, false, 1234);
    TopicAclData tad1;
    tad1.set_topicname("test1");
    tad1.set_checkstatus(TOPIC_AUTH_CHECK_ON);
    TopicAccessInfo topicAccessInfo;
    topicAccessInfo.mutable_accessauthinfo()->set_accessid("id1");
    topicAccessInfo.set_accesspriority(ACCESS_PRIORITY_P4);
    *(tad1.add_topicaccessinfos()) = topicAccessInfo;
    dataManager._topicAclDataMap["test1"] = tad1;

    // topic not exists
    ret = dataManager.updateTopicAccessPriority("test2", tai);
    ASSERT_TRUE(ret.is_err());
    EXPECT_EQ("topic not exists[test2]", ret.get_error().message());

    // topic exists, accessid not exists
    ret = dataManager.updateTopicAccessPriority("test1", tai);
    ASSERT_TRUE(ret.is_err());
    EXPECT_EQ("topic accessid not exist, update failed [test1, id0]", ret.get_error().message());

    // topic exists, accessid exists
    tai.mutable_accessauthinfo()->set_accessid("id1");
    EXPECT_EQ(1, dataManager._topicAclDataMap["test1"].topicaccessinfos_size());
    EXPECT_EQ("id1", dataManager._topicAclDataMap["test1"].topicaccessinfos(0).accessauthinfo().accessid());
    EXPECT_EQ(ACCESS_PRIORITY_P4, dataManager._topicAclDataMap["test1"].topicaccessinfos(0).accesspriority());
    ret = dataManager.updateTopicAccessPriority("test1", tai);
    ASSERT_TRUE(ret.is_ok());
    EXPECT_EQ(1, dataManager._topicAclDataMap["test1"].topicaccessinfos_size());
    EXPECT_EQ("id1", dataManager._topicAclDataMap["test1"].topicaccessinfos(0).accessauthinfo().accessid());
    EXPECT_EQ(ACCESS_PRIORITY_P3, dataManager._topicAclDataMap["test1"].topicaccessinfos(0).accesspriority());
}

TEST_F(TopicAclDataManagerTest, testUpdateTopicAccessType) {
    TopicAccessInfo tai;
    tai.mutable_accessauthinfo()->set_accessid("id0");
    tai.set_accesstype(TOPIC_ACCESS_READ);
    TopicAclDataManager dataManager;
    dataManager._readOnly = true;
    autil::Result<bool> ret = dataManager.updateTopicAccessType("test1", tai);
    ASSERT_TRUE(ret.is_err());
    EXPECT_EQ("readOnly mode, can't update topic access", ret.get_error().message());

    // prepare acl data map
    dataManager.init(_zkRoot, false, 1234);
    TopicAclData tad1;
    tad1.set_topicname("test1");
    tad1.set_checkstatus(TOPIC_AUTH_CHECK_ON);
    TopicAccessInfo topicAccessInfo;
    topicAccessInfo.mutable_accessauthinfo()->set_accessid("id1");
    topicAccessInfo.set_accesstype(TOPIC_ACCESS_READ_WRITE);
    *(tad1.add_topicaccessinfos()) = topicAccessInfo;
    dataManager._topicAclDataMap["test1"] = tad1;

    // topic not exists
    ret = dataManager.updateTopicAccessType("test2", tai);
    ASSERT_TRUE(ret.is_err());
    EXPECT_EQ("topic not exists[test2]", ret.get_error().message());

    // topic exists, accessid not exists
    ret = dataManager.updateTopicAccessType("test1", tai);
    ASSERT_TRUE(ret.is_err());
    EXPECT_EQ("topic accessid not exist, update failed [test1, id0]", ret.get_error().message());

    // topic exists, accessid exists
    tai.mutable_accessauthinfo()->set_accessid("id1");
    EXPECT_EQ(1, dataManager._topicAclDataMap["test1"].topicaccessinfos_size());
    EXPECT_EQ("id1", dataManager._topicAclDataMap["test1"].topicaccessinfos(0).accessauthinfo().accessid());
    EXPECT_EQ(TOPIC_ACCESS_READ_WRITE, dataManager._topicAclDataMap["test1"].topicaccessinfos(0).accesstype());
    ret = dataManager.updateTopicAccessType("test1", tai);
    ASSERT_TRUE(ret.is_ok());
    EXPECT_EQ(1, dataManager._topicAclDataMap["test1"].topicaccessinfos_size());
    EXPECT_EQ("id1", dataManager._topicAclDataMap["test1"].topicaccessinfos(0).accessauthinfo().accessid());
    EXPECT_EQ(TOPIC_ACCESS_READ, dataManager._topicAclDataMap["test1"].topicaccessinfos(0).accesstype());
}

TEST_F(TopicAclDataManagerTest, testDeleteTopicAccess) {
    TopicAccessInfo tai;
    tai.mutable_accessauthinfo()->set_accessid("id0");
    TopicAclDataManager dataManager;
    dataManager._readOnly = true;
    autil::Result<bool> ret = dataManager.deleteTopicAccess("test1", tai);
    ASSERT_TRUE(ret.is_err());
    EXPECT_EQ("readOnly mode, can't delete topic access", ret.get_error().message());

    // prepare acl data map
    dataManager.init(_zkRoot, false, 1234);
    TopicAclData tad1;
    tad1.set_topicname("test1");
    tad1.set_checkstatus(TOPIC_AUTH_CHECK_ON);
    TopicAccessInfo topicAccessInfo;
    topicAccessInfo.mutable_accessauthinfo()->set_accessid("id1");
    topicAccessInfo.mutable_accessauthinfo()->set_accesskey("key1");
    topicAccessInfo.set_accesspriority(ACCESS_PRIORITY_P4);
    topicAccessInfo.set_accesstype(TOPIC_ACCESS_READ_WRITE);
    TopicAccessInfo topicAccessInfo2;
    topicAccessInfo2.mutable_accessauthinfo()->set_accessid("id2");
    topicAccessInfo2.mutable_accessauthinfo()->set_accesskey("key2");
    topicAccessInfo2.set_accesspriority(ACCESS_PRIORITY_P3);
    topicAccessInfo2.set_accesstype(TOPIC_ACCESS_READ);
    *(tad1.add_topicaccessinfos()) = topicAccessInfo;
    *(tad1.add_topicaccessinfos()) = topicAccessInfo2;
    dataManager._topicAclDataMap["test1"] = tad1;

    // topic not exists
    ret = dataManager.deleteTopicAccess("test2", tai);
    ASSERT_TRUE(ret.is_err());
    EXPECT_EQ("topic not exists[test2]", ret.get_error().message());

    // topic exists, accessid not exists, do nothing
    EXPECT_EQ(2, dataManager._topicAclDataMap["test1"].topicaccessinfos_size());
    ret = dataManager.deleteTopicAccess("test1", tai);
    ASSERT_TRUE(ret.is_ok());
    EXPECT_EQ(2, dataManager._topicAclDataMap["test1"].topicaccessinfos_size());

    // topic exists, accessid exists, delete it
    tai.mutable_accessauthinfo()->set_accessid("id1");
    ret = dataManager.deleteTopicAccess("test1", tai);
    ASSERT_TRUE(ret.is_ok());
    EXPECT_EQ(1, dataManager._topicAclDataMap["test1"].topicaccessinfos_size());
    EXPECT_EQ("id2", dataManager._topicAclDataMap["test1"].topicaccessinfos(0).accessauthinfo().accessid());
    EXPECT_EQ("key2", dataManager._topicAclDataMap["test1"].topicaccessinfos(0).accessauthinfo().accesskey());
    EXPECT_EQ(ACCESS_PRIORITY_P3, dataManager._topicAclDataMap["test1"].topicaccessinfos(0).accesspriority());
    EXPECT_EQ(TOPIC_ACCESS_READ, dataManager._topicAclDataMap["test1"].topicaccessinfos(0).accesstype());
}

TEST_F(TopicAclDataManagerTest, testClearTopicAccess) {
    TopicAclDataManager dataManager;
    dataManager._readOnly = true;
    autil::Result<bool> ret = dataManager.clearTopicAccess("test1");
    ASSERT_TRUE(ret.is_err());
    EXPECT_EQ("readOnly mode, can't clear topic access", ret.get_error().message());

    // prepare acl data map
    dataManager.init(_zkRoot, false, 1234);
    TopicAclData tad1;
    tad1.set_topicname("test1");
    tad1.set_checkstatus(TOPIC_AUTH_CHECK_ON);
    TopicAccessInfo topicAccessInfo;
    topicAccessInfo.mutable_accessauthinfo()->set_accessid("id1");
    topicAccessInfo.mutable_accessauthinfo()->set_accesskey("key1");
    topicAccessInfo.set_accesspriority(ACCESS_PRIORITY_P4);
    topicAccessInfo.set_accesstype(TOPIC_ACCESS_READ_WRITE);
    TopicAccessInfo topicAccessInfo2;
    topicAccessInfo2.mutable_accessauthinfo()->set_accessid("id2");
    topicAccessInfo2.mutable_accessauthinfo()->set_accesskey("key2");
    topicAccessInfo2.set_accesspriority(ACCESS_PRIORITY_P3);
    topicAccessInfo2.set_accesstype(TOPIC_ACCESS_READ);
    *(tad1.add_topicaccessinfos()) = topicAccessInfo;
    *(tad1.add_topicaccessinfos()) = topicAccessInfo2;
    dataManager._topicAclDataMap["test1"] = tad1;

    // topic not exists
    ret = dataManager.clearTopicAccess("test2");
    ASSERT_TRUE(ret.is_err());
    EXPECT_EQ("topic not exists[test2]", ret.get_error().message());

    // topic exists, clear all topic access
    EXPECT_EQ(2, dataManager._topicAclDataMap["test1"].topicaccessinfos_size());
    ret = dataManager.clearTopicAccess("test1");
    ASSERT_TRUE(ret.is_ok());
    EXPECT_EQ(0, dataManager._topicAclDataMap["test1"].topicaccessinfos_size());
}

TEST_F(TopicAclDataManagerTest, testGetTopicAclData) {
    // prepare acl data map
    TopicAclDataManager dataManager;
    TopicAclData tad1;
    tad1.set_topicname("test1");
    tad1.set_checkstatus(TOPIC_AUTH_CHECK_ON);
    TopicAccessInfo topicAccessInfo;
    topicAccessInfo.mutable_accessauthinfo()->set_accessid("id1");
    topicAccessInfo.mutable_accessauthinfo()->set_accesskey("key1");
    topicAccessInfo.set_accesspriority(ACCESS_PRIORITY_P4);
    topicAccessInfo.set_accesstype(TOPIC_ACCESS_READ_WRITE);
    TopicAccessInfo topicAccessInfo2;
    topicAccessInfo2.mutable_accessauthinfo()->set_accessid("id2");
    topicAccessInfo2.mutable_accessauthinfo()->set_accesskey("key2");
    topicAccessInfo2.set_accesspriority(ACCESS_PRIORITY_P3);
    topicAccessInfo2.set_accesstype(TOPIC_ACCESS_READ);
    *(tad1.add_topicaccessinfos()) = topicAccessInfo;
    *(tad1.add_topicaccessinfos()) = topicAccessInfo2;
    dataManager._topicAclDataMap["test1"] = tad1;

    // topic not exists
    {
        TopicAclData aclData;
        autil::Result<bool> ret = dataManager.getTopicAclData("test2", "id1", aclData);
        ASSERT_TRUE(ret.is_err());
        EXPECT_EQ("topic not exists[test2]", ret.get_error().message());
        EXPECT_EQ(0, aclData.topicaccessinfos_size());
    }
    // topic exists
    {
        // valid id1
        TopicAclData aclData;
        autil::Result<bool> ret = dataManager.getTopicAclData("test1", "id1", aclData);
        ASSERT_TRUE(ret.is_ok());
        EXPECT_EQ("test1", aclData.topicname());
        EXPECT_EQ(TOPIC_AUTH_CHECK_ON, aclData.checkstatus());
        EXPECT_EQ(1, aclData.topicaccessinfos_size());
        EXPECT_EQ("id1", aclData.topicaccessinfos(0).accessauthinfo().accessid());
        EXPECT_EQ("key1", aclData.topicaccessinfos(0).accessauthinfo().accesskey());
        EXPECT_EQ(ACCESS_PRIORITY_P4, aclData.topicaccessinfos(0).accesspriority());
        EXPECT_EQ(TOPIC_ACCESS_READ_WRITE, aclData.topicaccessinfos(0).accesstype());
    }
    {
        // valid id2
        TopicAclData aclData;
        autil::Result<bool> ret = dataManager.getTopicAclData("test1", "id2", aclData);
        ASSERT_TRUE(ret.is_ok());
        EXPECT_EQ("test1", aclData.topicname());
        EXPECT_EQ(TOPIC_AUTH_CHECK_ON, aclData.checkstatus());
        EXPECT_EQ(1, aclData.topicaccessinfos_size());
        EXPECT_EQ("id2", aclData.topicaccessinfos(0).accessauthinfo().accessid());
        EXPECT_EQ("key2", aclData.topicaccessinfos(0).accessauthinfo().accesskey());
        EXPECT_EQ(ACCESS_PRIORITY_P3, aclData.topicaccessinfos(0).accesspriority());
        EXPECT_EQ(TOPIC_ACCESS_READ, aclData.topicaccessinfos(0).accesstype());
    }
    {
        // invalid id3
        TopicAclData aclData;
        autil::Result<bool> ret = dataManager.getTopicAclData("test1", "id3", aclData);
        ASSERT_TRUE(ret.is_ok());
        EXPECT_EQ("test1", aclData.topicname());
        EXPECT_EQ(TOPIC_AUTH_CHECK_ON, aclData.checkstatus());
        EXPECT_EQ(0, aclData.topicaccessinfos_size());
    }
}

TEST_F(TopicAclDataManagerTest, testGetTopicAclDatas) {
    // prepare acl data map
    TopicAclDataManager dataManager;
    TopicAclData tad1;
    tad1.set_topicname("test1");
    tad1.set_checkstatus(TOPIC_AUTH_CHECK_ON);
    TopicAccessInfo topicAccessInfo;
    topicAccessInfo.mutable_accessauthinfo()->set_accessid("id1");
    topicAccessInfo.mutable_accessauthinfo()->set_accesskey("key1");
    topicAccessInfo.set_accesspriority(ACCESS_PRIORITY_P4);
    topicAccessInfo.set_accesstype(TOPIC_ACCESS_READ_WRITE);
    TopicAccessInfo topicAccessInfo2;
    topicAccessInfo2.mutable_accessauthinfo()->set_accessid("id2");
    topicAccessInfo2.mutable_accessauthinfo()->set_accesskey("key2");
    topicAccessInfo2.set_accesspriority(ACCESS_PRIORITY_P3);
    topicAccessInfo2.set_accesstype(TOPIC_ACCESS_READ);
    *(tad1.add_topicaccessinfos()) = topicAccessInfo;
    *(tad1.add_topicaccessinfos()) = topicAccessInfo2;
    dataManager._topicAclDataMap["test1"] = tad1;

    // topic not exists
    {
        TopicAclData aclData;
        autil::Result<bool> ret = dataManager.getTopicAclDatas("test2", aclData);
        ASSERT_TRUE(ret.is_err());
        EXPECT_EQ("topic not exists [test2]", ret.get_error().message());
    }
    // topic exists, return all accessids/keys
    {
        TopicAclData aclData;
        autil::Result<bool> ret = dataManager.getTopicAclDatas("test1", aclData);
        ASSERT_TRUE(ret.is_ok());
        EXPECT_EQ("test1", aclData.topicname());
        EXPECT_EQ(TOPIC_AUTH_CHECK_ON, aclData.checkstatus());
        EXPECT_EQ(2, aclData.topicaccessinfos_size());
        EXPECT_EQ("id1", aclData.topicaccessinfos(0).accessauthinfo().accessid());
        EXPECT_EQ("key1", aclData.topicaccessinfos(0).accessauthinfo().accesskey());
        EXPECT_EQ(ACCESS_PRIORITY_P4, aclData.topicaccessinfos(0).accesspriority());
        EXPECT_EQ(TOPIC_ACCESS_READ_WRITE, aclData.topicaccessinfos(0).accesstype());
        EXPECT_EQ("id2", aclData.topicaccessinfos(1).accessauthinfo().accessid());
        EXPECT_EQ("key2", aclData.topicaccessinfos(1).accessauthinfo().accesskey());
        EXPECT_EQ(ACCESS_PRIORITY_P3, aclData.topicaccessinfos(1).accesspriority());
        EXPECT_EQ(TOPIC_ACCESS_READ, aclData.topicaccessinfos(1).accesstype());
    }
}

TEST_F(TopicAclDataManagerTest, testCreateTopicAclItems) {
    vector<string> topics{"test1", "test2"};
    TopicAclDataManager dataManager;
    dataManager._readOnly = true;
    autil::Result<bool> ret = dataManager.createTopicAclItems(topics);
    ASSERT_TRUE(ret.is_err());
    EXPECT_EQ("readOnly mode, can't create topic access", ret.get_error().message());

    // create topic acl item successfully
    dataManager.init(_zkRoot, false, 1234);
    ret = dataManager.createTopicAclItems(topics);
    ASSERT_TRUE(ret.is_ok());
    EXPECT_EQ(2, dataManager._topicAclDataMap.size());
    EXPECT_EQ("test1", dataManager._topicAclDataMap["test1"].topicname());
    EXPECT_EQ("test2", dataManager._topicAclDataMap["test2"].topicname());
    EXPECT_EQ(TOPIC_AUTH_CHECK_OFF, dataManager._topicAclDataMap["test1"].checkstatus());
    EXPECT_EQ(TOPIC_AUTH_CHECK_OFF, dataManager._topicAclDataMap["test2"].checkstatus());

    // ignore existed topic test1&test2
    topics.push_back("test3");
    TopicAccessInfo topicAccessInfo;
    topicAccessInfo.mutable_accessauthinfo()->set_accessid("id1");
    topicAccessInfo.mutable_accessauthinfo()->set_accesskey("key1");
    topicAccessInfo.set_accesspriority(ACCESS_PRIORITY_P4);
    topicAccessInfo.set_accesstype(TOPIC_ACCESS_READ_WRITE);
    *(dataManager._topicAclDataMap["test1"].add_topicaccessinfos()) = topicAccessInfo;
    ret = dataManager.createTopicAclItems(topics);
    ASSERT_TRUE(ret.is_ok());
    EXPECT_EQ(3, dataManager._topicAclDataMap.size());
    EXPECT_EQ("test1", dataManager._topicAclDataMap["test1"].topicname());
    EXPECT_EQ("test2", dataManager._topicAclDataMap["test2"].topicname());
    EXPECT_EQ("test3", dataManager._topicAclDataMap["test3"].topicname());
    EXPECT_EQ(TOPIC_AUTH_CHECK_OFF, dataManager._topicAclDataMap["test1"].checkstatus());
    EXPECT_EQ(TOPIC_AUTH_CHECK_OFF, dataManager._topicAclDataMap["test2"].checkstatus());
    EXPECT_EQ(TOPIC_AUTH_CHECK_OFF, dataManager._topicAclDataMap["test3"].checkstatus());
    EXPECT_EQ(1, dataManager._topicAclDataMap["test1"].topicaccessinfos_size());
    EXPECT_EQ("id1", dataManager._topicAclDataMap["test1"].topicaccessinfos(0).accessauthinfo().accessid());
    EXPECT_EQ("key1", dataManager._topicAclDataMap["test1"].topicaccessinfos(0).accessauthinfo().accesskey());
    EXPECT_EQ(ACCESS_PRIORITY_P4, dataManager._topicAclDataMap["test1"].topicaccessinfos(0).accesspriority());
    EXPECT_EQ(TOPIC_ACCESS_READ_WRITE, dataManager._topicAclDataMap["test1"].topicaccessinfos(0).accesstype());
    EXPECT_EQ(0, dataManager._topicAclDataMap["test2"].topicaccessinfos_size());
    EXPECT_EQ(0, dataManager._topicAclDataMap["test3"].topicaccessinfos_size());
}

TEST_F(TopicAclDataManagerTest, testDeleteTopicAclItems) {
    vector<string> topics{"test1"};
    TopicAclDataManager dataManager;
    dataManager._readOnly = true;
    autil::Result<bool> ret = dataManager.deleteTopicAclItems(topics);
    ASSERT_TRUE(ret.is_err());
    EXPECT_EQ("readOnly mode, can't delete topic acl items", ret.get_error().message());

    // not exists topic doesn't matter
    dataManager.init(_zkRoot, false, 1234);
    ret = dataManager.deleteTopicAclItems(topics);
    ASSERT_TRUE(ret.is_ok());

    // prepare data
    TopicAclData &tad1 = dataManager._topicAclDataMap["test1"];
    tad1.set_topicname("test1");
    tad1.set_checkstatus(TOPIC_AUTH_CHECK_ON);
    TopicAccessInfo topicAccessInfo;
    topicAccessInfo.mutable_accessauthinfo()->set_accessid("id1");
    topicAccessInfo.mutable_accessauthinfo()->set_accesskey("key1");
    topicAccessInfo.set_accesspriority(ACCESS_PRIORITY_P4);
    topicAccessInfo.set_accesstype(TOPIC_ACCESS_READ_WRITE);
    *(tad1.add_topicaccessinfos()) = topicAccessInfo;
    TopicAclData &tad2 = dataManager._topicAclDataMap["test2"];
    tad2.set_topicname("test2");
    tad2.set_checkstatus(TOPIC_AUTH_CHECK_OFF);
    TopicAccessInfo topicAccessInfo2;
    topicAccessInfo2.mutable_accessauthinfo()->set_accessid("id2");
    topicAccessInfo2.mutable_accessauthinfo()->set_accesskey("key2");
    topicAccessInfo2.set_accesspriority(ACCESS_PRIORITY_P3);
    topicAccessInfo2.set_accesstype(TOPIC_ACCESS_READ);
    *(tad2.add_topicaccessinfos()) = topicAccessInfo2;

    // delete test1
    ret = dataManager.deleteTopicAclItems(topics);
    EXPECT_TRUE(ret.is_ok());
    EXPECT_EQ(1, dataManager._topicAclDataMap.size());
    EXPECT_EQ("test2", dataManager._topicAclDataMap["test2"].topicname());
    EXPECT_EQ(TOPIC_AUTH_CHECK_OFF, dataManager._topicAclDataMap["test2"].checkstatus());
    EXPECT_EQ(1, dataManager._topicAclDataMap["test2"].topicaccessinfos_size());
    EXPECT_EQ("id2", dataManager._topicAclDataMap["test2"].topicaccessinfos(0).accessauthinfo().accessid());
    EXPECT_EQ("key2", dataManager._topicAclDataMap["test2"].topicaccessinfos(0).accessauthinfo().accesskey());
    EXPECT_EQ(ACCESS_PRIORITY_P3, dataManager._topicAclDataMap["test2"].topicaccessinfos(0).accesspriority());
    EXPECT_EQ(TOPIC_ACCESS_READ, dataManager._topicAclDataMap["test2"].topicaccessinfos(0).accesstype());
}
}; // namespace auth
}; // namespace swift
