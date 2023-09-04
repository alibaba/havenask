#include "swift/network/SwiftAdminAdapter.h"

#include <cstdint>
#include <fstream>
#include <map>
#include <memory>
#include <string>

#include "fslib/fs/FileSystem.h"
#include "swift/client/fake_client/FakeSwiftAdminClient.h"
#include "swift/network/SwiftAdminClient.h"
#include "swift/network/SwiftRpcChannelManager.h"
#include "swift/protocol/AdminRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "unittest/unittest.h"

using namespace std;
using namespace swift::protocol;
using namespace swift::client;
namespace swift {
namespace network {

class SwiftAdminAdapterTest : public TESTBASE {
public:
    void writeFile(const string &path, const string &content) { fslib::fs::FileSystem::writeFile(path, content); }
};

TEST_F(SwiftAdminAdapterTest, testLatelyErrorInterval) {
    SwiftAdminAdapter adminAdapter("", SwiftRpcChannelManagerPtr());
    {
        int64_t t1 = adminAdapter.getLatelyErrorInterval();
        ASSERT_TRUE(t1 >= adminAdapter._latelyErrorTimeMinInterval);
        ASSERT_TRUE(t1 <= adminAdapter._latelyErrorTimeMaxInterval);
    }
    {
        adminAdapter._latelyErrorTimeMinInterval = 1;
        adminAdapter._latelyErrorTimeMaxInterval = 1;
        int64_t t1 = adminAdapter.getLatelyErrorInterval();
        ASSERT_TRUE(1 == t1 || 2 == t1);
    }
}

TEST_F(SwiftAdminAdapterTest, testGetLatelyError) {
    SwiftAdminAdapter adminAdapter("", SwiftRpcChannelManagerPtr());
    adminAdapter._lastGeneralErrorCode = ERROR_NONE;
    ErrorCode ec = adminAdapter.getLatelyError(0);
    EXPECT_EQ(ERROR_NONE, ec);

    adminAdapter._lastGeneralErrorCode = ERROR_CLIENT_GET_ADMIN_INFO_FAILED;
    ec = adminAdapter.getLatelyError(5000);
    EXPECT_EQ(ERROR_CLIENT_GET_ADMIN_INFO_FAILED, ec);
    ec = adminAdapter.getLatelyError(11 * 1000 * 1000);
    EXPECT_EQ(ERROR_NONE, ec);

    adminAdapter.checkError(ERROR_CLIENT_GET_ADMIN_INFO_FAILED, (int64_t)3);
    EXPECT_EQ(ERROR_CLIENT_GET_ADMIN_INFO_FAILED, adminAdapter._lastGeneralErrorCode);
    EXPECT_EQ((int64_t)3, adminAdapter._lastGeneralErrorTime);
    adminAdapter.checkError(ERROR_UNKNOWN, (int64_t)5);
    EXPECT_EQ(ERROR_NONE, adminAdapter._lastGeneralErrorCode);
    EXPECT_EQ((int64_t)-1, adminAdapter._lastGeneralErrorTime);
}

TEST_F(SwiftAdminAdapterTest, testGetTopicInfo) {
    SwiftAdminAdapter adminAdapter("", SwiftRpcChannelManagerPtr());
    adminAdapter._lastGeneralErrorCode = ERROR_CLIENT_GET_ADMIN_INFO_FAILED;
    string address;
    TopicInfoResponse response;
    ErrorCode ec = adminAdapter.getTopicInfo("topic", response, 0, {});
    EXPECT_EQ(ERROR_CLIENT_GET_ADMIN_INFO_FAILED, ec);

    adminAdapter._lastGeneralErrorCode = ERROR_NONE;
    FakeSwiftAdminClient *adminClient = new FakeSwiftAdminClient;
    adminAdapter._adminClient.reset(adminClient);
    adminAdapter._readAdminClient = adminAdapter._adminClient;
    adminClient->setErrorCode(ERROR_UNKNOWN);
    ec = adminAdapter.getTopicInfo("topic", response, 0, {});
    EXPECT_EQ(ERROR_UNKNOWN, ec);
    EXPECT_EQ(ERROR_NONE, adminAdapter._lastGeneralErrorCode);
    EXPECT_EQ((int64_t)-1, adminAdapter._lastGeneralErrorTime);

    adminClient->setErrorCode(ERROR_CLIENT_GET_ADMIN_INFO_FAILED);
    ec = adminAdapter.getTopicInfo("topic", response, 0, {});
    EXPECT_EQ(ERROR_CLIENT_GET_ADMIN_INFO_FAILED, ec);
    EXPECT_EQ(ERROR_CLIENT_GET_ADMIN_INFO_FAILED, adminAdapter._lastGeneralErrorCode);
    EXPECT_TRUE(adminAdapter._lastGeneralErrorTime > 0);
}

TEST_F(SwiftAdminAdapterTest, testGetTopicInfoWithAccessAuthority) {
    SwiftAdminAdapter adminAdapter("", SwiftRpcChannelManagerPtr());
    adminAdapter._lastGeneralErrorCode = ERROR_NONE;
    FakeSwiftAdminClient *adminClient = new FakeSwiftAdminClient;
    adminClient->setAccessAuthInfo("xx", "xx1");
    adminAdapter._adminClient.reset(adminClient);
    adminAdapter._readAdminClient = adminAdapter._adminClient;
    {
        TopicInfoResponse response;
        ErrorCode ec = adminAdapter.getTopicInfo("topic", response, 0, {});
        EXPECT_EQ(ERROR_ADMIN_AUTHENTICATION_FAILED, ec);
    }
    {
        TopicInfoResponse response;
        AuthenticationInfo authInfo;
        authInfo.mutable_accessauthinfo()->set_accessid("xx");
        authInfo.mutable_accessauthinfo()->set_accesskey("xx1");
        ErrorCode ec = adminAdapter.getTopicInfo("topic", response, 0, authInfo);
        EXPECT_EQ(ERROR_ADMIN_TOPIC_NOT_EXISTED, ec);
    }
    {
        TopicInfoResponse response;
        AuthenticationInfo authInfo;
        authInfo.set_username("xx");
        authInfo.set_passwd("xx1");
        ErrorCode ec = adminAdapter.getTopicInfo("topic", response, 0, authInfo);
        EXPECT_EQ(ERROR_ADMIN_TOPIC_NOT_EXISTED, ec);
    }
    {
        TopicInfoResponse response;
        AuthenticationInfo authInfo;
        authInfo.set_username("xx1");
        authInfo.set_passwd("xx1");
        ErrorCode ec = adminAdapter.getTopicInfo("topic", response, 0, authInfo);
        EXPECT_EQ(ERROR_ADMIN_AUTHENTICATION_FAILED, ec);
    }
}

TEST_F(SwiftAdminAdapterTest, testGetTopicInfoFromLeader) {
    SwiftAdminAdapter adminAdapter("", SwiftRpcChannelManagerPtr());
    FakeSwiftAdminClient *adminClient = new FakeSwiftAdminClient;
    adminAdapter._adminClient.reset(adminClient);
    adminAdapter._readAdminClient = adminAdapter._adminClient;
    string topicName = "abc";
    TopicCreationRequest request;
    request.set_topicname(topicName);
    request.set_partitioncount(1);
    ErrorCode ec = adminAdapter.createTopic(request);
    EXPECT_EQ(ERROR_NONE, ec);
    TopicInfoResponse response;
    ec = adminAdapter.getTopicInfoWithLeader(topicName, response, true, {});
    EXPECT_EQ(ERROR_NONE, ec);
    ec = adminAdapter.getTopicInfoWithLeader(topicName, response, false, {});
    EXPECT_EQ(ERROR_NONE, ec);
    FakeSwiftAdminClient *adminClient2 = new FakeSwiftAdminClient;
    adminAdapter._readAdminClient.reset(adminClient2);
    ec = adminAdapter.getTopicInfoWithLeader(topicName, response, false, {});
    EXPECT_EQ(ERROR_ADMIN_TOPIC_NOT_EXISTED, ec);
}

TEST_F(SwiftAdminAdapterTest, testGetPartitionCountWithoutModifyTime) {
    SwiftAdminAdapter adminAdapter("", SwiftRpcChannelManagerPtr());
    FakeSwiftAdminClient *adminClient = new FakeSwiftAdminClient;
    adminAdapter._adminClient.reset(adminClient);
    adminAdapter._readAdminClient = adminAdapter._adminClient;
    string topicName = "abc";
    TopicCreationRequest request;
    request.set_topicname(topicName);
    request.set_partitioncount(1);
    ErrorCode ec = adminAdapter.createTopic(request);
    EXPECT_EQ(ERROR_NONE, ec);

    TopicInfoResponse response;
    uint32_t partCount, rangeCount;
    ec = adminAdapter.getPartitionCount(topicName, 1, partCount, rangeCount);
    EXPECT_EQ(ERROR_NONE, ec);
    EXPECT_EQ(1, partCount);
    EXPECT_EQ(0, rangeCount);
    EXPECT_EQ(2, adminClient->_getTopicInfoCount);
    ec = adminAdapter.getPartitionCount(topicName, 1, partCount, rangeCount); // hit cache, modify time
    EXPECT_EQ(ERROR_NONE, ec);
    EXPECT_EQ(1, partCount);
    EXPECT_EQ(0, rangeCount);
    EXPECT_EQ(3, adminClient->_getTopicInfoCount);
}

TEST_F(SwiftAdminAdapterTest, testGetPartitionCountWithModifyTime0) {
    SwiftAdminAdapter adminAdapter("", SwiftRpcChannelManagerPtr());
    FakeSwiftAdminClient *adminClient = new FakeSwiftAdminClient;
    adminClient->_topicModifyTime = 10;
    adminAdapter._adminClient.reset(adminClient);
    adminAdapter._readAdminClient = adminAdapter._adminClient;
    string topicName = "abc";
    TopicCreationRequest request;
    request.set_topicname(topicName);
    request.set_partitioncount(1);
    ErrorCode ec = adminAdapter.createTopic(request);
    EXPECT_EQ(ERROR_NONE, ec);

    TopicInfoResponse response;
    uint32_t partCount, rangeCount;
    ec = adminAdapter.getPartitionCount(topicName, 0, partCount, rangeCount);
    EXPECT_EQ(ERROR_NONE, ec);
    EXPECT_EQ(1, partCount);
    EXPECT_EQ(0, rangeCount);
    EXPECT_EQ(2, adminClient->_getTopicInfoCount);
    ec = adminAdapter.getPartitionCount(topicName, 1, partCount, rangeCount); // hit cache
    EXPECT_EQ(ERROR_NONE, ec);
    EXPECT_EQ(1, partCount);
    EXPECT_EQ(0, rangeCount);
    EXPECT_EQ(2, adminClient->_getTopicInfoCount);
}

TEST_F(SwiftAdminAdapterTest, testGetPartitionCountWithModifyTime1) {
    SwiftAdminAdapter adminAdapter("", SwiftRpcChannelManagerPtr());
    FakeSwiftAdminClient *adminClient = new FakeSwiftAdminClient;
    adminClient->_topicModifyTime = 10;
    adminAdapter._adminClient.reset(adminClient);
    adminAdapter._readAdminClient = adminAdapter._adminClient;
    string topicName = "abc";
    TopicCreationRequest request;
    request.set_topicname(topicName);
    request.set_partitioncount(1);
    ErrorCode ec = adminAdapter.createTopic(request);
    EXPECT_EQ(ERROR_NONE, ec);

    TopicInfoResponse response;
    uint32_t partCount, rangeCount;
    ec = adminAdapter.getPartitionCount(topicName, 1, partCount, rangeCount);
    EXPECT_EQ(ERROR_NONE, ec);
    EXPECT_EQ(1, partCount);
    EXPECT_EQ(0, rangeCount);
    EXPECT_EQ(1, adminClient->_getTopicInfoCount);
    ec = adminAdapter.getPartitionCount(topicName, 1, partCount, rangeCount); // hit cache
    EXPECT_EQ(ERROR_NONE, ec);
    EXPECT_EQ(1, partCount);
    EXPECT_EQ(0, rangeCount);
    EXPECT_EQ(1, adminClient->_getTopicInfoCount);
}

TEST_F(SwiftAdminAdapterTest, testGetPartitionCountWithModifyTime10) {
    SwiftAdminAdapter adminAdapter("", SwiftRpcChannelManagerPtr());
    FakeSwiftAdminClient *adminClient = new FakeSwiftAdminClient;
    adminClient->_topicModifyTime = 10;
    adminAdapter._adminClient.reset(adminClient);
    adminAdapter._readAdminClient = adminAdapter._adminClient;
    string topicName = "abc";
    TopicCreationRequest request;
    request.set_topicname(topicName);
    request.set_partitioncount(1);
    ErrorCode ec = adminAdapter.createTopic(request);
    EXPECT_EQ(ERROR_NONE, ec);

    TopicInfoResponse response;
    uint32_t partCount, rangeCount;
    ec = adminAdapter.getPartitionCount(topicName, 10, partCount, rangeCount);
    EXPECT_EQ(ERROR_NONE, ec);
    EXPECT_EQ(1, partCount);
    EXPECT_EQ(0, rangeCount);
    EXPECT_EQ(1, adminClient->_getTopicInfoCount);
    ec = adminAdapter.getPartitionCount(topicName, 10, partCount, rangeCount); // hit cache
    EXPECT_EQ(ERROR_NONE, ec);
    EXPECT_EQ(1, partCount);
    EXPECT_EQ(0, rangeCount);
    EXPECT_EQ(1, adminClient->_getTopicInfoCount);
    ec = adminAdapter.getPartitionCount(topicName, 11, partCount, rangeCount); // hit cache
    EXPECT_EQ(ERROR_NONE, ec);
    EXPECT_EQ(1, partCount);
    EXPECT_EQ(0, rangeCount);
    EXPECT_EQ(2, adminClient->_getTopicInfoCount);
}

TEST_F(SwiftAdminAdapterTest, testGetPartitionCountWithModifyTime11) {
    SwiftAdminAdapter adminAdapter("", SwiftRpcChannelManagerPtr());
    FakeSwiftAdminClient *adminClient = new FakeSwiftAdminClient;
    adminClient->_topicModifyTime = 10;
    adminAdapter._adminClient.reset(adminClient);
    adminAdapter._readAdminClient = adminAdapter._adminClient;
    string topicName = "abc";
    TopicCreationRequest request;
    request.set_topicname(topicName);
    request.set_partitioncount(1);
    ErrorCode ec = adminAdapter.createTopic(request);
    EXPECT_EQ(ERROR_NONE, ec);

    TopicInfoResponse response;
    uint32_t partCount, rangeCount;
    ec = adminAdapter.getPartitionCount(topicName, 11, partCount, rangeCount);
    EXPECT_EQ(ERROR_NONE, ec);
    EXPECT_EQ(1, partCount);
    EXPECT_EQ(0, rangeCount);
    EXPECT_EQ(2, adminClient->_getTopicInfoCount);
    ec = adminAdapter.getPartitionCount(topicName, 11, partCount, rangeCount); // hit cache
    EXPECT_EQ(ERROR_NONE, ec);
    EXPECT_EQ(1, partCount);
    EXPECT_EQ(0, rangeCount);
    EXPECT_EQ(3, adminClient->_getTopicInfoCount); // from admin
}

TEST_F(SwiftAdminAdapterTest, testCreateTopic) {
    SwiftAdminAdapter adminAdapter("", SwiftRpcChannelManagerPtr());
    adminAdapter._lastGeneralErrorCode = ERROR_CLIENT_GET_ADMIN_INFO_FAILED;
    TopicCreationRequest request;
    request.set_topicname("abc");
    request.set_partitioncount(1);
    ErrorCode ec = adminAdapter.createTopic(request);
    EXPECT_EQ(ERROR_CLIENT_GET_ADMIN_INFO_FAILED, ec);
    adminAdapter._lastGeneralErrorCode = ERROR_NONE;
    FakeSwiftAdminClient *adminClient = new FakeSwiftAdminClient;
    adminAdapter._adminClient.reset(adminClient);
    adminAdapter._readAdminClient = adminAdapter._adminClient;
    adminClient->setErrorCode(ERROR_UNKNOWN);
    ec = adminAdapter.createTopic(request);
    EXPECT_EQ(ERROR_UNKNOWN, ec);
    EXPECT_EQ(ERROR_NONE, adminAdapter._lastGeneralErrorCode);
    EXPECT_EQ((int64_t)-1, adminAdapter._lastGeneralErrorTime);

    adminClient->setErrorCode(ERROR_NONE);
    ec = adminAdapter.createTopic(request);
    EXPECT_EQ(ERROR_NONE, ec);

    ec = adminAdapter.createTopic(request);
    EXPECT_EQ(ERROR_ADMIN_TOPIC_HAS_EXISTED, ec);
}

TEST_F(SwiftAdminAdapterTest, testCreateTopicBatch) {
    SwiftAdminAdapter adminAdapter("", SwiftRpcChannelManagerPtr());
    adminAdapter._lastGeneralErrorCode = ERROR_CLIENT_GET_ADMIN_INFO_FAILED;
    TopicBatchCreationRequest request;
    TopicBatchCreationResponse response;
    TopicCreationRequest *meta = request.add_topicrequests();
    meta->set_topicname("1");
    meta->set_partitioncount(1);
    meta = request.add_topicrequests();
    meta->set_topicname("2");
    meta->set_partitioncount(2);
    ErrorCode ec = adminAdapter.createTopicBatch(request, response);
    EXPECT_EQ(ERROR_CLIENT_GET_ADMIN_INFO_FAILED, ec);
    EXPECT_EQ(ERROR_NONE, response.errorinfo().errcode());
    adminAdapter._lastGeneralErrorCode = ERROR_NONE;
    FakeSwiftAdminClient *adminClient = new FakeSwiftAdminClient;
    adminAdapter._adminClient.reset(adminClient);
    adminAdapter._readAdminClient = adminAdapter._adminClient;
    adminClient->setErrorCode(ERROR_UNKNOWN);
    ec = adminAdapter.createTopicBatch(request, response);
    EXPECT_EQ(ERROR_UNKNOWN, ec);
    EXPECT_EQ(ERROR_UNKNOWN, response.errorinfo().errcode());
    EXPECT_EQ(ERROR_NONE, adminAdapter._lastGeneralErrorCode);
    EXPECT_EQ((int64_t)-1, adminAdapter._lastGeneralErrorTime);

    adminClient->setErrorCode(ERROR_NONE);
    ec = adminAdapter.createTopicBatch(request, response);
    EXPECT_EQ(ERROR_NONE, ec);
    EXPECT_EQ(ERROR_NONE, response.errorinfo().errcode());

    ec = adminAdapter.createTopicBatch(request, response);
    EXPECT_EQ(ERROR_ADMIN_TOPIC_HAS_EXISTED, ec);
    EXPECT_EQ(ERROR_ADMIN_TOPIC_HAS_EXISTED, response.errorinfo().errcode());
    EXPECT_EQ("ERROR_ADMIN_TOPIC_HAS_EXISTED[1;2]", response.errorinfo().errmsg());
}

TEST_F(SwiftAdminAdapterTest, testDeleteTopic) {
    SwiftAdminAdapter adminAdapter("", SwiftRpcChannelManagerPtr());
    adminAdapter._lastGeneralErrorCode = ERROR_CLIENT_GET_ADMIN_INFO_FAILED;
    string topicName = "abc";
    ErrorCode ec = adminAdapter.deleteTopic(topicName);
    EXPECT_EQ(ERROR_CLIENT_GET_ADMIN_INFO_FAILED, ec);
    adminAdapter._lastGeneralErrorCode = ERROR_NONE;
    FakeSwiftAdminClient *adminClient = new FakeSwiftAdminClient;
    adminAdapter._adminClient.reset(adminClient);
    adminAdapter._readAdminClient = adminAdapter._adminClient;

    adminClient->setErrorCode(ERROR_UNKNOWN);
    ec = adminAdapter.deleteTopic(topicName);
    EXPECT_EQ(ERROR_UNKNOWN, ec);
    EXPECT_EQ(ERROR_NONE, adminAdapter._lastGeneralErrorCode);
    EXPECT_EQ((int64_t)-1, adminAdapter._lastGeneralErrorTime);

    adminClient->setErrorCode(ERROR_NONE);
    ec = adminAdapter.deleteTopic(topicName);
    EXPECT_EQ(ERROR_ADMIN_TOPIC_NOT_EXISTED, ec);

    TopicCreationRequest request;
    request.set_topicname(topicName);
    request.set_partitioncount(1);

    ec = adminAdapter.createTopic(request);
    EXPECT_EQ(ERROR_NONE, ec);

    ec = adminAdapter.deleteTopic(topicName);
    EXPECT_EQ(ERROR_NONE, ec);
    ec = adminAdapter.deleteTopic(topicName);
    EXPECT_EQ(ERROR_ADMIN_TOPIC_NOT_EXISTED, ec);
}

TEST_F(SwiftAdminAdapterTest, testDeleteTopicBatch) {
    SwiftAdminAdapter adminAdapter("", SwiftRpcChannelManagerPtr());
    adminAdapter._lastGeneralErrorCode = ERROR_CLIENT_GET_ADMIN_INFO_FAILED;
    TopicBatchDeletionRequest request;
    TopicBatchDeletionResponse response;
    request.add_topicnames("1");
    request.add_topicnames("2");
    ErrorCode ec = adminAdapter.deleteTopicBatch(request, response);
    EXPECT_EQ(ERROR_CLIENT_GET_ADMIN_INFO_FAILED, ec);
    EXPECT_EQ(ERROR_NONE, response.errorinfo().errcode());
    adminAdapter._lastGeneralErrorCode = ERROR_NONE;
    FakeSwiftAdminClient *adminClient = new FakeSwiftAdminClient;
    adminAdapter._adminClient.reset(adminClient);
    adminAdapter._readAdminClient = adminAdapter._adminClient;

    adminClient->setErrorCode(ERROR_UNKNOWN);
    ec = adminAdapter.deleteTopicBatch(request, response);
    EXPECT_EQ(ERROR_UNKNOWN, ec);
    EXPECT_EQ(ERROR_UNKNOWN, response.errorinfo().errcode());
    EXPECT_EQ(ERROR_NONE, adminAdapter._lastGeneralErrorCode);
    EXPECT_EQ((int64_t)-1, adminAdapter._lastGeneralErrorTime);

    adminClient->setErrorCode(ERROR_NONE);
    ec = adminAdapter.deleteTopicBatch(request, response);
    EXPECT_EQ(ERROR_ADMIN_TOPIC_NOT_EXISTED, response.errorinfo().errcode());
    EXPECT_EQ(ERROR_ADMIN_TOPIC_NOT_EXISTED, ec);

    TopicBatchCreationRequest addRequest;
    TopicBatchCreationResponse addResponse;
    TopicCreationRequest *meta = addRequest.add_topicrequests();
    meta->set_topicname("1");
    meta->set_partitioncount(1);
    meta = addRequest.add_topicrequests();
    meta->set_topicname("2");
    meta->set_partitioncount(2);
    ec = adminAdapter.createTopicBatch(addRequest, addResponse);
    EXPECT_EQ(ERROR_NONE, ec);

    ec = adminAdapter.deleteTopicBatch(request, response);
    EXPECT_EQ(ERROR_NONE, ec);
    EXPECT_EQ(ERROR_NONE, response.errorinfo().errcode());
    ec = adminAdapter.deleteTopicBatch(request, response);
    EXPECT_EQ(ERROR_ADMIN_TOPIC_NOT_EXISTED, ec);
    EXPECT_EQ(ERROR_ADMIN_TOPIC_NOT_EXISTED, response.errorinfo().errcode());
}

TEST_F(SwiftAdminAdapterTest, testCreateReadAdminClient) {
    { // use follower admin
        LeaderInfo leaderInfo;
        leaderInfo.set_address("addr1");
        leaderInfo.set_sysstop(false);
        leaderInfo.set_starttimesec(1);
        AdminInfo *adminInfo1 = leaderInfo.add_admininfos();
        adminInfo1->set_address("addr1");
        adminInfo1->set_isprimary(true);
        adminInfo1->set_isalive(true);
        AdminInfo *adminInfo2 = leaderInfo.add_admininfos();
        adminInfo2->set_address("addr2");
        adminInfo2->set_isprimary(false);
        adminInfo2->set_isalive(false);
        AdminInfo *adminInfo3 = leaderInfo.add_admininfos();
        adminInfo3->set_address("addr3");
        adminInfo3->set_isprimary(false);
        adminInfo3->set_isalive(true);
        string leaderInfoStr;
        string zkPath = GET_TEMPLATE_DATA_PATH() + "/leader1/admin/leader_info";
        leaderInfo.SerializeToString(&leaderInfoStr);
        writeFile(zkPath, leaderInfoStr);
        SwiftAdminAdapter adminAdapter(GET_TEMPLATE_DATA_PATH() + "/leader1", SwiftRpcChannelManagerPtr(), true);
        ASSERT_EQ(ERROR_NONE, adminAdapter.createAdminClient());
        ASSERT_TRUE(adminAdapter._adminClient);
        ASSERT_TRUE(adminAdapter._readAdminClient);
        ASSERT_TRUE(adminAdapter._readAdminClient != adminAdapter._adminClient);
        EXPECT_EQ("tcp:addr3", adminAdapter._readAdminClient->_address);
        EXPECT_EQ("tcp:addr1", adminAdapter._adminClient->_address);
    }
    {
        LeaderInfo leaderInfo;
        leaderInfo.set_address("addr1");
        leaderInfo.set_sysstop(false);
        leaderInfo.set_starttimesec(1);
        string leaderInfoStr;
        string zkPath = GET_TEMPLATE_DATA_PATH() + "/leader2/admin/leader_info";
        leaderInfo.SerializeToString(&leaderInfoStr);
        writeFile(zkPath, leaderInfoStr);
        SwiftAdminAdapter adminAdapter(GET_TEMPLATE_DATA_PATH() + "/leader2", SwiftRpcChannelManagerPtr());
        ASSERT_EQ(ERROR_NONE, adminAdapter.createAdminClient());
        ASSERT_TRUE(adminAdapter._adminClient);
        ASSERT_TRUE(adminAdapter._readAdminClient);
        ASSERT_TRUE(adminAdapter._readAdminClient == adminAdapter._adminClient);
        EXPECT_EQ("tcp:addr1", adminAdapter._readAdminClient->_address);
        EXPECT_EQ("tcp:addr1", adminAdapter._adminClient->_address);
    }
    {
        LeaderInfo leaderInfo;
        leaderInfo.set_address("addr1");
        leaderInfo.set_sysstop(false);
        leaderInfo.set_starttimesec(1);
        AdminInfo *adminInfo1 = leaderInfo.add_admininfos();
        adminInfo1->set_address("addr1");
        adminInfo1->set_isprimary(true);
        adminInfo1->set_isalive(true);
        AdminInfo *adminInfo2 = leaderInfo.add_admininfos();
        adminInfo2->set_address("addr2");
        adminInfo2->set_isprimary(false);
        adminInfo2->set_isalive(false);
        string leaderInfoStr;
        string zkPath = GET_TEMPLATE_DATA_PATH() + "/leader3/admin/leader_info";
        leaderInfo.SerializeToString(&leaderInfoStr);
        writeFile(zkPath, leaderInfoStr);
        SwiftAdminAdapter adminAdapter(GET_TEMPLATE_DATA_PATH() + "/leader3", SwiftRpcChannelManagerPtr(), true);
        ASSERT_EQ(ERROR_NONE, adminAdapter.createAdminClient());
        ASSERT_TRUE(adminAdapter._adminClient);
        ASSERT_TRUE(adminAdapter._readAdminClient);
        ASSERT_TRUE(adminAdapter._readAdminClient == adminAdapter._adminClient);
        EXPECT_EQ("tcp:addr1", adminAdapter._readAdminClient->_address);
        EXPECT_EQ("tcp:addr1", adminAdapter._adminClient->_address);
    }
    { // not use follow admin
        LeaderInfo leaderInfo;
        leaderInfo.set_address("addr1");
        leaderInfo.set_sysstop(false);
        leaderInfo.set_starttimesec(1);
        AdminInfo *adminInfo1 = leaderInfo.add_admininfos();
        adminInfo1->set_address("addr1");
        adminInfo1->set_isprimary(true);
        adminInfo1->set_isalive(true);
        AdminInfo *adminInfo2 = leaderInfo.add_admininfos();
        adminInfo2->set_address("addr2");
        adminInfo2->set_isprimary(false);
        adminInfo2->set_isalive(false);
        AdminInfo *adminInfo3 = leaderInfo.add_admininfos();
        adminInfo3->set_address("addr3");
        adminInfo3->set_isprimary(false);
        adminInfo3->set_isalive(true);
        string leaderInfoStr;
        string zkPath = GET_TEMPLATE_DATA_PATH() + "/leader1/admin/leader_info";
        leaderInfo.SerializeToString(&leaderInfoStr);
        writeFile(zkPath, leaderInfoStr);
        SwiftAdminAdapter adminAdapter(GET_TEMPLATE_DATA_PATH() + "/leader1", SwiftRpcChannelManagerPtr(), false);
        ASSERT_EQ(ERROR_NONE, adminAdapter.createAdminClient());
        ASSERT_TRUE(adminAdapter._adminClient);
        ASSERT_TRUE(adminAdapter._readAdminClient);
        ASSERT_TRUE(adminAdapter._readAdminClient == adminAdapter._adminClient);
        EXPECT_EQ("tcp:addr1", adminAdapter._readAdminClient->_address);
        EXPECT_EQ("tcp:addr1", adminAdapter._adminClient->_address);
    }
}

TEST_F(SwiftAdminAdapterTest, testGetLatelyErrorInterval) {
    SwiftAdminAdapter adminAdapter("", SwiftRpcChannelManagerPtr());
    EXPECT_TRUE(adminAdapter.getLatelyErrorInterval() >= 300 * 1000);
    EXPECT_TRUE(adminAdapter.getLatelyErrorInterval() <= 10 * 1000 * 1000);
}

TEST_F(SwiftAdminAdapterTest, testSealTopic) {
    SwiftAdminAdapter adminAdapter("", SwiftRpcChannelManagerPtr());
    adminAdapter._lastGeneralErrorCode = ERROR_CLIENT_GET_ADMIN_INFO_FAILED;
    TopicCreationRequest request;
    const string topicName = "abc";
    request.set_topicname(topicName);
    request.set_partitioncount(1);
    ErrorCode ec = adminAdapter.createTopic(request);
    EXPECT_EQ(ERROR_CLIENT_GET_ADMIN_INFO_FAILED, ec);
    adminAdapter._lastGeneralErrorCode = ERROR_NONE;
    FakeSwiftAdminClient *adminClient = new FakeSwiftAdminClient;
    adminAdapter._adminClient.reset(adminClient);
    adminAdapter._readAdminClient = adminAdapter._adminClient;

    ec = adminAdapter.sealTopic(topicName);
    EXPECT_EQ(ERROR_ADMIN_TOPIC_NOT_EXISTED, ec);

    adminClient->setErrorCode(ERROR_NONE);
    ec = adminAdapter.createTopic(request);
    EXPECT_EQ(ERROR_NONE, ec);
    EXPECT_FALSE(adminClient->_topicInfo[topicName].sealed());

    ec = adminAdapter.sealTopic(topicName);
    EXPECT_TRUE(adminClient->_topicInfo[topicName].sealed());
    EXPECT_EQ(ERROR_NONE, ec);

    adminClient->_rpcFail = true;
    ec = adminAdapter.sealTopic(topicName);
    EXPECT_EQ(ERROR_CLIENT_ARPC_ERROR, ec);
}

TEST_F(SwiftAdminAdapterTest, testChangePartCount) {
    SwiftAdminAdapter adminAdapter("", SwiftRpcChannelManagerPtr());
    adminAdapter._lastGeneralErrorCode = ERROR_CLIENT_GET_ADMIN_INFO_FAILED;
    TopicCreationRequest request;
    const string topicName = "abc";
    request.set_topicname(topicName);
    request.set_partitioncount(1);
    ErrorCode ec = adminAdapter.createTopic(request);
    EXPECT_EQ(ERROR_CLIENT_GET_ADMIN_INFO_FAILED, ec);
    adminAdapter._lastGeneralErrorCode = ERROR_NONE;
    adminAdapter._adminClient.reset(new FakeSwiftAdminClient);
    adminAdapter._readAdminClient = adminAdapter._adminClient;
    FakeSwiftAdminClient *adminClient = dynamic_cast<FakeSwiftAdminClient *>(adminAdapter._adminClient.get());

    ec = adminAdapter.changePartCount(topicName, 3);
    EXPECT_EQ(ERROR_ADMIN_TOPIC_NOT_EXISTED, ec);

    adminClient->setErrorCode(ERROR_NONE);
    ec = adminAdapter.createTopic(request);
    EXPECT_EQ(ERROR_NONE, ec);
    EXPECT_EQ(1, adminClient->_topicInfo[topicName].partitioncount());

    ec = adminAdapter.changePartCount(topicName, 3);
    EXPECT_EQ(ERROR_NONE, ec);
    EXPECT_EQ(3, adminClient->_topicInfo[topicName].partitioncount());

    adminClient->_rpcFail = true;
    ec = adminAdapter.changePartCount(topicName, 5);
    EXPECT_EQ(ERROR_CLIENT_ARPC_ERROR, ec);
}

TEST_F(SwiftAdminAdapterTest, testDoGetBrokerInfo) {
    SwiftAdminAdapter adminAdapter("", SwiftRpcChannelManagerPtr());
    FakeSwiftAdminClient *adminClient = new FakeSwiftAdminClient;
    adminAdapter._adminClient.reset(adminClient);
    adminAdapter._readAdminClient = adminAdapter._adminClient;

    TopicCreationRequest request;
    request.set_topicname("logic_topic");
    request.set_partitioncount(1);
    request.set_topictype(TOPIC_TYPE_LOGIC);
    ErrorCode ec = adminAdapter.createTopic(request);
    EXPECT_EQ(ERROR_NONE, ec);
    request.set_topicname("normal_topic");
    request.set_partitioncount(2);
    request.set_topictype(TOPIC_TYPE_NORMAL);
    ec = adminAdapter.createTopic(request);
    EXPECT_EQ(ERROR_NONE, ec);

    string brokerAddress;
    BrokerVersionInfo versionInfo;
    // 1. not exist topic
    ec = adminAdapter.doGetBrokerInfo("not_exist", 0, brokerAddress, versionInfo, {});
    EXPECT_EQ(ERROR_ADMIN_TOPIC_NOT_EXISTED, ec);

    // 2. logic topic not loaded in broker
    ec = adminAdapter.doGetBrokerInfo("logic_topic", 0, brokerAddress, versionInfo, {});
    EXPECT_EQ(ERROR_CLIENT_LOGIC_TOPIC_NOT_IN_BROKER, ec);

    // 3. part id illegal
    ec = adminAdapter.doGetBrokerInfo("normal_topic", 2, brokerAddress, versionInfo, {});
    EXPECT_EQ(ERROR_CLIENT_GET_BROKER_ADDRESS_FAILED, ec);

    // 4. part has no address
    ec = adminAdapter.doGetBrokerInfo("normal_topic", 0, brokerAddress, versionInfo, {});
    EXPECT_EQ(ERROR_CLIENT_GET_BROKER_ADDRESS_FAILED, ec);

    // 5. hit cache, no broker address
    adminClient->_needPartInfoAddress = true;
    ec = adminAdapter.doGetBrokerInfo("normal_topic", 0, brokerAddress, versionInfo, {});
    EXPECT_EQ(ERROR_CLIENT_GET_BROKER_ADDRESS_FAILED, ec);
    EXPECT_TRUE(brokerAddress.empty());
    EXPECT_TRUE(versionInfo.version().empty());

    // 6. doGetBrokerAddr
    adminAdapter._topicInfoCache.clear();
    adminClient->_needPartInfoAddress = true;
    ec = adminAdapter.doGetBrokerInfo("normal_topic", 0, brokerAddress, versionInfo, {});
    EXPECT_EQ(ERROR_NONE, ec);
    EXPECT_EQ("10.10.10.0:0000", brokerAddress);
    EXPECT_EQ("1", versionInfo.version());
    // 7. delete topic
    ec = adminAdapter.deleteTopic("normal_topic");
    EXPECT_EQ(ERROR_NONE, ec);
    // 8. from cache
    adminAdapter._refreshTime = 10 * 1000 * 1000;
    adminAdapter._latelyErrorTimeMinInterval = 10000000;
    ec = adminAdapter.doGetBrokerInfo("normal_topic", 0, brokerAddress, versionInfo, {});
    EXPECT_EQ(ERROR_NONE, ec);
    EXPECT_EQ("10.10.10.0:0000", brokerAddress);
    EXPECT_EQ("1", versionInfo.version());
    // 9. add error info, address not same, hit cache
    adminAdapter.addErrorInfoToCacheTopic("normal_topic", 0, "tcp:10.10.10.1:0000");
    ec = adminAdapter.doGetBrokerInfo("normal_topic", 0, brokerAddress, versionInfo, {});
    EXPECT_EQ(ERROR_NONE, ec);
    EXPECT_EQ("10.10.10.0:0000", brokerAddress);
    // 9.1 add error info, part id larger, hit cache
    adminAdapter.addErrorInfoToCacheTopic("normal_topic", 2, "tcp:10.10.10.1:0000");
    ec = adminAdapter.doGetBrokerInfo("normal_topic", 0, brokerAddress, versionInfo, {});
    EXPECT_EQ(ERROR_NONE, ec);
    EXPECT_EQ("10.10.10.0:0000", brokerAddress);

    // 10. add error info, address same, hit cache
    adminAdapter.addErrorInfoToCacheTopic("normal_topic", 0, "tcp:10.10.10.0:0000");
    ec = adminAdapter.doGetBrokerInfo("normal_topic", 0, brokerAddress, versionInfo, {});
    EXPECT_EQ(ERROR_NONE, ec);
    EXPECT_EQ("10.10.10.0:0000", brokerAddress);
    // 11. cache ignore
    adminAdapter._latelyErrorTimeMinInterval = 0;
    ec = adminAdapter.doGetBrokerInfo("normal_topic", 0, brokerAddress, versionInfo, {});
    EXPECT_EQ(ERROR_ADMIN_TOPIC_NOT_EXISTED, ec);
}

TEST_F(SwiftAdminAdapterTest, testModifyTopic) {
    SwiftAdminAdapter adminAdapter("", SwiftRpcChannelManagerPtr());
    FakeSwiftAdminClient *adminClient = new FakeSwiftAdminClient;
    adminAdapter._adminClient.reset(adminClient);
    adminAdapter._readAdminClient = adminAdapter._adminClient;
    string topicName = "abc";
    TopicCreationRequest request;
    request.set_topicname(topicName);
    request.set_partitioncount(1);
    ErrorCode ec = adminAdapter.createTopic(request);
    EXPECT_EQ(ERROR_NONE, ec);

    std::string dfsRoot = "path://dfsroot";
    request.set_topicname(topicName);
    request.set_dfsroot(dfsRoot);
    request.set_versioncontrol(true);
    protocol::TopicCreationResponse response2;
    ec = adminAdapter.modifyTopic(request, response2);
    EXPECT_EQ(ERROR_NONE, ec);
}
TEST_F(SwiftAdminAdapterTest, testUpdateWriterVersion) {
    SwiftAdminAdapter adminAdapter("", SwiftRpcChannelManagerPtr());
    adminAdapter._lastGeneralErrorCode = ERROR_CLIENT_GET_ADMIN_INFO_FAILED;
    protocol::TopicWriterVersionInfo writerVersionInfo;
    writerVersionInfo.set_topicname("abc");
    writerVersionInfo.set_majorversion(123);
    ErrorCode ec = adminAdapter.updateWriterVersion(writerVersionInfo);
    EXPECT_EQ(ERROR_CLIENT_GET_ADMIN_INFO_FAILED, ec);

    adminAdapter._lastGeneralErrorCode = ERROR_NONE;
    FakeSwiftAdminClient *adminClient = new FakeSwiftAdminClient;
    adminAdapter._adminClient.reset(adminClient);
    adminAdapter._readAdminClient = adminAdapter._adminClient;
    adminClient->setErrorCode(ERROR_UNKNOWN);
    ec = adminAdapter.updateWriterVersion(writerVersionInfo);
    EXPECT_EQ(ERROR_UNKNOWN, ec);
    EXPECT_EQ(ERROR_NONE, adminAdapter._lastGeneralErrorCode);
    EXPECT_EQ((int64_t)-1, adminAdapter._lastGeneralErrorTime);

    adminClient->setErrorCode(ERROR_NONE);
    ec = adminAdapter.updateWriterVersion(writerVersionInfo);
    EXPECT_EQ(ERROR_NONE, ec);
    EXPECT_EQ(123, adminClient->_topicWriterVersion["abc"]);

    adminClient->_rpcFail = true;
    ec = adminAdapter.updateWriterVersion(writerVersionInfo);
    EXPECT_EQ(ERROR_CLIENT_ARPC_ERROR, ec);
}

} // namespace network
} // namespace swift
