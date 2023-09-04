#include "swift/auth/TopicAclRequestHandler.h"

#include <iostream>
#include <map>
#include <memory>
#include <string>

#include "autil/result/Result.h"
#include "fslib/fs/zookeeper/test/ZkServer.h"
#include "swift/auth/TopicAclDataManager.h"
#include "swift/auth/TopicAclDataSyncher.h"
#include "swift/protocol/AdminRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace swift::protocol;

namespace swift {
namespace auth {

class TopicAclRequestHandlerTest : public TESTBASE {
public:
    void setUp();
    void tearDown();

protected:
    zookeeper::ZkServer *_zkServer = nullptr;
    string _zkRoot;
};

void TopicAclRequestHandlerTest::setUp() {
    _zkServer = zookeeper::ZkServer::getZkServer();
    std::ostringstream oss;
    oss << "127.0.0.1:" << _zkServer->port();
    _zkRoot = "zfs://" + oss.str() + "/";
    cout << "zkRoot: " << _zkRoot << endl;
}

void TopicAclRequestHandlerTest::tearDown() { _zkServer->stop(); }

TEST_F(TopicAclRequestHandlerTest, testHandleRequest) {
    TopicAclDataManagerPtr dataManager(new TopicAclDataManager());
    dataManager->init(_zkRoot, false, 100000000);
    TopicAclRequestHandler handler(dataManager);
    EXPECT_EQ(0, dataManager->_topicAclDataMap.size());

    // prepare request
    TopicAclRequest request;
    TopicAclResponse response;
    request.set_topicname("test1");
    TopicAccessInfo *tai = request.mutable_topicaccessinfo();
    tai->mutable_accessauthinfo()->set_accessid("id1");
    tai->mutable_accessauthinfo()->set_accesskey("key1");
    tai->set_accesspriority(ACCESS_PRIORITY_P3);
    tai->set_accesstype(TOPIC_ACCESS_READ);
    {
        // case: ADD_TOPIC_ACCESS
        request.set_accessop(ADD_TOPIC_ACCESS);
        handler.handleRequest(&request, &response);
        EXPECT_EQ("ERROR_ADMIN_OPERATION_FAILED[topic not exists[test1]]", response.errorinfo().errmsg());
        EXPECT_EQ(0, dataManager->_topicAclDataMap.size());
    }
    // prepare topic acl data map
    TopicAclData tad;
    tad.set_topicname("test1");
    tad.set_checkstatus(TOPIC_AUTH_CHECK_ON);
    TopicAccessInfo topicAccessInfo;
    topicAccessInfo.mutable_accessauthinfo()->set_accessid("id1");
    topicAccessInfo.mutable_accessauthinfo()->set_accesskey("key0");
    topicAccessInfo.set_accesspriority(ACCESS_PRIORITY_P4);
    topicAccessInfo.set_accesstype(TOPIC_ACCESS_READ_WRITE);
    *(tad.add_topicaccessinfos()) = topicAccessInfo;
    dataManager->_topicAclDataMap["test1"] = tad;
    {
        // case: UPDATE_TOPIC_ACCESS
        response.clear_errorinfo();
        request.set_accessop(UPDATE_TOPIC_ACCESS);
        handler.handleRequest(&request, &response);
        EXPECT_EQ(ERROR_NONE, response.errorinfo().errcode());
        EXPECT_EQ("id1", dataManager->_topicAclDataMap["test1"].topicaccessinfos(0).accessauthinfo().accessid());
        EXPECT_EQ("key1", dataManager->_topicAclDataMap["test1"].topicaccessinfos(0).accessauthinfo().accesskey());
        EXPECT_EQ(ACCESS_PRIORITY_P3, dataManager->_topicAclDataMap["test1"].topicaccessinfos(0).accesspriority());
        EXPECT_EQ(TOPIC_ACCESS_READ, dataManager->_topicAclDataMap["test1"].topicaccessinfos(0).accesstype());
        EXPECT_EQ(TOPIC_AUTH_CHECK_ON, dataManager->_topicAclDataMap["test1"].checkstatus());
    }
    {
        // case: UPDATE_TOPIC_ACCESS_KEY
        tai->mutable_accessauthinfo()->set_accesskey("key2");
        response.clear_errorinfo();
        request.set_accessop(UPDATE_TOPIC_ACCESS_KEY);
        handler.handleRequest(&request, &response);
        EXPECT_EQ(ERROR_NONE, response.errorinfo().errcode());
        EXPECT_EQ("key2", dataManager->_topicAclDataMap["test1"].topicaccessinfos(0).accessauthinfo().accesskey());
    }
    {
        // case: UPDATE_TOPIC_ACCESS_PRIORITY
        tai->set_accesspriority(ACCESS_PRIORITY_P0);
        response.clear_errorinfo();
        request.set_accessop(UPDATE_TOPIC_ACCESS_PRIORITY);
        handler.handleRequest(&request, &response);
        EXPECT_EQ(ERROR_NONE, response.errorinfo().errcode());
        EXPECT_EQ(ACCESS_PRIORITY_P0, dataManager->_topicAclDataMap["test1"].topicaccessinfos(0).accesspriority());
    }
    {
        // case: UPDATE_TOPIC_ACCESS_TYPE
        tai->set_accesstype(TOPIC_ACCESS_NONE);
        response.clear_errorinfo();
        request.set_accessop(UPDATE_TOPIC_ACCESS_TYPE);
        handler.handleRequest(&request, &response);
        EXPECT_EQ(ERROR_NONE, response.errorinfo().errcode());
        EXPECT_EQ(TOPIC_ACCESS_NONE, dataManager->_topicAclDataMap["test1"].topicaccessinfos(0).accesstype());
    }
    {
        // case: DELETE_TOPIC_ACCESS
        response.clear_errorinfo();
        request.set_accessop(DELETE_TOPIC_ACCESS);
        handler.handleRequest(&request, &response);
        EXPECT_EQ(ERROR_NONE, response.errorinfo().errcode());
        EXPECT_EQ(0, dataManager->_topicAclDataMap["test1"].topicaccessinfos_size());
    }
    {
        // case: UPDATE_TOPIC_AUTH_STATUS
        response.clear_errorinfo();
        request.set_accessop(UPDATE_TOPIC_AUTH_STATUS);
        request.set_authcheckstatus(TOPIC_AUTH_CHECK_OFF);
        handler.handleRequest(&request, &response);
        EXPECT_EQ(ERROR_NONE, response.errorinfo().errcode());
        EXPECT_EQ(TOPIC_AUTH_CHECK_OFF, dataManager->_topicAclDataMap["test1"].checkstatus());
    }
    {
        // case: CLEAR_TOPIC_ACCESS
        dataManager->_topicAclDataMap["test1"] = tad;
        EXPECT_EQ(1, dataManager->_topicAclDataMap["test1"].topicaccessinfos_size());
        response.clear_errorinfo();
        request.set_accessop(CLEAR_TOPIC_ACCESS);
        handler.handleRequest(&request, &response);
        EXPECT_EQ(ERROR_NONE, response.errorinfo().errcode());
        EXPECT_EQ(0, dataManager->_topicAclDataMap["test1"].topicaccessinfos_size());
    }
    {
        // case: LIST_ONE_TOPIC_ACCESS
        dataManager->_topicAclDataMap["test1"] = tad;
        EXPECT_EQ(1, dataManager->_topicAclDataMap["test1"].topicaccessinfos_size());
        response.clear_errorinfo();
        response.clear_topicacldatas();
        request.set_accessop(LIST_ONE_TOPIC_ACCESS);
        handler.handleRequest(&request, &response);
        EXPECT_EQ(ERROR_NONE, response.errorinfo().errcode());
        EXPECT_EQ(1, response.topicacldatas().alltopicacldata_size());
        EXPECT_EQ("id1", response.topicacldatas().alltopicacldata(0).topicaccessinfos(0).accessauthinfo().accessid());
        EXPECT_EQ("key0", response.topicacldatas().alltopicacldata(0).topicaccessinfos(0).accessauthinfo().accesskey());
        EXPECT_EQ(ACCESS_PRIORITY_P4, response.topicacldatas().alltopicacldata(0).topicaccessinfos(0).accesspriority());
        EXPECT_EQ(TOPIC_ACCESS_READ_WRITE,
                  response.topicacldatas().alltopicacldata(0).topicaccessinfos(0).accesstype());
        EXPECT_EQ(TOPIC_AUTH_CHECK_ON, response.topicacldatas().alltopicacldata(0).checkstatus());
        EXPECT_EQ("test1", response.topicacldatas().alltopicacldata(0).topicname());
    }
    {
        // case: LIST_ALL_TOPIC_ACCESS
        tad.set_topicname("test2");
        dataManager->_topicAclDataMap["test2"] = tad;
        response.clear_errorinfo();
        response.clear_topicacldatas();
        request.set_accessop(LIST_ALL_TOPIC_ACCESS);
        handler.handleRequest(&request, &response);
        EXPECT_EQ(ERROR_NONE, response.errorinfo().errcode());
        EXPECT_EQ(2, response.topicacldatas().alltopicacldata_size());
        EXPECT_EQ("test1", response.topicacldatas().alltopicacldata(0).topicname());
        EXPECT_EQ("test2", response.topicacldatas().alltopicacldata(1).topicname());
    }
}

TEST_F(TopicAclRequestHandlerTest, testCheckTopicAcl) {
    TopicAclDataManagerPtr dataManager(new TopicAclDataManager());
    dataManager->init(_zkRoot, false, 100000000);
    TopicAclRequestHandler handler(dataManager);
    autil::Result<bool> ret;
    {
        // topic not exist
        ret = handler.checkTopicAcl("test1", "id1", "", false);
        ASSERT_TRUE(ret.is_ok());
    }

    TopicAclData tad;
    tad.set_topicname("test1");
    tad.set_checkstatus(TOPIC_AUTH_CHECK_OFF);
    dataManager->_topicAclDataMap["test1"] = tad;
    { // check auth off
        ret = handler.checkTopicAcl("test1", "id1", "", false);
        ASSERT_TRUE(ret.is_ok());
    }

    tad.set_checkstatus(TOPIC_AUTH_CHECK_ON);
    TopicAccessInfo topicAccessInfo;
    topicAccessInfo.mutable_accessauthinfo()->set_accessid("id1");
    topicAccessInfo.mutable_accessauthinfo()->set_accesskey("key1");
    topicAccessInfo.set_accesspriority(ACCESS_PRIORITY_P4);
    topicAccessInfo.set_accesstype(TOPIC_ACCESS_READ_WRITE);
    *(tad.add_topicaccessinfos()) = topicAccessInfo;
    TopicAccessInfo topicAccessInfo2;
    topicAccessInfo2.mutable_accessauthinfo()->set_accessid("id2");
    topicAccessInfo2.mutable_accessauthinfo()->set_accesskey("key2");
    topicAccessInfo2.set_accesspriority(ACCESS_PRIORITY_P4);
    topicAccessInfo2.set_accesstype(TOPIC_ACCESS_READ);
    *(tad.add_topicaccessinfos()) = topicAccessInfo2;
    TopicAccessInfo topicAccessInfo3;
    topicAccessInfo3.mutable_accessauthinfo()->set_accessid("id3");
    topicAccessInfo3.mutable_accessauthinfo()->set_accesskey("key3");
    topicAccessInfo3.set_accesspriority(ACCESS_PRIORITY_P4);
    topicAccessInfo3.set_accesstype(TOPIC_ACCESS_NONE);
    *(tad.add_topicaccessinfos()) = topicAccessInfo3;
    dataManager->_topicAclDataMap["test1"] = tad;

    {
        // accessAuthInfo empty
        ret = handler.checkTopicAcl("test1", "id1", "", false);
        ASSERT_TRUE(ret.is_err());
        EXPECT_EQ("topic access info has empty field, topic [test1], accessid [id1]", ret.get_error().message());
        ret = handler.checkTopicAcl("test1", "", "key1", false);
        ASSERT_TRUE(ret.is_err());
        EXPECT_EQ("topic access info has empty field, topic [test1], accessid []", ret.get_error().message());
    }
    {
        // check accessid and accesskey not equal
        ret = handler.checkTopicAcl("test1", "id2", "key1", false);
        ASSERT_TRUE(ret.is_err());
        EXPECT_EQ("check topic access failed, accesskey not equal, topic [test1], accessid [id2], accesskey [key1]",
                  ret.get_error().message());
    }
    EXPECT_TRUE(handler.checkTopicAcl("test1", "id1", "key1", false).is_ok());
    EXPECT_TRUE(handler.checkTopicAcl("test1", "id1", "key1", true).is_ok());
    EXPECT_TRUE(handler.checkTopicAcl("test1", "id2", "key2", false).is_ok());
    EXPECT_FALSE(handler.checkTopicAcl("test1", "id2", "key2", true).is_ok());
    EXPECT_FALSE(handler.checkTopicAcl("test1", "id3", "key3", false).is_ok());
    EXPECT_FALSE(handler.checkTopicAcl("test1", "id3", "key3", true).is_ok());
}

TEST_F(TopicAclRequestHandlerTest, testGetTopicAccessInfo) {
    {
        TopicAclDataManagerPtr dataManager(new TopicAclDataManager());
        TopicAclRequestHandler handler(dataManager);

        AccessAuthInfo authInfo;
        TopicAccessInfo tai;
        auto ret = handler.getTopicAccessInfo("test1", authInfo, tai);
        ASSERT_TRUE(ret.is_err());
        EXPECT_EQ("topic not exists[test1]", ret.get_error().message());
    }
    {
        TopicAclDataManagerPtr dataManager(new TopicAclDataManager());
        TopicAclData &tad = dataManager->_topicAclDataMap["test1"];
        tad.set_checkstatus(TOPIC_AUTH_CHECK_OFF);
        tad.set_topicname("test1");
        TopicAccessInfo *tai = tad.add_topicaccessinfos();
        tai->mutable_accessauthinfo()->set_accessid("id1");
        tai->mutable_accessauthinfo()->set_accesskey("key1");
        tai->set_accesstype(TOPIC_ACCESS_READ_WRITE);
        TopicAclRequestHandler handler(dataManager);

        AccessAuthInfo authInfo;
        authInfo.set_accessid("id1");
        authInfo.set_accesskey("key1");
        TopicAccessInfo accessInfo;
        auto ret = handler.getTopicAccessInfo("test1", authInfo, accessInfo);
        ASSERT_TRUE(ret.is_err());
        EXPECT_EQ("topic check status off", ret.get_error().message());
    }
    {
        TopicAclDataManagerPtr dataManager(new TopicAclDataManager());
        TopicAclData &tad = dataManager->_topicAclDataMap["test1"];
        tad.set_checkstatus(TOPIC_AUTH_CHECK_ON);
        tad.set_topicname("test1");
        TopicAccessInfo *tai = tad.add_topicaccessinfos();
        tai->mutable_accessauthinfo()->set_accessid("id1");
        tai->mutable_accessauthinfo()->set_accesskey("key1");
        tai->set_accesstype(TOPIC_ACCESS_READ_WRITE);
        TopicAclRequestHandler handler(dataManager);

        AccessAuthInfo authInfo;
        authInfo.set_accessid("id2");
        authInfo.set_accesskey("key1");
        TopicAccessInfo accessInfo;
        auto ret = handler.getTopicAccessInfo("test1", authInfo, accessInfo);
        ASSERT_TRUE(ret.is_ok());
        EXPECT_EQ(TOPIC_ACCESS_NONE, accessInfo.accesstype());

        authInfo.set_accessid("id1");
        authInfo.set_accesskey("key2");
        ret = handler.getTopicAccessInfo("test1", authInfo, accessInfo);
        ASSERT_TRUE(ret.is_ok());
        EXPECT_EQ(TOPIC_ACCESS_NONE, accessInfo.accesstype());

        authInfo.set_accessid("id1");
        authInfo.set_accesskey("key1");
        ret = handler.getTopicAccessInfo("test1", authInfo, accessInfo);
        ASSERT_TRUE(ret.is_ok());
        EXPECT_EQ(TOPIC_ACCESS_READ_WRITE, accessInfo.accesstype());
    }
}

}; // namespace auth
}; // namespace swift
