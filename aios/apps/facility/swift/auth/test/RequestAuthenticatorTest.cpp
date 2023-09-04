#include "swift/auth/RequestAuthenticator.h"

#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "autil/result/Result.h"
#include "fslib/fs/zookeeper/test/ZkServer.h"
#include "swift/auth/TopicAclDataManager.h"
#include "swift/auth/TopicAclDataSyncher.h"
#include "swift/auth/TopicAclRequestHandler.h"
#include "swift/config/AuthorizerInfo.h"
#include "swift/protocol/AdminRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "unittest/unittest.h"

using namespace std;
using namespace swift::protocol;

namespace swift {
namespace auth {

class RequestAuthenticatorTest : public TESTBASE {
public:
    void setUp();
    void tearDown();

protected:
    zookeeper::ZkServer *_zkServer = nullptr;
    string _zkRoot;
};

void RequestAuthenticatorTest::setUp() {
    _zkServer = zookeeper::ZkServer::getZkServer();
    std::ostringstream oss;
    oss << "127.0.0.1:" << _zkServer->port();
    _zkRoot = "zfs://" + oss.str() + "/";
    cout << "zkRoot: " << _zkRoot << endl;
}

void RequestAuthenticatorTest::tearDown() { _zkServer->stop(); }

TEST_F(RequestAuthenticatorTest, testCheckAuthorizerRequest) {
    config::AuthorizerInfo config;
    TopicAclDataManagerPtr dataManager(new TopicAclDataManager());
    TopicAclRequestHandlerPtr handler(new TopicAclRequestHandler(dataManager));
    RequestAuthenticator requestAuthenticator(config, handler);
    protocol::TopicAclData aclData;
    aclData.set_topicname("test1");
    aclData.set_checkstatus(protocol::TOPIC_AUTH_CHECK_ON);
    protocol::TopicAccessInfo *accessInfo = aclData.add_topicaccessinfos();
    accessInfo->mutable_accessauthinfo()->set_accessid("id");
    accessInfo->mutable_accessauthinfo()->set_accesskey("key");
    dataManager->_topicAclDataMap.insert(make_pair("test1", aclData));

    // authentication disable, all request allowed
    requestAuthenticator._config._enable = false;
    TopicInfoRequest request;
    request.set_topicname("test1");
    request.set_timeout(1248);
    EXPECT_TRUE(requestAuthenticator.checkSysAuthorizer(&request));

    // authentication enable, empty request refused
    requestAuthenticator._config._enable = true;
    requestAuthenticator._config._sysUsers["123456"] = "654321";
    requestAuthenticator._config._normalUsers["13579"] = "24680";
    EXPECT_FALSE(requestAuthenticator.checkSysAuthorizer(&request));

    // normal user can't checkSys
    AuthenticationInfo *info = request.mutable_authentication();
    info->set_username("13579");
    info->set_passwd("24680");
    EXPECT_TRUE(requestAuthenticator.checkAuthorizerAndTopicAcl(&request));
    EXPECT_TRUE(requestAuthenticator.checkAuthorizer(&request));
    EXPECT_FALSE(requestAuthenticator.checkSysAuthorizer(&request));

    // sys user can do everything
    info->set_username("123456");
    info->set_passwd("654321");
    EXPECT_TRUE(requestAuthenticator.checkAuthorizerAndTopicAcl(&request));
    EXPECT_TRUE(requestAuthenticator.checkAuthorizer(&request));
    EXPECT_TRUE(requestAuthenticator.checkSysAuthorizer(&request));

    // no authentication, refused
    info->clear_username();
    info->clear_passwd();
    EXPECT_FALSE(requestAuthenticator.checkAuthorizerAndTopicAcl(&request));
    EXPECT_FALSE(requestAuthenticator.checkAuthorizer(&request));
    EXPECT_FALSE(requestAuthenticator.checkSysAuthorizer(&request));

    // topic access can only access topic
    info->clear_username();
    info->clear_passwd();
    info->mutable_accessauthinfo()->set_accessid("id");
    info->mutable_accessauthinfo()->set_accesskey("key");
    EXPECT_TRUE(requestAuthenticator.checkAuthorizerAndTopicAcl(&request));
    EXPECT_FALSE(requestAuthenticator.checkAuthorizer(&request));
    EXPECT_FALSE(requestAuthenticator.checkSysAuthorizer(&request));
}

TEST_F(RequestAuthenticatorTest, testHandleTopicAclRequest) {
    config::AuthorizerInfo config;
    RequestAuthenticator requestAuthenticator(config, nullptr);
    TopicAclRequest request;
    TopicAclResponse response;
    request.set_accessop(ADD_TOPIC_ACCESS);
    request.set_topicname("test1");
    TopicAccessInfo *tai = request.mutable_topicaccessinfo();
    tai->mutable_accessauthinfo()->set_accessid("id1");
    tai->mutable_accessauthinfo()->set_accesskey("key1");
    tai->set_accesspriority(ACCESS_PRIORITY_P3);
    tai->set_accesstype(TOPIC_ACCESS_READ);

    // TopicAclRequestHandler == nullptr
    auto ret = requestAuthenticator.handleTopicAclRequest(&request, &response);
    ASSERT_TRUE(ret.is_err());
    EXPECT_EQ("topic acl request handler not initialized [test1]", ret.get_error().message());

    // TopicAclRequestHandler inited
    TopicAclDataManagerPtr dataManager(new TopicAclDataManager());
    dataManager->init(_zkRoot, false, 100000000);
    TopicAclRequestHandlerPtr handler(new TopicAclRequestHandler(dataManager));
    requestAuthenticator.updateTopicAclRequestHandler(handler);
    response.clear_errorinfo();
    ret = requestAuthenticator.handleTopicAclRequest(&request, &response);
    ASSERT_TRUE(ret.is_ok());
    EXPECT_EQ("ERROR_ADMIN_OPERATION_FAILED[topic not exists[test1]]", response.errorinfo().errmsg());
}

TEST_F(RequestAuthenticatorTest, testCreateTopicAclItems) {
    config::AuthorizerInfo config;
    TopicAclDataManagerPtr dataManager(new TopicAclDataManager());
    dataManager->init(_zkRoot, false, 100000000);
    TopicAclRequestHandlerPtr handler(new TopicAclRequestHandler(dataManager));
    RequestAuthenticator requestAuthenticator(config, handler);

    vector<string> topics{"aios", "aichenTech"};
    auto ret = requestAuthenticator.createTopicAclItems(topics);
    EXPECT_TRUE(ret.is_ok());
    EXPECT_EQ(2, dataManager->_topicAclDataMap.size());
    EXPECT_EQ("aios", dataManager->_topicAclDataMap["aios"].topicname());
    EXPECT_EQ("aichenTech", dataManager->_topicAclDataMap["aichenTech"].topicname());
}

TEST_F(RequestAuthenticatorTest, testDeleteTopicAclItems) {
    config::AuthorizerInfo config;
    TopicAclDataManagerPtr dataManager(new TopicAclDataManager());
    dataManager->init(_zkRoot, false, 100000000);
    TopicAclRequestHandlerPtr handler(new TopicAclRequestHandler(dataManager));
    RequestAuthenticator requestAuthenticator(config, handler);

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
    tad.set_topicname("test2");
    dataManager->_topicAclDataMap["test2"] = tad;
    tad.set_topicname("test3");
    dataManager->_topicAclDataMap["test3"] = tad;

    vector<string> topics{"test1", "test3", "test4"};
    auto ret = requestAuthenticator.deleteTopicAclItems(topics);
    EXPECT_TRUE(ret.is_ok());
    EXPECT_EQ(1, dataManager->_topicAclDataMap.size());
    EXPECT_EQ("test2", dataManager->_topicAclDataMap["test2"].topicname());
}

TEST_F(RequestAuthenticatorTest, testValidateTopicAclRequest) {
    config::AuthorizerInfo config;
    RequestAuthenticator requestAuthenticator(config, nullptr);

    // topicName, accessId, accessKey all-or-nothing
    std::function<void(TopicAccessOperator)> func1 = [&requestAuthenticator](TopicAccessOperator accessop) {
        autil::Result<bool> ret;
        TopicAclRequest request;
        request.set_accessop(accessop);
        request.set_topicname("test1");
        request.mutable_topicaccessinfo()->mutable_accessauthinfo()->set_accessid("id1");
        request.mutable_topicaccessinfo()->mutable_accessauthinfo()->set_accesskey("key1");
        ret = requestAuthenticator.validateTopicAclRequest(&request);
        ASSERT_TRUE(ret.is_ok());
        request.mutable_topicaccessinfo()->mutable_accessauthinfo()->clear_accessid();
        ret = requestAuthenticator.validateTopicAclRequest(&request);
        ASSERT_TRUE(ret.is_err());
        ASSERT_EQ("request has empty field, topicName [test1], accessid [], accesskey [key1]",
                  ret.get_error().message());
        request.mutable_topicaccessinfo()->mutable_accessauthinfo()->set_accessid("id1");
        request.mutable_topicaccessinfo()->mutable_accessauthinfo()->clear_accesskey();
        ret = requestAuthenticator.validateTopicAclRequest(&request);
        ASSERT_TRUE(ret.is_err());
        ASSERT_EQ("request has empty field, topicName [test1], accessid [id1], accesskey []",
                  ret.get_error().message());
        request.mutable_topicaccessinfo()->mutable_accessauthinfo()->set_accesskey("key1");
        request.clear_topicname();
        ret = requestAuthenticator.validateTopicAclRequest(&request);
        ASSERT_TRUE(ret.is_err());
        ASSERT_EQ("request has empty field, topicName [], accessid [id1], accesskey [key1]", ret.get_error().message());
    };

    func1(ADD_TOPIC_ACCESS);
    func1(UPDATE_TOPIC_ACCESS_KEY);

    // (topicName, accessId) necessary, accessKey whatever
    std::function<void(TopicAccessOperator)> func2 = [&requestAuthenticator](TopicAccessOperator accessop) {
        autil::Result<bool> ret;
        TopicAclRequest request;
        request.set_accessop(accessop);
        request.set_topicname("test1");
        request.mutable_topicaccessinfo()->mutable_accessauthinfo()->set_accessid("id1");
        ret = requestAuthenticator.validateTopicAclRequest(&request);
        ASSERT_TRUE(ret.is_ok());
        request.mutable_topicaccessinfo()->mutable_accessauthinfo()->set_accesskey("key1");
        ret = requestAuthenticator.validateTopicAclRequest(&request);
        ASSERT_TRUE(ret.is_ok());
        request.mutable_topicaccessinfo()->mutable_accessauthinfo()->clear_accessid();
        ret = requestAuthenticator.validateTopicAclRequest(&request);
        ASSERT_TRUE(ret.is_err());
        ASSERT_EQ("request has empty field, topicName [test1], accessid []", ret.get_error().message());
        request.mutable_topicaccessinfo()->mutable_accessauthinfo()->set_accessid("id1");
        request.clear_topicname();
        ret = requestAuthenticator.validateTopicAclRequest(&request);
        ASSERT_TRUE(ret.is_err());
        ASSERT_EQ("request has empty field, topicName [], accessid [id1]", ret.get_error().message());
    };

    func2(UPDATE_TOPIC_ACCESS);
    func2(UPDATE_TOPIC_ACCESS_TYPE);
    func2(UPDATE_TOPIC_ACCESS_PRIORITY);
    func2(DELETE_TOPIC_ACCESS);

    // topicName necessary, (accessId, accessKey) whatever
    std::function<void(TopicAccessOperator)> func3 = [&requestAuthenticator](TopicAccessOperator accessop) {
        autil::Result<bool> ret;
        TopicAclRequest request;
        request.set_accessop(accessop);
        request.set_topicname("test1");
        ret = requestAuthenticator.validateTopicAclRequest(&request);
        ASSERT_TRUE(ret.is_ok());
        request.mutable_topicaccessinfo()->mutable_accessauthinfo()->set_accessid("id1");
        ret = requestAuthenticator.validateTopicAclRequest(&request);
        ASSERT_TRUE(ret.is_ok());
        request.mutable_topicaccessinfo()->mutable_accessauthinfo()->set_accesskey("key1");
        ret = requestAuthenticator.validateTopicAclRequest(&request);
        ASSERT_TRUE(ret.is_ok());
        request.clear_topicname();
        ret = requestAuthenticator.validateTopicAclRequest(&request);
        ASSERT_TRUE(ret.is_err());
        ASSERT_EQ("request has empty field, topicName []", ret.get_error().message());
    };
    func3(UPDATE_TOPIC_AUTH_STATUS);
    func3(CLEAR_TOPIC_ACCESS);
    func3(LIST_ONE_TOPIC_ACCESS);

    // list all topic need no param
    TopicAclRequest request;
    request.set_accessop(LIST_ALL_TOPIC_ACCESS);
    auto ret = requestAuthenticator.validateTopicAclRequest(&request);
    ASSERT_TRUE(ret.is_ok());
}

TEST_F(RequestAuthenticatorTest, testCheckAuthorizerTopic) {
    config::AuthorizerInfo config;
    RequestAuthenticator requestAuthenticator(config, nullptr);
    map<string, string> userMap{{"111", "222"}, {"333", "444"}};
    EXPECT_TRUE(requestAuthenticator.checkAuthorizer("111", "222", userMap));
    EXPECT_TRUE(requestAuthenticator.checkAuthorizer("333", "444", userMap));
    EXPECT_FALSE(requestAuthenticator.checkAuthorizer("111", "444", userMap));
    EXPECT_FALSE(requestAuthenticator.checkAuthorizer("1234", "", userMap));
    EXPECT_FALSE(requestAuthenticator.checkAuthorizer("1234", "", {}));
}

TEST_F(RequestAuthenticatorTest, testCheckTopicAcl) {
    TopicAclDataManagerPtr dataManager(new TopicAclDataManager());
    dataManager->init(_zkRoot, false, 100000000);

    TopicAclRequestHandlerPtr handler;
    config::AuthorizerInfo config;
    RequestAuthenticator requestAuthenticator(config, handler);
    EXPECT_FALSE(requestAuthenticator.checkTopicAcl("test1", "id1", "key1", false)); // handler is nullptr
    requestAuthenticator._topicAclRequestHandler.reset(new TopicAclRequestHandler(dataManager));
    TopicAclData tad;
    tad.set_topicname("test1");
    tad.set_checkstatus(TOPIC_AUTH_CHECK_ON);
    TopicAccessInfo topicAccessInfo;
    topicAccessInfo.mutable_accessauthinfo()->set_accessid("id1");
    topicAccessInfo.mutable_accessauthinfo()->set_accesskey("key1");
    topicAccessInfo.set_accesspriority(ACCESS_PRIORITY_P4);
    topicAccessInfo.set_accesstype(TOPIC_ACCESS_READ);
    *(tad.add_topicaccessinfos()) = topicAccessInfo;
    dataManager->_topicAclDataMap["test1"] = tad;

    EXPECT_TRUE(requestAuthenticator.checkTopicAcl("test1", "id1", "key1", false));
    EXPECT_FALSE(requestAuthenticator.checkTopicAcl("test1", "id1", "key1", true));
}

TEST_F(RequestAuthenticatorTest, testGetTopicAccessInfo) {
    {
        config::AuthorizerInfo config;
        config.setEnable(true);
        RequestAuthenticator requestAuthenticator(config, nullptr);
        AuthenticationInfo auth;
        TopicAccessInfo tai = requestAuthenticator.getTopicAccessInfo("test1", auth);
        EXPECT_EQ(ACCESS_PRIORITY_P0, tai.accesspriority());
        EXPECT_EQ(TOPIC_ACCESS_READ_WRITE, tai.accesstype());
    }
    {
        config::AuthorizerInfo config;
        config.setEnable(false);
        RequestAuthenticator requestAuthenticator(config, nullptr);
        AuthenticationInfo auth;
        TopicAccessInfo tai = requestAuthenticator.getTopicAccessInfo("test1", auth);
        EXPECT_EQ(ACCESS_PRIORITY_P5, tai.accesspriority());
        EXPECT_EQ(TOPIC_ACCESS_NONE, tai.accesstype());
    }
    {
        TopicAclDataManagerPtr dataManager(new TopicAclDataManager());
        TopicAclRequestHandlerPtr handler(new TopicAclRequestHandler(dataManager));

        config::AuthorizerInfo config;
        RequestAuthenticator requestAuthenticator(config, handler);
        AuthenticationInfo auth;
        auth.mutable_accessauthinfo()->set_accessid("id1");
        auth.mutable_accessauthinfo()->set_accesskey("key1");
        TopicAccessInfo tai = requestAuthenticator.getTopicAccessInfo("test1", auth);
        EXPECT_EQ(ACCESS_PRIORITY_P5, tai.accesspriority());
        EXPECT_EQ(TOPIC_ACCESS_READ_WRITE, tai.accesstype());
    }
    {
        TopicAclDataManagerPtr dataManager(new TopicAclDataManager());
        TopicAclData &tad = dataManager->_topicAclDataMap["test1"];
        tad.set_checkstatus(TOPIC_AUTH_CHECK_ON);
        tad.set_topicname("test1");
        TopicAccessInfo *tai = tad.add_topicaccessinfos();
        tai->mutable_accessauthinfo()->set_accessid("id1");
        tai->mutable_accessauthinfo()->set_accesskey("key1");
        tai->set_accesstype(TOPIC_ACCESS_READ);
        tai->set_accesspriority(ACCESS_PRIORITY_P3);
        TopicAclRequestHandlerPtr handler(new TopicAclRequestHandler(dataManager));

        config::AuthorizerInfo config;
        RequestAuthenticator requestAuthenticator(config, handler);
        AuthenticationInfo auth;
        auth.mutable_accessauthinfo()->set_accessid("id1");
        auth.mutable_accessauthinfo()->set_accesskey("key1");
        TopicAccessInfo topicAccessInfo = requestAuthenticator.getTopicAccessInfo("test1", auth);
        EXPECT_EQ(ACCESS_PRIORITY_P3, topicAccessInfo.accesspriority());
        EXPECT_EQ(TOPIC_ACCESS_READ, topicAccessInfo.accesstype());
    }
}

} // namespace auth
} // namespace swift
