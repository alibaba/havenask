#include "swift/common/TopicWriterController.h"

#include <iostream>
#include <memory>
#include <string>

#include "aios/apps/facility/cm2/cm_basic/util/zk_wrapper.h"
#include "autil/CommonMacros.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/File.h"
#include "fslib/fs/FileSystem.h"
#include "fslib/fs/zookeeper/test/ZkServer.h"
#include "swift/protocol/AdminRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/util/TargetRecorder.h"
#include "swift/util/ZkDataAccessor.h"
#include "unittest/unittest.h"

using namespace std;
using namespace swift::util;
using namespace swift::protocol;
using namespace fslib;
using namespace fslib::fs;
namespace swift {
namespace common {

class TopicWriterControllerTest : public TESTBASE {
public:
    void setUp();
    void tearDown();

protected:
    cm_basic::ZkWrapper *_zkWrapper = nullptr;
    zookeeper::ZkServer *_zkServer = nullptr;
    string _zkRoot;
};

void TopicWriterControllerTest::setUp() {
    _zkServer = zookeeper::ZkServer::getZkServer();
    std::ostringstream oss;
    oss << "127.0.0.1:" << _zkServer->port();
    string zkPath = "zfs://" + oss.str() + "/";
    _zkWrapper = new cm_basic::ZkWrapper(oss.str(), 1000);
    ASSERT_TRUE(_zkWrapper->open());
    _zkRoot = zkPath + GET_CLASS_NAME();
    cout << "zkRoot: " << _zkRoot << endl;
    fs::FileSystem::mkDir(_zkRoot);
}

void TopicWriterControllerTest::tearDown() { DELETE_AND_SET_NULL(_zkWrapper); }

TEST_F(TopicWriterControllerTest, testValidateRequest) {
    ZkDataAccessorPtr zkAccessor(new ZkDataAccessor());
    TopicWriterController twc(zkAccessor, _zkRoot);

    UpdateWriterVersionRequest request;
    string errorMsg;
    // 1. no writer version
    EXPECT_FALSE(twc.validateRequest(&request, errorMsg));
    EXPECT_EQ("topic write version empty", errorMsg);

    // 2. no writer version & no topic name
    TopicWriterVersionInfo twvInfo;
    request.mutable_topicwriterversion()->set_majorversion(100);
    EXPECT_FALSE(twc.validateRequest(&request, errorMsg));
    EXPECT_EQ("write version empty", errorMsg);

    // 3. no topic name
    WriterVersion wVersion;
    *request.mutable_topicwriterversion()->add_writerversions() = wVersion;
    EXPECT_FALSE(twc.validateRequest(&request, errorMsg));
    EXPECT_EQ("topic name empty", errorMsg);

    // 4. success
    request.mutable_topicwriterversion()->set_topicname("test");
    errorMsg = "";
    EXPECT_TRUE(twc.validateRequest(&request, errorMsg));
    EXPECT_TRUE(errorMsg.empty());
}

TEST_F(TopicWriterControllerTest, testUpdateWriterVersion) {
    ZkDataAccessorPtr zkAccessor(new ZkDataAccessor());
    TopicWriterController twc(zkAccessor, _zkRoot);

    UpdateWriterVersionRequest request;
    UpdateWriterVersionResponse response;

    request.mutable_topicwriterversion()->set_topicname("test_topic");
    request.mutable_topicwriterversion()->set_majorversion(100);
    WriterVersion version;
    version.set_name("processor_2_200");
    version.set_version(200);
    *request.mutable_topicwriterversion()->add_writerversions() = version;

    // 1. single init fail
    twc.updateWriterVersion(&request, &response);
    EXPECT_EQ(ERROR_ADMIN_OPERATION_FAILED, response.errorinfo().errcode());

    // 2. success update
    ASSERT_TRUE(zkAccessor->init(_zkWrapper, _zkRoot));
    twc.updateWriterVersion(&request, &response);
    EXPECT_EQ(ERROR_NONE, response.errorinfo().errcode());

    // 3. major version invalid
    request.mutable_topicwriterversion()->set_majorversion(99);
    twc.updateWriterVersion(&request, &response);
    EXPECT_EQ(ERROR_CLIENT_INVALID_PARAMETERS, response.errorinfo().errcode());
}

} // namespace common
} // namespace swift
