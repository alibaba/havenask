#include "swift/common/SingleTopicWriterController.h"

#include <cstdint>
#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>

#include "aios/apps/facility/cm2/cm_basic/util/zk_wrapper.h"
#include "autil/CommonMacros.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/File.h"
#include "fslib/fs/FileSystem.h"
#include "fslib/fs/zookeeper/test/ZkServer.h"
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

class SingleTopicWriterControllerTest : public TESTBASE {
public:
    void setUp();
    void tearDown();

protected:
    cm_basic::ZkWrapper *_zkWrapper = nullptr;
    zookeeper::ZkServer *_zkServer = nullptr;
    string _zkRoot;
};

void SingleTopicWriterControllerTest::setUp() {
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

void SingleTopicWriterControllerTest::tearDown() { DELETE_AND_SET_NULL(_zkWrapper); }

TEST_F(SingleTopicWriterControllerTest, testInit) {
    ZkDataAccessorPtr zkAccessor(new ZkDataAccessor());
    SingleTopicWriterController stwc(zkAccessor, _zkRoot, "test_topic");

    // 1. check exist fail
    EXPECT_FALSE(stwc.init());

    // 2. file not exist
    ASSERT_TRUE(zkAccessor->init(_zkWrapper, _zkRoot));
    EXPECT_TRUE(stwc.init());

    // 3. file exist but empty
    ASSERT_TRUE(zkAccessor->writeFile(stwc._path, ""));
    EXPECT_TRUE(stwc.init());

    // 4. file invalid
    ASSERT_TRUE(zkAccessor->writeFile(stwc._path, "aaa"));
    EXPECT_FALSE(stwc.init());

    // 5. file valid
    TopicWriterVersionInfo versionInfo;
    versionInfo.set_topicname("test_topic");
    versionInfo.set_majorversion(100);
    WriterVersion version;
    version.set_name("processor_1_100");
    version.set_version(200);
    *versionInfo.add_writerversions() = version;
    version.set_name("processor_3_300");
    version.set_version(300);
    *versionInfo.add_writerversions() = version;
    ASSERT_TRUE(zkAccessor->writeZk(stwc._path, versionInfo, false, false));
    EXPECT_TRUE(stwc.init());
    EXPECT_EQ(2, stwc._writerVersion.size());
    EXPECT_EQ(100, stwc._majorVersion);
    EXPECT_EQ(200, stwc._writerVersion["processor_1_100"]);
    EXPECT_EQ(300, stwc._writerVersion["processor_3_300"]);
}

TEST_F(SingleTopicWriterControllerTest, testUpdateWriterVersion) {
    ZkDataAccessorPtr zkAccessor(new ZkDataAccessor());
    SingleTopicWriterController stwc(zkAccessor, _zkRoot, "test_topic");

    // 1. write zk fail
    TopicWriterVersionInfo versionInfo;
    versionInfo.set_topicname("test_topic");
    versionInfo.set_majorversion(100);
    WriterVersion version;
    version.set_name("processor_1_100");
    version.set_version(200);
    *versionInfo.add_writerversions() = version;
    version.set_name("processor_3_300");
    version.set_version(300);
    *versionInfo.add_writerversions() = version;
    ErrorInfo error = stwc.updateWriterVersion(versionInfo);
    EXPECT_EQ(ERROR_BROKER_WRITE_FILE, error.errcode());
    EXPECT_EQ(0, stwc._writerVersion.size());
    EXPECT_EQ(0, stwc._majorVersion);

    // 2. normal update & write zk success
    ASSERT_TRUE(zkAccessor->init(_zkWrapper, _zkRoot));
    ASSERT_TRUE(stwc.init());
    error = stwc.updateWriterVersion(versionInfo);
    EXPECT_EQ(ERROR_NONE, error.errcode());

    EXPECT_EQ(2, stwc._writerVersion.size());
    EXPECT_EQ(100, stwc._majorVersion);
    EXPECT_EQ(200, stwc._writerVersion["processor_1_100"]);
    EXPECT_EQ(300, stwc._writerVersion["processor_3_300"]);
    string zkContent;
    EXPECT_EQ(EC_OK, FileSystem::readFile(_zkRoot + "/write_version_control/test_topic", zkContent));
    TopicWriterVersionInfo zkVersion;
    ASSERT_TRUE(zkVersion.ParseFromString(zkContent));
    EXPECT_TRUE(zkVersion.topicname().empty()); // topic name not set
    EXPECT_EQ(100, zkVersion.majorversion());
    EXPECT_EQ(2, zkVersion.writerversions_size());
    EXPECT_EQ("processor_3_300", zkVersion.writerversions(0).name());
    EXPECT_EQ(300, zkVersion.writerversions(0).version());
    EXPECT_EQ("processor_1_100", zkVersion.writerversions(1).name());
    EXPECT_EQ(200, zkVersion.writerversions(1).version());

    // 3. major version smaller, fail
    versionInfo.set_majorversion(99);
    versionInfo.clear_writerversions();
    version.set_name("processor_3_300");
    version.set_version(303);
    *versionInfo.add_writerversions() = version;
    version.set_name("processor_4_400");
    version.set_version(400);
    *versionInfo.add_writerversions() = version;

    error = stwc.updateWriterVersion(versionInfo);
    EXPECT_EQ(ERROR_CLIENT_INVALID_PARAMETERS, error.errcode());
    EXPECT_EQ(2, stwc._writerVersion.size());
    EXPECT_EQ(100, stwc._majorVersion);

    // 4. major version equal, success
    versionInfo.set_majorversion(100);
    error = stwc.updateWriterVersion(versionInfo);
    EXPECT_EQ(ERROR_NONE, error.errcode());
    EXPECT_EQ(3, stwc._writerVersion.size());
    EXPECT_EQ(100, stwc._majorVersion);
    EXPECT_EQ(200, stwc._writerVersion["processor_1_100"]);
    EXPECT_EQ(303, stwc._writerVersion["processor_3_300"]);
    EXPECT_EQ(400, stwc._writerVersion["processor_4_400"]);
    zkContent.clear();
    EXPECT_EQ(EC_OK, FileSystem::readFile(_zkRoot + "/write_version_control/test_topic", zkContent));
    ASSERT_TRUE(zkVersion.ParseFromString(zkContent));
    EXPECT_EQ(100, zkVersion.majorversion());
    EXPECT_EQ(3, zkVersion.writerversions_size());
    EXPECT_EQ("processor_4_400", zkVersion.writerversions(0).name());
    EXPECT_EQ(400, zkVersion.writerversions(0).version());
    EXPECT_EQ("processor_3_300", zkVersion.writerversions(1).name());
    EXPECT_EQ(303, zkVersion.writerversions(1).version());
    EXPECT_EQ("processor_1_100", zkVersion.writerversions(2).name());
    EXPECT_EQ(200, zkVersion.writerversions(2).version());

    // 5. higher major version, will clean current
    versionInfo.set_majorversion(101);
    versionInfo.clear_writerversions();
    version.set_name("processor_5_500");
    version.set_version(500);
    *versionInfo.add_writerversions() = version;
    error = stwc.updateWriterVersion(versionInfo);
    EXPECT_EQ(ERROR_NONE, error.errcode());
    EXPECT_EQ(1, stwc._writerVersion.size());
    EXPECT_EQ(101, stwc._majorVersion);
    EXPECT_EQ(500, stwc._writerVersion["processor_5_500"]);
    zkContent.clear();
    EXPECT_EQ(EC_OK, FileSystem::readFile(_zkRoot + "/write_version_control/test_topic", zkContent));
    ASSERT_TRUE(zkVersion.ParseFromString(zkContent));
    EXPECT_EQ(101, zkVersion.majorversion());
    EXPECT_EQ(1, zkVersion.writerversions_size());
    EXPECT_EQ("processor_5_500", zkVersion.writerversions(0).name());
    EXPECT_EQ(500, zkVersion.writerversions(0).version());
}

TEST_F(SingleTopicWriterControllerTest, testValidateThenUpdate) {
    SingleTopicWriterController stwc(ZkDataAccessorPtr(), _zkRoot, "test_topic");
    ASSERT_EQ(0, stwc._majorVersion);
    ASSERT_EQ(0, stwc._writerVersion.size());

    EXPECT_TRUE(stwc.validateThenUpdate("w1", 100, 101));
    ASSERT_EQ(100, stwc._majorVersion);
    ASSERT_EQ(1, stwc._writerVersion.size());
    ASSERT_EQ(101, stwc._writerVersion["w1"]);

    EXPECT_FALSE(stwc.validateThenUpdate("w1", 100, 100));
    ASSERT_EQ(101, stwc._writerVersion["w1"]);

    EXPECT_FALSE(stwc.validateThenUpdate("w1", 99, 102));
    ASSERT_EQ(100, stwc._majorVersion);
    ASSERT_EQ(101, stwc._writerVersion["w1"]);

    EXPECT_TRUE(stwc.validateThenUpdate("w1", 100, 103));
    ASSERT_EQ(100, stwc._majorVersion);
    ASSERT_EQ(1, stwc._writerVersion.size());
    ASSERT_EQ(103, stwc._writerVersion["w1"]);

    EXPECT_TRUE(stwc.validateThenUpdate("w2", 100, 104));
    ASSERT_EQ(100, stwc._majorVersion);
    ASSERT_EQ(2, stwc._writerVersion.size());
    ASSERT_EQ(103, stwc._writerVersion["w1"]);
    ASSERT_EQ(104, stwc._writerVersion["w2"]);

    EXPECT_TRUE(stwc.validateThenUpdate("w2", 101, 104));
    ASSERT_EQ(101, stwc._majorVersion);
    ASSERT_EQ(1, stwc._writerVersion.size());
    ASSERT_EQ(104, stwc._writerVersion["w2"]);
}

TEST_F(SingleTopicWriterControllerTest, testControlStr) {
    SingleTopicWriterController stwc(ZkDataAccessorPtr(), _zkRoot, "test_topic");
    stwc._majorVersion = 100;
    stwc._writerVersion["w1"] = 101;
    stwc._writerVersion["w2"] = 102;
    EXPECT_EQ("100-101", stwc.getDebugVersionInfo("w1"));
    EXPECT_EQ("100-102", stwc.getDebugVersionInfo("w2"));
    EXPECT_EQ("100-0", stwc.getDebugVersionInfo("w3"));
    EXPECT_EQ("100-w2:102;w1:101;", stwc.getWriterVersionStr(100, stwc._writerVersion));
}

} // namespace common
} // namespace swift
