#ifndef WORKER_FRAMEWORK_ZKSTATETEST_H
#define WORKER_FRAMEWORK_ZKSTATETEST_H

#include "worker_framework/ZkState.h"

#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "fslib/fs/zookeeper/test/ZkServer.h"
#include "test/test.h"
#include "unittest/unittest.h"
#include "worker_framework/PathUtil.h"

using namespace std;
using namespace autil;
using namespace testing;
using namespace worker_framework;
namespace worker_framework {

class ZkStateTest : public testing::Test {
protected:
    ZkStateTest() { _zkWrapper = NULL; }

    virtual ~ZkStateTest() {}

    virtual void SetUp() {
        unsigned short port = fslib::fs::zookeeper::ZkServer::getZkServer()->port();
        _host = string("127.0.0.1:") + StringUtil::toString(port);
        DELETE_AND_SET_NULL(_zkWrapper);

        _zkWrapper = new cm_basic::ZkWrapper(_host); // 10 seconds
        ASSERT_TRUE(_zkWrapper->open());

        _zkDir = "/ZkStateTest";
        ASSERT_TRUE(_zkWrapper->remove(_zkDir));

        _zkStateFile = PathUtil::joinPath(_zkDir, "zk_state");
    }

    virtual void TearDown() { DELETE_AND_SET_NULL(_zkWrapper); }

protected:
    string _host;
    string _zkDir;
    string _zkStateFile;
    cm_basic::ZkWrapper *_zkWrapper;

protected:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(worker_framework.worker_base, ZkStateTest);

TEST_F(ZkStateTest, testWrite) {
    ZkState state(_zkWrapper, _zkStateFile);
    EXPECT_EQ("", state.getLastContent());
    bool isExist = false;
    EXPECT_TRUE(_zkWrapper->check(_zkStateFile, isExist));
    EXPECT_FALSE(isExist);

    EXPECT_EQ(WorkerState::EC_UPDATE, state.write("aa"));
    EXPECT_TRUE(_zkWrapper->check(_zkStateFile, isExist));
    EXPECT_TRUE(isExist);
    EXPECT_EQ("aa", state.getLastContent());

    EXPECT_EQ(WorkerState::EC_OK, state.write("aa"));
    EXPECT_EQ("aa", state.getLastContent());

    EXPECT_EQ(WorkerState::EC_UPDATE, state.write("abc"));
    EXPECT_EQ("abc", state.getLastContent());
}

TEST_F(ZkStateTest, testRead) {
    ZkState readState(_zkWrapper, _zkStateFile);
    ZkState writeState(_zkWrapper, _zkStateFile);
    string content;

    // state file not exist
    EXPECT_EQ(WorkerState::EC_NOT_EXIST, readState.read(content));

    EXPECT_EQ(WorkerState::EC_UPDATE, writeState.write("abc"));
    EXPECT_EQ(WorkerState::EC_UPDATE, readState.read(content));
    EXPECT_EQ("abc", content);
    EXPECT_EQ("abc", readState.getLastContent());

    // read again
    EXPECT_EQ(WorkerState::EC_OK, readState.read(content));
    EXPECT_EQ("abc", content);
    EXPECT_EQ("abc", readState.getLastContent());

    EXPECT_EQ(WorkerState::EC_UPDATE, writeState.write("abcd"));
    EXPECT_EQ(WorkerState::EC_UPDATE, readState.read(content));
    EXPECT_EQ("abcd", content);
    EXPECT_EQ("abcd", readState.getLastContent());
}

TEST_F(ZkStateTest, testStaticReadAndWrite) {
    string zfsPath = "zfs://" + _host + _zkDir + "/zkState";
    string content;
    ASSERT_FALSE(ZkState::read(zfsPath, content));
    ASSERT_TRUE(ZkState::write(zfsPath, "abc"));
    ASSERT_TRUE(ZkState::read(zfsPath, content));
    ASSERT_EQ("abc", content);

    content = "";
    ASSERT_TRUE(ZkState::read(_zkWrapper, zfsPath, content));
    ASSERT_EQ("abc", content);
    ASSERT_TRUE(ZkState::write(_zkWrapper, zfsPath, "123"));
    ASSERT_TRUE(ZkState::read(_zkWrapper, zfsPath, content));
    ASSERT_EQ("123", content);
}

TEST_F(ZkStateTest, testStaticReadAndWriteFailed) {
    string zfsPath = "zfs://not_exist_path";
    string content;
    ASSERT_FALSE(ZkState::write(zfsPath, "abc"));
    ASSERT_FALSE(ZkState::read(zfsPath, content));
}

TEST_F(ZkStateTest, testGetZkRelativePath) {
    string zfsPath = string("zfs://") + _host + _zkStateFile;
    EXPECT_EQ("/ZkStateTest/zk_state", ZkState::getZkRelativePath(zfsPath));
    EXPECT_EQ("/ZkStateTest/zk_state", ZkState::getZkRelativePath(_zkStateFile));
}

}; // namespace worker_framework

#endif // WORKER_FRAMEWORK_ZKSTATETEST_H
