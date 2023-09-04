#include "swift/heartbeat/TaskInfoMonitor.h"

#include <functional>
#include <gmock/gmock-function-mocker.h>
#include <gmock/gmock-matchers.h>
#include <gtest/gtest-matchers.h>
#include <iostream>
#include <stdint.h>
#include <stdlib.h>
#include <string>
#include <type_traits>
#include <unistd.h>

#include "aios/apps/facility/cm2/cm_basic/util/zk_wrapper.h"
#include "autil/CommonMacros.h"
#include "autil/Log.h"
#include "fslib/fs/File.h"
#include "fslib/fs/zookeeper/test/ZkServer.h"
#include "swift/config/BrokerConfig.h"
#include "swift/heartbeat/StatusUpdater.h"
#include "swift/heartbeat/ZkConnectionStatus.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/test/MessageCreator.h"
#include "unittest/unittest.h"

using namespace std;
using namespace std::placeholders;
using namespace cm_basic;
using namespace swift::protocol;
using namespace fslib::fs;
namespace swift {
namespace heartbeat {

class MockTaskInfoMonitor : public TaskInfoMonitor {
private:
    MOCK_METHOD2(getData, bool(const std::string &path, protocol::DispatchInfo &msg));
};

class MockStatusUpdater : public StatusUpdater {
private:
    MOCK_METHOD1(updateTaskInfo, void(const protocol::DispatchInfo &));
};

class TaskInfoMonitorTest : public TESTBASE {
public:
    TaskInfoMonitorTest();
    ~TaskInfoMonitorTest();

public:
    void setUp();
    void tearDown();

public:
    void checkExtractParameter(const std::string &address,
                               bool f,
                               const std::string &expectHost,
                               const std::string &expectPath);
    void addTaskInfo(protocol::DispatchInfo &dispatchInfo, const std::string &partitionId, uint32_t bufferSize = 1024);

protected:
    std::string _rand;
    protocol::DispatchInfo _globalMsg;
    std::string _host;
    config::BrokerConfig _config;
    ZkConnectionStatus *_status;
    int _count;
    zookeeper::ZkServer *_zkServer;

protected:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(swift, TaskInfoMonitorTest);

TaskInfoMonitorTest::TaskInfoMonitorTest() : _status(NULL), _zkServer(NULL) {}

TaskInfoMonitorTest::~TaskInfoMonitorTest() {}

void TaskInfoMonitorTest::setUp() {
    AUTIL_LOG(DEBUG, "setUp!");
    _zkServer = zookeeper::ZkServer::getZkServer();
    unsigned short port = _zkServer->port();
    std::ostringstream oss;
    oss << "127.0.0.1:" << port;
    _host = oss.str();
    _rand = GET_CLASS_NAME();
    cout << "zkPath: " << _host << ", rand: " << _rand << endl;

    _status = new ZkConnectionStatus;
}

void TaskInfoMonitorTest::tearDown() {
    AUTIL_LOG(DEBUG, "tearDown!");
    DELETE_AND_SET_NULL(_status);
}

TEST_F(TaskInfoMonitorTest, testExtractParameter) {
    AUTIL_LOG(DEBUG, "Begin Test!");

    checkExtractParameter("zfs://127.0.0.1:1234/swift", true, "127.0.0.1:1234", "/swift");

    checkExtractParameter("zfs://127.0.0.1:1234/swift/service/user/", true, "127.0.0.1:1234", "/swift/service/user");

    checkExtractParameter("admin://127.0.0.1:1234/swift/service/user", false, "", "");

    checkExtractParameter("zfs://127.0.0.1:1234/", false, "", "");

    checkExtractParameter("zfs:/", false, "", "");
}

void TaskInfoMonitorTest::checkExtractParameter(const std::string &address,
                                                bool f,
                                                const std::string &expectHost,
                                                const std::string &expectPath) {
    TaskInfoMonitor monitor;
    monitor.setZkConnectionStatus(_status);
    string host, path;
    ASSERT_EQ(f, monitor.extractParameter(address, host, path));
    if (f) {
        ASSERT_EQ(expectHost, host);
        ASSERT_EQ(expectPath, path);
    }
}

TEST_F(TaskInfoMonitorTest, testTrigleCreate) {
    AUTIL_LOG(DEBUG, "Begin Test!");

    TaskInfoMonitor monitor;
    MockStatusUpdater statusUpdater;
    monitor.setStatusUpdater(&statusUpdater);
    monitor.setZkConnectionStatus(_status);
    string path = "/unittest/HBMT/" + _rand;
    string address = "zfs://" + _host + path;
    ASSERT_TRUE(monitor.init(address));

    protocol::DispatchInfo msg;
    msg.set_brokeraddress("127.0.0.1:7785");

    addTaskInfo(msg, "topic1_0");

    protocol::DispatchInfo actualMsg;
    EXPECT_CALL(statusUpdater, updateTaskInfo(_))
        .WillRepeatedly(Invoke([&](const protocol::DispatchInfo &dispatchInfo) { actualMsg = dispatchInfo; }));

    ZkWrapper zk;
    zk.setParameter(_host);
    ASSERT_TRUE(zk.open());

    ASSERT_TRUE(monitor.start());
    sleep(3);
    std::string value1;
    addTaskInfo(msg, "topic1_1");
    protocol::DispatchInfo msg1 = msg;
    ASSERT_TRUE(msg.SerializeToString(&value1));
    ASSERT_TRUE(zk.touch(path, value1, false));
    sleep(3);
    EXPECT_EQ(msg.ShortDebugString(), actualMsg.ShortDebugString());

    std::string value2;
    addTaskInfo(msg, "topic2_0");
    protocol::DispatchInfo msg2 = msg;
    ASSERT_TRUE(msg.SerializeToString(&value2));
    ASSERT_TRUE(zk.touch(path, value2, false));
    sleep(3);
    EXPECT_EQ(msg2.ShortDebugString(), actualMsg.ShortDebugString());

    ASSERT_TRUE(zk.touch(path, value1, false));
    sleep(3);
    EXPECT_EQ(msg1.ShortDebugString(), actualMsg.ShortDebugString());

    ASSERT_TRUE(zk.remove(path));
    sleep(3);
    EXPECT_EQ("", actualMsg.ShortDebugString());

    ASSERT_TRUE(zk.touch(path, value2, false));
    sleep(3);
    EXPECT_EQ(msg2.ShortDebugString(), actualMsg.ShortDebugString());
    monitor.stop();
}

TEST_F(TaskInfoMonitorTest, testFillTaskInfo) {
    AUTIL_LOG(DEBUG, "Begin Test!");
    MockTaskInfoMonitor monitor;
    MockStatusUpdater statusUpdater;
    monitor.setStatusUpdater(&statusUpdater);
    monitor.setZkConnectionStatus(_status);
    string path = "/unittest/HBMT/" + _rand;
    string address = "zfs://" + _host + path;
    ASSERT_TRUE(monitor.init(address));
    ASSERT_TRUE(_status->_zkMonitorAlive);
    protocol::DispatchInfo msg;
    msg.set_brokeraddress("127.0.0.1:7785");

    addTaskInfo(msg, "topic1_0");

    protocol::DispatchInfo actualMsg;
    EXPECT_CALL(monitor, getData(_, _)).WillRepeatedly(Return(false));
    ZkWrapper zk;
    zk.setParameter(_host);
    ASSERT_TRUE(zk.open());

    ASSERT_TRUE(monitor.start());
    sleep(3);
    std::string value1;
    addTaskInfo(msg, "topic1_1");
    protocol::DispatchInfo msg1 = msg;
    ASSERT_TRUE(msg.SerializeToString(&value1));
    ASSERT_TRUE(zk.touch(path, value1, false));
    sleep(3);
    ASSERT_FALSE(_status->_zkMonitorAlive);
}

TEST_F(TaskInfoMonitorTest, testPathExistBeforeStart) {
    AUTIL_LOG(DEBUG, "Begin Test!");

    TaskInfoMonitor monitor;
    MockStatusUpdater statusUpdater;
    monitor.setStatusUpdater(&statusUpdater);
    monitor.setZkConnectionStatus(_status);
    string path = "/unittest/HBMT/" + _rand;
    string address = "zfs://" + _host + path;
    ASSERT_TRUE(monitor.init(address));

    protocol::DispatchInfo msg;
    msg.set_brokeraddress("127.0.0.1:7785");

    protocol::DispatchInfo actualMsg;
    EXPECT_CALL(statusUpdater, updateTaskInfo(_))
        .WillRepeatedly(Invoke([&](const protocol::DispatchInfo &dispatchInfo) { actualMsg = dispatchInfo; }));

    ZkWrapper zk;
    zk.setParameter(_host);
    ASSERT_TRUE(zk.open());

    std::string value3;
    protocol::DispatchInfo msg3 = msg;
    ASSERT_TRUE(msg3.SerializeToString(&value3));

    std::string value1;
    addTaskInfo(msg, "topic1_0");
    addTaskInfo(msg, "topic1_1");
    addTaskInfo(msg, "topic2_0");
    protocol::DispatchInfo msg1 = msg;
    ASSERT_TRUE(msg1.SerializeToString(&value1));
    ASSERT_TRUE(zk.touch(path, value1, false));

    ASSERT_TRUE(monitor.start());
    sleep(3);
    EXPECT_EQ(msg1.ShortDebugString(), actualMsg.ShortDebugString());

    std::string value2;
    addTaskInfo(msg, "topic2_1");
    protocol::DispatchInfo msg2 = msg;
    ASSERT_TRUE(msg2.SerializeToString(&value2));
    ASSERT_TRUE(zk.touch(path, value2, false));
    sleep(3);
    EXPECT_EQ(msg2.ShortDebugString(), actualMsg.ShortDebugString());

    ASSERT_TRUE(zk.remove(path));
    sleep(3);
    EXPECT_EQ("", actualMsg.ShortDebugString());

    ASSERT_TRUE(zk.touch(path, value1, false));
    sleep(3);
    EXPECT_EQ(msg1.ShortDebugString(), actualMsg.ShortDebugString());

    ASSERT_TRUE(zk.touch(path, value3, false));
    sleep(3);
    EXPECT_EQ(msg3.ShortDebugString(), actualMsg.ShortDebugString());

    monitor.stop();
}
TEST_F(TaskInfoMonitorTest, testDisconnectedAndDeadNode) {
    AUTIL_LOG(DEBUG, "Begin Test!");

    TaskInfoMonitor monitor;
    MockStatusUpdater statusUpdater;
    monitor.setStatusUpdater(&statusUpdater);
    monitor.setZkConnectionStatus(_status);
    string path = "/unittest/testDisconnected/" + _rand;
    string address = "zfs://" + _host + path;
    ASSERT_TRUE(monitor.init(address));

    ZkWrapper zk;
    protocol::DispatchInfo msg;
    msg.set_brokeraddress("127.0.0.1:7785");
    addTaskInfo(msg, "topic1_0");
    addTaskInfo(msg, "topic1_1");

    string value;
    ASSERT_TRUE(msg.SerializeToString(&value));

    protocol::DispatchInfo actualMsg;
    EXPECT_CALL(statusUpdater, updateTaskInfo(_))
        .WillRepeatedly(Invoke([&](const protocol::DispatchInfo &dispatchInfo) { actualMsg = dispatchInfo; }));

    zk.setParameter(_host);
    ASSERT_TRUE(zk.open());
    ASSERT_TRUE(zk.touch(path, value));

    ASSERT_TRUE(monitor.start());
    sleep(1);
    EXPECT_EQ(msg.ShortDebugString(), actualMsg.ShortDebugString());

    monitor._zk.close();
    ASSERT_TRUE(monitor._zk.isBad());
    //    ASSERT_TRUE(_tpSupervisor->getStatus() == false);
    addTaskInfo(msg, "topic1_2");
    ASSERT_TRUE(msg.SerializeToString(&value));
    ASSERT_TRUE(zk.touch(path, value));
    monitor._zk.open();
    sleep(3);
    EXPECT_EQ(msg.ShortDebugString(), actualMsg.ShortDebugString());
    monitor.stop();
}

void TaskInfoMonitorTest::addTaskInfo(DispatchInfo &dispatchInfo, const std::string &partitionId, uint32_t bufferSize) {
    protocol::TaskInfo *task = dispatchInfo.add_taskinfos();
    *(task->mutable_partitionid()) = MessageCreator::createPartitionId(partitionId);
    task->set_minbuffersize(bufferSize);
    task->set_maxbuffersize(bufferSize);
    task->set_topicmode(TOPIC_MODE_MEMORY_ONLY);
}

TEST_F(TaskInfoMonitorTest, testDisconnectedNotify) {
    AUTIL_LOG(DEBUG, "Begin Test!");
    TaskInfoMonitor monitor;
    MockStatusUpdater statusUpdater;
    monitor.setStatusUpdater(&statusUpdater);
    monitor.setZkConnectionStatus(_status);
    string path = "/unittest/testDisconnected/" + _rand;
    string address = "zfs://" + _host + path;
    ASSERT_TRUE(monitor.init(address));

    ZkWrapper zk;
    protocol::DispatchInfo msg;
    msg.set_brokeraddress("127.0.0.1:7785");
    addTaskInfo(msg, "topic1_0");
    addTaskInfo(msg, "topic1_1");

    string value;
    ASSERT_TRUE(msg.SerializeToString(&value));

    zk.setParameter(_host);
    ASSERT_TRUE(zk.open());
    ASSERT_TRUE(zk.touch(path, value));

    protocol::DispatchInfo actualMsg;
    EXPECT_CALL(statusUpdater, updateTaskInfo(_))
        .WillRepeatedly(Invoke([&](const protocol::DispatchInfo &dispatchInfo) { actualMsg = dispatchInfo; }));

    ASSERT_TRUE(monitor.start());
    //    sleep(1);
    EXPECT_EQ(msg.ShortDebugString(), actualMsg.ShortDebugString());

    monitor.stop();
    ASSERT_TRUE(monitor._zk.isBad());
    monitor.start();
    //    sleep(1);
    EXPECT_EQ(msg.ShortDebugString(), actualMsg.ShortDebugString());

    monitor.stop();
    ASSERT_TRUE(monitor._zk.isBad());
    addTaskInfo(msg, "topic1_2");
    ASSERT_TRUE(msg.SerializeToString(&value));
    ASSERT_TRUE(zk.touch(path, value));
    monitor.start();
    sleep(1);
    EXPECT_EQ(msg.ShortDebugString(), actualMsg.ShortDebugString());
    monitor.stop();
}

} // namespace heartbeat
} // namespace swift
