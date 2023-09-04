#include "swift/heartbeat/HeartbeatReporter.h"

#include <sstream>
#include <stdint.h>
#include <stdlib.h>
#include <string>
#include <time.h>
#include <unistd.h>

#include "aios/apps/facility/cm2/cm_basic/util/zk_wrapper.h"
#include "autil/Lock.h"
#include "autil/TimeUtility.h"
#include "fslib/fs/File.h"
#include "fslib/fs/zookeeper/test/ZkServer.h"
#include "swift/heartbeat/ZkHeartbeatWrapper.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/Heartbeat.pb.h"
#include "swift/protocol/MessageComparator.h" // IWYU pragma: keep
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
using namespace cm_basic;
using namespace swift::protocol;
using namespace fslib::fs;
namespace swift {
namespace heartbeat {

class HeartbeatReporterTest : public TESTBASE {
public:
    void setUp() {
        _zkServer = zookeeper::ZkServer::getZkServer();
        unsigned short port = _zkServer->port();
        std::ostringstream oss;
        oss << "127.0.0.1:" << port;
        _host = oss.str();

        srand(time(0));
        std::ostringstream oss2;
        oss2 << rand();
        _rand = oss2.str();
    }
    void tearDown() {}

protected:
    void checkExtractParameter(const std::string &address,
                               bool f,
                               const std::string &expectHost,
                               const std::string &expectPath,
                               const std::string &expectName);

protected:
    std::string _rand;
    autil::ThreadCond _cond;
    protocol::HeartbeatInfo globalMsg;
    std::string _host;
    zookeeper::ZkServer *_zkServer = nullptr;
};

TEST_F(HeartbeatReporterTest, testExtractParameter) {
    checkExtractParameter(
        "zfs://127.0.0.1:1234/swift/123.123.123.123:1234", true, "127.0.0.1:1234", "/swift", "123.123.123.123:1234");

    checkExtractParameter("zfs://127.0.0.1:1234/swift/service/user/123.123.123.123:1234",
                          true,
                          "127.0.0.1:1234",
                          "/swift/service/user",
                          "123.123.123.123:1234");

    checkExtractParameter("admin://127.0.0.1:1234/swift/service/user/123.123.123.123:1234", false, "", "", "");

    checkExtractParameter("zfs://127.0.0.1:1234/123.123.123.123:1234", false, "", "", "");

    checkExtractParameter("zfs://127.0.0.1:1234/swift/123.123.123.123:1234/", false, "", "", "");

    checkExtractParameter("zfs:/", false, "", "", "");
}

void HeartbeatReporterTest::checkExtractParameter(const std::string &address,
                                                  bool f,
                                                  const std::string &expectHost,
                                                  const std::string &expectPath,
                                                  const std::string &expectName) {
    HeartbeatReporter reporter;
    string host, path, name;
    ASSERT_EQ(f, reporter.extractParameter(address, host, path, name));
    if (f) {
        ASSERT_EQ(expectHost, host);
        ASSERT_EQ(expectPath, path);
        ASSERT_EQ(expectName, name);
    }
}

TEST_F(HeartbeatReporterTest, testBadParameter) {
    HeartbeatReporter reporter;
    reporter.setParameter("notExist", "/unittest/HBRT/" + _rand, "nodeA");

    HeartbeatInfo msg;
    msg.set_alive(true);
    ASSERT_TRUE(!reporter.heartBeat(msg));
}

TEST_F(HeartbeatReporterTest, testHeartbeats) {
    std::string path = "/unittest/HBRT/" + _rand;
    std::string node = "nodeB";

    HeartbeatReporter reporter;
    ASSERT_TRUE(reporter._firstHeartbeat);
    ASSERT_EQ(0, reporter._lastReportTime);
    reporter.setParameter(_host, path, node);

    HeartbeatInfo msg;
    msg.set_alive(true);
    msg.set_address("10.250.12.8:123");
    msg.set_role(ROLE_TYPE_BROKER);
    TaskStatus *tasks = (msg.add_tasks());
    tasks->set_status(PARTITION_STATUS_STARTING);
    ASSERT_TRUE(reporter._firstHeartbeat);
    ASSERT_TRUE(reporter.heartBeat(msg));
    ASSERT_TRUE(!reporter._firstHeartbeat);

    ZkHeartbeatWrapper zk;
    zk.setParameter(_host);
    zk.open();

    HeartbeatInfo msg2;
    // test heartbeat
    ASSERT_TRUE(zk.getHeartBeatInfo(path + "/" + node, msg2));
    ASSERT_EQ(msg, msg2);

    // test change
    msg2.Clear();
    tasks->set_status(PARTITION_STATUS_RUNNING);

    ASSERT_TRUE(reporter.heartBeat(msg));
    ASSERT_TRUE(zk.getHeartBeatInfo(path + "/" + node, msg2));
    ASSERT_EQ(msg, msg2);

    // test force
    msg2.Clear();
    HeartbeatInfo msg3 = msg;
    msg3.mutable_tasks(0)->set_status(PARTITION_STATUS_STARTING);

    zk.touch(path + "/" + node, msg3);
    ASSERT_TRUE(zk.getHeartBeatInfo(path + "/" + node, msg2));
    ASSERT_EQ(msg3, msg2);
    ASSERT_TRUE(reporter.heartBeat(msg));
    msg2.Clear();
    ASSERT_TRUE(zk.getHeartBeatInfo(path + "/" + node, msg2));
    ASSERT_EQ(msg3, msg2);
    ASSERT_TRUE(reporter.heartBeat(msg, true));
    msg2.Clear();
    ASSERT_TRUE(zk.getHeartBeatInfo(path + "/" + node, msg2));
    ASSERT_EQ(msg, msg2);

    // test FORCE_REPORT_INTERVAL_SEC
    HeartbeatInfo zkMsg;
    int64_t curTime = TimeUtility::currentTimeInSeconds();
    reporter._lastReportTime = curTime - 100;
    ASSERT_TRUE(reporter.heartBeat(msg));
    zkMsg.Clear();
    ASSERT_TRUE(zk.getHeartBeatInfo(path + "/" + node, zkMsg));
    ASSERT_EQ(msg, reporter._localImage);
    ASSERT_EQ(msg, zkMsg);
    ASSERT_TRUE((curTime - 100) == reporter._lastReportTime);
    zkMsg.Clear();
    reporter._lastReportTime = curTime - 301; // FORCE_REPORT_INTERVAL_SEC + 1
    ASSERT_TRUE(reporter.heartBeat(msg));
    ASSERT_TRUE(zk.getHeartBeatInfo(path + "/" + node, zkMsg));
    ASSERT_EQ(msg, reporter._localImage);
    ASSERT_EQ(msg, zkMsg);
    ASSERT_TRUE(reporter._lastReportTime - curTime <= 1);

    // test ephemeral
    reporter.shutdown();
    // ASSERT_TRUE(!zk.getHeartBeatInfo(path + "/" + node, msg2));

    // test auto reconnect
    ASSERT_TRUE(reporter.heartBeat(msg));
    msg2.Clear();
    ASSERT_TRUE(zk.getHeartBeatInfo(path + "/" + node, msg2));
    ASSERT_EQ(msg, msg2);
}

TEST_F(HeartbeatReporterTest, testNodeDisappear) {
    std::string path = "/unittest/HBRT/" + _rand;
    std::string node = "nodeE";

    HeartbeatReporter reporter;
    reporter.setParameter(_host, path, node);

    HeartbeatInfo msg;
    msg.set_alive(true);
    msg.set_address("abcd");

    ASSERT_TRUE(reporter.heartBeat(msg));

    ZkHeartbeatWrapper zk;
    zk.setParameter(_host);
    zk.open();

    HeartbeatInfo msg2;

    // test heartbeat

    ASSERT_TRUE(zk.getHeartBeatInfo(path + "/" + node, msg2));
    ASSERT_EQ(msg, msg2);

    // remove it
    ASSERT_TRUE(zk.remove(path + "/" + node));

    for (int i = 0; i < 1000; ++i) {
        if (reporter._bChanged) {
            break;
        }
        usleep(10000);
    }

    // test recreate
    ASSERT_TRUE(reporter.heartBeat(msg));

    ASSERT_TRUE(zk.getHeartBeatInfo(path + "/" + node, msg2));
    ASSERT_EQ(msg, msg2);
}

TEST_F(HeartbeatReporterTest, testDisconnected) {
    std::string path = "/unittest/HBRT/" + _rand;
    std::string node = "nodeF";

    HeartbeatReporter reporter;
    reporter.setParameter(_host, path, node);

    HeartbeatInfo msg;
    msg.set_alive(true);
    msg.set_address("abcd");

    ASSERT_TRUE(reporter.heartBeat(msg));

    {
        ZkHeartbeatWrapper zk;
        zk.setParameter(_host);
        zk.open();

        HeartbeatInfo msg2;

        // test heartbeat

        ASSERT_TRUE(zk.getHeartBeatInfo(path + "/" + node, msg2));
        ASSERT_EQ(msg, msg2);
    }

    _zkServer->stop();
    sleep(3);
    _zkServer->simpleStart();

    ZkHeartbeatWrapper zk;
    zk.setParameter(_host);
    zk.open();

    bool exist = true;
    ASSERT_TRUE(zk.check(path + "/" + node, exist));
    ASSERT_TRUE(!exist);

    // test recreate
    ASSERT_TRUE(reporter.heartBeat(msg));

    HeartbeatInfo msg2;

    // test heartbeat
    ASSERT_TRUE(zk.getHeartBeatInfo(path + "/" + node, msg2));
    ASSERT_EQ(msg, msg2);
}

} // namespace heartbeat
} // namespace swift
