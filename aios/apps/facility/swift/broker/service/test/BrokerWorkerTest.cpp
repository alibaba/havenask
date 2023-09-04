#include "swift/broker/service/BrokerWorker.h"

#include <gmock/gmock-function-mocker.h>
#include <iosfwd>
#include <map>
#include <memory>
#include <ostream>
#include <stdint.h>
#include <stdlib.h>
#include <string>
#include <time.h>
#include <unistd.h>
#include <utility>

#include "autil/Log.h"
#include "autil/TimeUtility.h"
#include "fslib/fs/File.h"
#include "fslib/fs/zookeeper/test/ZkServer.h"
#include "swift/broker/service/BrokerPartition.h"
#include "swift/broker/service/TopicPartitionSupervisor.h"
#include "swift/common/Common.h"
#include "swift/config/BrokerConfig.h"
#include "swift/heartbeat/HeartbeatReporter.h"
#include "swift/heartbeat/ThreadSafeTaskStatus.h"
#include "swift/heartbeat/ZkConnectionStatus.h"
#include "swift/heartbeat/ZkHeartbeatWrapper.h"
#include "swift/monitor/BrokerMetricsReporter.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/Heartbeat.pb.h"
#include "swift/protocol/MessageComparator.h" // IWYU pragma: keep
#include "swift/util/IpMapper.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
using namespace swift::protocol;
using namespace swift::common;
using namespace swift::config;
using namespace swift::heartbeat;
using namespace swift::monitor;
using namespace fslib::fs;

namespace swift {
namespace service {

class MockBrokerWorker : public BrokerWorker {
public:
    MOCK_CONST_METHOD0(isLeader, bool());
};

class BrokerWorkerTest : public TESTBASE {
public:
    BrokerWorkerTest() {}
    ~BrokerWorkerTest() {}

public:
    void setUp() {}
    void tearDown() {}

private:
    void set_part_in_metric(PartitionInMetric *metric,
                            const string &topicName,
                            uint32_t partId,
                            uint32_t lastWriteTime,
                            uint32_t lastReadTime,
                            uint64_t writeRate1min,
                            uint64_t writeRate5min,
                            uint64_t readRate1min,
                            uint64_t readRate5min,
                            uint32_t writeRequest1min,
                            uint32_t writeRequest5min,
                            uint32_t readRequest1min,
                            uint32_t readRequest5min,
                            int64_t dfsTimeout);
    void expect_in_metric_collector(const BrokerInStatusCollector &collector,
                                    uint32_t updateTime,
                                    double cpu,
                                    double mem,
                                    uint64_t writeRate1min,
                                    uint64_t writeRate5min,
                                    uint64_t readRate1min,
                                    uint64_t readRate5min,
                                    uint64_t writeRequest1min,
                                    uint64_t writeRequest5min,
                                    uint64_t readRequest1min,
                                    uint64_t readRequest5min,
                                    int64_t dfsTimeout);

protected:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(swift, BrokerWorkerTest);

void BrokerWorkerTest::set_part_in_metric(PartitionInMetric *metric,
                                          const string &topicName,
                                          uint32_t partId,
                                          uint32_t lastWriteTime,
                                          uint32_t lastReadTime,
                                          uint64_t writeRate1min,
                                          uint64_t writeRate5min,
                                          uint64_t readRate1min,
                                          uint64_t readRate5min,
                                          uint32_t writeRequest1min,
                                          uint32_t writeRequest5min,
                                          uint32_t readRequest1min,
                                          uint32_t readRequest5min,
                                          int64_t commitDelay) {
    metric->set_topicname(topicName);
    metric->set_partid(partId);
    metric->set_lastwritetime(lastWriteTime); // s
    metric->set_lastreadtime(lastReadTime);   // s
    metric->set_writerate1min(writeRate1min); // byte
    metric->set_writerate5min(writeRate5min); // byte
    metric->set_readrate1min(readRate1min);   // byte
    metric->set_readrate5min(readRate5min);   // byte
    metric->set_writerequest1min(writeRequest1min);
    metric->set_writerequest5min(writeRequest5min);
    metric->set_readrequest1min(readRequest1min);
    metric->set_readrequest5min(readRequest5min);
    metric->set_commitdelay(commitDelay);
}

void BrokerWorkerTest::expect_in_metric_collector(const BrokerInStatusCollector &collector,
                                                  uint32_t updateTime,
                                                  double cpu,
                                                  double mem,
                                                  uint64_t writeRate1min,
                                                  uint64_t writeRate5min,
                                                  uint64_t readRate1min,
                                                  uint64_t readRate5min,
                                                  uint64_t writeRequest1min,
                                                  uint64_t writeRequest5min,
                                                  uint64_t readRequest1min,
                                                  uint64_t readRequest5min,
                                                  int64_t commitDelay) {
    EXPECT_EQ(cpu, collector.cpu);
    EXPECT_EQ(mem, collector.mem);
    EXPECT_EQ(writeRate1min, collector.writeRate1min);
    EXPECT_EQ(writeRate5min, collector.writeRate5min);
    EXPECT_EQ(readRate1min, collector.readRate1min);
    EXPECT_EQ(readRate5min, collector.readRate5min);
    EXPECT_EQ(writeRequest1min, collector.writeRequest1min);
    EXPECT_EQ(writeRequest5min, collector.writeRequest5min);
    EXPECT_EQ(readRequest1min, collector.readRequest1min);
    EXPECT_EQ(readRequest5min, collector.readRequest5min);
    EXPECT_EQ(commitDelay, collector.commitDelay);
}

TEST_F(BrokerWorkerTest, testConvertBrokerInMetric2Status) {
    BrokerWorker worker;
    BrokerInMetric metric;
    metric.set_reporttime(10);
    metric.set_cpuratio(18.8);
    metric.set_memratio(19.9);
    PartitionInMetric *part = metric.add_partinmetrics();
    set_part_in_metric(part, "topic", 1, 101, 200, 301, 401, 501, 601, 701, 801, 901, 1001, 100);
    BrokerInStatusCollector collector1;
    worker.convertBrokerInMetric2Status(metric, collector1);
    expect_in_metric_collector(collector1, 10, 18.8, 19.9, 301, 401, 501, 601, 701, 801, 901, 1001, 100);
    part = metric.add_partinmetrics();
    set_part_in_metric(part, "topic2", 2, 100, 201, 300, 400, 500, 600, 700, 800, 900, 1000, 50);
    BrokerInStatusCollector collector2;
    worker.convertBrokerInMetric2Status(metric, collector2);
    expect_in_metric_collector(collector2, 10, 18.8, 19.9, 601, 801, 1001, 1201, 1401, 1601, 1801, 2001, 100);
}

TEST_F(BrokerWorkerTest, testHeartbeat) {
    zookeeper::ZkServer *zkServer = zookeeper::ZkServer::getZkServer();
    unsigned short port = zkServer->port();
    ostringstream oss;
    oss << "127.0.0.1:" << port;
    string host = oss.str();

    srand(time(0));
    ostringstream oss2;
    string path = "/unittest/testHeartbeat/" + to_string(rand());
    string node = "broker";
    heartbeat::ZkHeartbeatWrapper zk;
    zk.setParameter(host);
    zk.open();

    MockBrokerWorker broker;
    broker._heartbeatReporter.setParameter(host, path, node);
    broker._tpSupervisor = new TopicPartitionSupervisor(nullptr, nullptr, nullptr);
    PartitionId partId;
    partId.set_topicname("test_topic");
    partId.set_id(100);
    TaskInfo taskInfo;
    taskInfo.mutable_partitionid()->set_topicname("test_topic");
    taskInfo.mutable_partitionid()->set_id(100);
    taskInfo.mutable_partitionid()->set_version(200);
    taskInfo.set_sealed(true);
    taskInfo.set_enablettldel(false);
    ThreadSafeTaskStatusPtr safeTaskPtr(new ThreadSafeTaskStatus(taskInfo));
    broker._tpSupervisor->_brokerPartitionMap[partId] = make_pair(safeTaskPtr, BrokerPartitionPtr());

    // leader report
    EXPECT_CALL(broker, isLeader()).WillOnce(Return(true));
    int64_t beginTime = TimeUtility::currentTime();
    broker._sessionId = 1234;
    broker.heartbeat();
    int64_t endTime = TimeUtility::currentTime();

    HeartbeatInfo msg;
    ASSERT_TRUE(zk.getHeartBeatInfo(path + "/" + node, msg));
    ASSERT_TRUE(msg.heartbeatid() >= beginTime);
    ASSERT_TRUE(msg.heartbeatid() <= endTime);
    ASSERT_TRUE(msg.alive());
    ASSERT_EQ(1234, msg.sessionid());
    ASSERT_TRUE(broker._status._heartbeatAlive);
    ASSERT_EQ(1, msg.tasks_size());
    ASSERT_EQ(safeTaskPtr->_taskStatus, msg.tasks(0));

    usleep(10 * 1000);
    // not leader
    EXPECT_CALL(broker, isLeader()).WillOnce(Return(false));
    broker._sessionId = 2345;
    beginTime = TimeUtility::currentTime();
    broker.heartbeat();
    endTime = TimeUtility::currentTime();
    HeartbeatInfo msg2;
    ASSERT_TRUE(zk.getHeartBeatInfo(path + "/" + node, msg2));
    ASSERT_EQ(msg2, msg);
    ASSERT_TRUE(msg2.heartbeatid() == msg.heartbeatid());
    ASSERT_FALSE(broker._status._heartbeatAlive);
    ASSERT_TRUE(broker._status._heartbeatLostTimestamp >= beginTime &&
                broker._status._heartbeatLostTimestamp <= endTime);
}

TEST_F(BrokerWorkerTest, testInitIpMapper) {
    {
        BrokerWorker brokerWorker;
        brokerWorker._brokerConfig = new BrokerConfig();
        ASSERT_TRUE(brokerWorker.initIpMapper());
    }
    {
        BrokerWorker brokerWorker;
        brokerWorker._brokerConfig = new BrokerConfig();
        brokerWorker._brokerConfig->ipMapFile = GET_TEMPLATE_DATA_PATH() + "broker/not_exist";
        ASSERT_FALSE(brokerWorker.initIpMapper());
    }
    {
        BrokerWorker brokerWorker;
        brokerWorker._brokerConfig = new BrokerConfig();
        brokerWorker._brokerConfig->ipMapFile = GET_TEST_DATA_PATH() + "broker/ip_seg_map";
        ASSERT_TRUE(brokerWorker.initIpMapper());
        ASSERT_TRUE(brokerWorker._ipMapper != nullptr);
        ASSERT_EQ("RU151", brokerWorker._ipMapper->mapIp("11.119.82.11:12345"));
    }
}

} // namespace service
} // namespace swift
