#include "swift/admin/modules/WorkerStatusModule.h"

#include <algorithm>
#include <cstdint>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/File.h"
#include "fslib/fs/zookeeper/test/ZkServer.h"
#include "swift/admin/SysController.h"
#include "swift/admin/TopicInStatusManager.h"
#include "swift/admin/TopicTable.h"
#include "swift/admin/WorkerInfo.h"
#include "swift/admin/WorkerTable.h"
#include "swift/admin/modules/BaseModule.h"
#include "swift/admin/test/TopicInStatusTestHelper.h"
#include "swift/common/Common.h"
#include "swift/config/AdminConfig.h"
#include "swift/protocol/AdminRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/Heartbeat.pb.h"
#include "swift/util/CircularVector.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace fslib::fs;
using namespace fslib;
using namespace autil;
using namespace swift::protocol;
using namespace swift::common;

namespace swift {
namespace admin {
using namespace swift::config;

class WorkerStatusModuleTest : public TESTBASE {
public:
    void setUp();
    void tearDown();

private:
    AdminConfig _adminConfig;
    std::string _zkRoot;
    zookeeper::ZkServer *_zkServer = nullptr;
};

void WorkerStatusModuleTest::setUp() {
    _zkServer = zookeeper::ZkServer::getZkServer();
    std::ostringstream oss;
    oss << "127.0.0.1:" << _zkServer->port();
    _zkRoot = "zfs://" + oss.str() + "/" + GET_CLASS_NAME();
    cout << "zkRoot: " << _zkRoot << endl;
    _adminConfig.dfsRoot = GET_TEMPLATE_DATA_PATH();
    _adminConfig.zkRoot = _zkRoot;
}

void WorkerStatusModuleTest::tearDown() {}

TEST_F(WorkerStatusModuleTest, testUpdateBrokerInMetric) {
    std::string broker("default##broker_1_40");
    std::string newName(broker);
    autil::StringUtil::replace(newName, '#', '_');
    EXPECT_EQ("default__broker_1_40", newName);

    SysController sysCtr(&_adminConfig, nullptr);
    sysCtr.init();
    WorkerStatusModule workerStatusModule;
    workerStatusModule._sysController = &sysCtr;
    workerStatusModule._adminConfig = &_adminConfig;
    ASSERT_TRUE(workerStatusModule.doInit());
    ASSERT_TRUE(workerStatusModule._workerTable);
    ASSERT_TRUE(workerStatusModule._topicInStatusManager);
    BrokerStatusRequest request;
    BrokerStatusResponse response;
    BrokerInMetric status;
    EXPECT_FALSE(workerStatusModule.updateBrokerInMetric(status));
    status.set_reporttime(10);
    status.set_rolename("default##role_1");
    status.set_cpuratio(18.8);
    status.set_memratio(19.9);
    status.set_zfstimeout(100);
    EXPECT_FALSE(workerStatusModule.updateBrokerInMetric(status));
    status.set_rolename("default##role_1_2#_#1.1.10.1:1234");
    EXPECT_FALSE(workerStatusModule.updateBrokerInMetric(status));
    std::vector<std::string> roleNames;
    std::string roleName("default##role_1_2");
    roleNames.push_back(roleName);
    sysCtr._workerTable.updateBrokerRoles(roleNames);
    EXPECT_TRUE(workerStatusModule.updateBrokerInMetric(status));
    WorkerInfoPtr info = sysCtr._workerTable._brokerWorkerMap[roleName];
    BrokerInStatus bStatus = info->_brokerInMetrics.back();
    EXPECT_EQ(10, bStatus.updateTime);
    EXPECT_EQ(18.8, bStatus.cpu);
    EXPECT_EQ(19.9, bStatus.mem);
    EXPECT_EQ(100, bStatus.zfsTimeout);
    EXPECT_EQ(0, bStatus.writeRate1min);
    EXPECT_EQ(0, bStatus.writeRate5min);
    EXPECT_EQ(0, bStatus.readRate1min);
    EXPECT_EQ(0, bStatus.readRate5min);
    EXPECT_EQ(0, bStatus.writeRequest1min);
    EXPECT_EQ(0, bStatus.writeRequest5min);
    EXPECT_EQ(0, bStatus.readRequest1min);
    EXPECT_EQ(0, bStatus.readRequest5min);

    PartitionInMetric *part = status.add_partinmetrics();
    set_part_in_metric(part, "topic", 1, 101, 200, 301, 401, 501, 601, 701, 801, 901, 1001, 200);
    part->set_topicname("topic");

    part = status.add_partinmetrics();
    set_part_in_metric(part, "topic", 2, 100, 201, 300, 400, 500, 600, 700, 800, 900, 1000, 300);

    TopicCreationRequest meta;
    meta.set_topicname("topic");
    meta.set_partitioncount(3);
    EXPECT_TRUE(sysCtr._topicTable.addTopic(&meta));
    EXPECT_TRUE(workerStatusModule.updateBrokerInMetric(status));

    // expect broker metric
    info = sysCtr._workerTable._brokerWorkerMap[roleName];
    bStatus = info->_brokerInMetrics.back();
    EXPECT_EQ(10, bStatus.updateTime);
    EXPECT_EQ(18.8, bStatus.cpu);
    EXPECT_EQ(19.9, bStatus.mem);
    EXPECT_EQ(601, bStatus.writeRate1min);
    EXPECT_EQ(801, bStatus.writeRate5min);
    EXPECT_EQ(1001, bStatus.readRate1min);
    EXPECT_EQ(1201, bStatus.readRate5min);
    EXPECT_EQ(1401, bStatus.writeRequest1min);
    EXPECT_EQ(1601, bStatus.writeRequest5min);
    EXPECT_EQ(1801, bStatus.readRequest1min);
    EXPECT_EQ(2001, bStatus.readRequest5min);
    EXPECT_EQ(300, bStatus.commitDelay);

    // expect partition metric
    TopicInStatus &tiStatus = sysCtr._topicInStatusManager._topicStatusMap["topic"];
    EXPECT_EQ(2, tiStatus._rwMetrics.size());

    const PartitionInStatus &pStatus1 = tiStatus._rwMetrics.get(1);
    ASSERT_NO_FATAL_FAILURE(expect_part_in_status(pStatus1, 1, 10, 301, 401, 501, 601, 701, 801, 901, 1001, 1));
    const PartitionInStatus &pStatus2 = tiStatus._rwMetrics.get(2);
    ASSERT_NO_FATAL_FAILURE(expect_part_in_status(pStatus2, 2, 10, 300, 400, 500, 600, 700, 800, 900, 1000, 1));

    std::pair<uint32_t, uint32_t> &rwTime = sysCtr._topicInStatusManager._topicRWTimeMap["topic"];
    EXPECT_EQ(101, rwTime.first);
    EXPECT_EQ(201, rwTime.second);
}

}; // namespace admin
}; // namespace swift
