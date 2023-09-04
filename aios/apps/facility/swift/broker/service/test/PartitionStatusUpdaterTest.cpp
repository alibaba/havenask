#include "swift/broker/service/PartitionStatusUpdater.h"

#include <algorithm>
#include <cstddef>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "ext/alloc_traits.h"
#include "swift/broker/service/TopicPartitionSupervisor.h"
#include "swift/common/Common.h"
#include "swift/config/BrokerConfig.h"
#include "swift/heartbeat/StatusUpdater.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/MessageComparator.h"
#include "swift/protocol/test/MessageCreator.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;

using namespace swift::protocol;
using namespace swift::heartbeat;

namespace swift {
namespace service {

class PartitionStatusUpdaterTest : public TESTBASE {
public:
    PartitionStatusUpdaterTest();
    ~PartitionStatusUpdaterTest();

public:
    void setUp();
    void tearDown();

protected:
    void checkUpdateTaskInfo(const std::string &partitionIdStr, PartitionStatusUpdater &partStatusUpdater);

protected:
    config::BrokerConfig _config;
    TopicPartitionSupervisor *_tpSupervisor;

protected:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(swift, PartitionStatusUpdaterTest);

PartitionStatusUpdaterTest::PartitionStatusUpdaterTest() {}

PartitionStatusUpdaterTest::~PartitionStatusUpdaterTest() {}

void PartitionStatusUpdaterTest::setUp() {
    AUTIL_LOG(DEBUG, "setUp!");
    _tpSupervisor = new TopicPartitionSupervisor(&_config, NULL, NULL);
}

void PartitionStatusUpdaterTest::tearDown() {
    AUTIL_LOG(DEBUG, "tearDown!");
    SWIFT_DELETE(_tpSupervisor);
}

TEST_F(PartitionStatusUpdaterTest, testUpdateTaskInfo) {
    PartitionStatusUpdater partStatusUpdater(_tpSupervisor);
    checkUpdateTaskInfo("topic1_0;topic1_1;topic2_0;topic2_1", partStatusUpdater);
    checkUpdateTaskInfo("topic1_0;topic2_0;topic2_1", partStatusUpdater);
    checkUpdateTaskInfo("", partStatusUpdater);
    checkUpdateTaskInfo("topic1_0;topic2_0;topic2_1", partStatusUpdater);
    checkUpdateTaskInfo("topic1_0;topic2_1;topic3_1", partStatusUpdater);
}

void PartitionStatusUpdaterTest::checkUpdateTaskInfo(const string &partitionIdStr,
                                                     PartitionStatusUpdater &partStatusUpdater) {
    protocol::DispatchInfo msg;
    vector<string> partitions = StringUtil::split(partitionIdStr, ";", true);
    std::vector<PartitionId> expectedPartitionIds;
    expectedPartitionIds.resize(partitions.size());
    for (size_t i = 0; i < partitions.size(); ++i) {
        protocol::TaskInfo *task = msg.add_taskinfos();
        *(task->mutable_partitionid()) = MessageCreator::createPartitionId(partitions[i]);
        task->set_minbuffersize(128);
        task->set_maxbuffersize(128);
        task->set_topicmode(TOPIC_MODE_MEMORY_ONLY);
        expectedPartitionIds[i] = MessageCreator::createPartitionId(partitions[i]);
    }

    partStatusUpdater.updateTaskInfo(msg);

    std::vector<PartitionId> partitionIds;
    _tpSupervisor->getAllPartitions(partitionIds);

    ASSERT_EQ(expectedPartitionIds.size(), partitionIds.size());

    sort(partitionIds.begin(), partitionIds.end());
    sort(expectedPartitionIds.begin(), expectedPartitionIds.end());

    for (size_t i = 0; i < partitionIds.size(); ++i) {
        ASSERT_TRUE(expectedPartitionIds[i] == partitionIds[i]);
    }
}

TEST_F(PartitionStatusUpdaterTest, testDoDiff) {
    PartitionStatusUpdater partStatusUpdater(_tpSupervisor);
    vector<PartitionId> current;
    set<TaskInfo> target;
    vector<TaskInfo> toLoad;
    vector<PartitionId> toUnLoad;

    TaskInfo ti;
    PartitionId pid;
    // t1-0 same with target
    pid.set_topicname("t1");
    pid.set_id(0);
    current.emplace_back(pid);
    *ti.mutable_partitionid() = pid;
    target.insert(ti);

    // t1-1 version change
    pid.set_id(1);
    pid.set_version(100);
    current.emplace_back(pid);
    pid.set_version(200);
    *ti.mutable_partitionid() = pid;
    target.insert(ti);

    // t2-0 inline version chagne
    pid.set_topicname("t2");
    pid.set_id(0);
    current.emplace_back(pid);
    auto *inlineVersion = pid.mutable_inlineversion();
    inlineVersion->set_masterversion(0);
    inlineVersion->set_partversion(1000);
    *ti.mutable_partitionid() = pid;
    target.insert(ti);

    // t2-1 not in current
    pid.set_topicname("t2");
    pid.set_id(1);
    *ti.mutable_partitionid() = pid;
    target.insert(ti);

    // t2-2 not in target
    pid.set_topicname("t2");
    pid.set_id(2);
    current.emplace_back(pid);

    EXPECT_EQ(4, current.size());
    EXPECT_EQ(4, target.size());
    partStatusUpdater.doDiff(current, target, toLoad, toUnLoad);
    EXPECT_EQ(3, toLoad.size());
    EXPECT_EQ("t1", toLoad[0].partitionid().topicname());
    EXPECT_EQ(1, toLoad[0].partitionid().id());
    EXPECT_EQ("t2", toLoad[1].partitionid().topicname());
    EXPECT_EQ(0, toLoad[1].partitionid().id());
    EXPECT_EQ("t2", toLoad[2].partitionid().topicname());
    EXPECT_EQ(1, toLoad[2].partitionid().id());
    EXPECT_EQ(3, toUnLoad.size());
    EXPECT_EQ("t1", toUnLoad[0].topicname());
    EXPECT_EQ(1, toUnLoad[0].id());
    EXPECT_EQ("t2", toUnLoad[1].topicname());
    EXPECT_EQ(0, toUnLoad[1].id());
    EXPECT_EQ("t2", toUnLoad[2].topicname());
    EXPECT_EQ(2, toUnLoad[2].id());
}

} // namespace service
} // namespace swift
