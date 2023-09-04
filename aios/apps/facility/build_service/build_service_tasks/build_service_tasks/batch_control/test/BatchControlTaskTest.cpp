#include "build_service_tasks/batch_control/BatchControlTask.h"

#include "autil/StringUtil.h"
#include "build_service/config/BuildServiceConfig.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/ResourceReaderManager.h"
#include "build_service_tasks/batch_control/test/MockBatchControlWorker.h"
#include "build_service_tasks/test/unittest.h"
#include "fslib/util/FileUtil.h"

using namespace std;
using namespace testing;
using namespace build_service::util;
using namespace build_service::config;

namespace build_service { namespace task_base {

typedef BatchControlWorker::BatchOp BatchOp;
typedef BatchControlWorker::BatchInfo BatchInfo;

class BatchControlTaskTest : public BUILD_SERVICE_TASKS_TESTBASE
{
public:
    void setUp();
    void tearDown();
};

void BatchControlTaskTest::setUp() {}

void BatchControlTaskTest::tearDown() {}

TEST_F(BatchControlTaskTest, testHandleTarget)
{
    MockBatchControlTask* task = new ::testing::StrictMock<MockBatchControlTask>;
    config::TaskTarget target;
    target.addTargetDescription(READ_SRC, SWIFT_READ_SRC);
    target.addTargetDescription(READ_SRC_TYPE, SWIFT_READ_SRC);
    target.addTargetDescription(SRC_BATCH_MODE, "true");
    // lack admin address
    ASSERT_FALSE(task->handleTarget(target));
    target.addTargetDescription(BS_TASK_BATCH_CONTROL_ADDRESS, "1.2.3.4");
    // lack port or invalid port
    ASSERT_FALSE(task->handleTarget(target));
    target.addTargetDescription(BS_TASK_BATCH_CONTROL_PORT, "");
    ASSERT_FALSE(task->handleTarget(target));
    target.updateTargetDescription(BS_TASK_BATCH_CONTROL_PORT, "8888");
    // lack topicname
    ASSERT_FALSE(task->handleTarget(target));
    target.addTargetDescription(BS_TASK_BATCH_CONTROL_TOPIC, "topic1");
    // lack swift root
    ASSERT_FALSE(task->handleTarget(target));
    target.addTargetDescription(BS_TASK_BATCH_CONTROL_SWIFT_ROOT, "swiftzk");
    // start timestamp
    target.updateTargetDescription(BS_TASK_BATCH_CONTROL_START_TS, "12356");

    EXPECT_CALL(*task, initSwiftReader("topic1", "swiftzk")).WillOnce(Return(false)).WillRepeatedly(Return(true));
    // create swift reader failed
    MockBatchControlWorker* worker = new ::testing::StrictMock<MockBatchControlWorker>;
    task->_worker.reset(worker);
    ASSERT_FALSE(task->handleTarget(target));
    // start worker failed
    EXPECT_CALL(*worker, start(_, 12356, "1.2.3.4", 8888)).WillOnce(Return(false)).WillOnce(Return(true));
    ASSERT_FALSE(task->handleTarget(target));
    ASSERT_TRUE(task->handleTarget(target));
    ASSERT_TRUE(task->_hasStarted);
    ASSERT_EQ(8888, task->_port);
    ASSERT_EQ("1.2.3.4", task->_adminAddress);

    EXPECT_CALL(*worker, resetHost(_, _)).WillOnce(Return(false)).WillOnce(Return(true));
    // ip & port not change
    ASSERT_TRUE(task->handleTarget(target));
    // reset host failed
    target.updateTargetDescription(BS_TASK_BATCH_CONTROL_ADDRESS, "1.2.3.5");
    target.updateTargetDescription(BS_TASK_BATCH_CONTROL_PORT, "4567");
    ASSERT_FALSE(task->handleTarget(target));
    ASSERT_TRUE(task->handleTarget(target));
    ASSERT_EQ(4567, task->_port);
    ASSERT_EQ("1.2.3.5", task->_adminAddress);

    ASSERT_EQ("topic1", task->_dataDesc[SWIFT_TOPIC_NAME]);
    ASSERT_EQ("swiftzk", task->_dataDesc[SWIFT_ZOOKEEPER_ROOT]);
    ASSERT_EQ("12356", task->_dataDesc[SWIFT_START_TIMESTAMP]);
    ASSERT_EQ("swift", task->_dataDesc[READ_SRC]);
    ASSERT_EQ("swift", task->_dataDesc[READ_SRC_TYPE]);
    ASSERT_EQ("true", task->_dataDesc[SRC_BATCH_MODE]);
    delete task;
}

TEST_F(BatchControlTaskTest, testHandleTargetWithoutTs)
{
    MockBatchControlTask* task = new ::testing::StrictMock<MockBatchControlTask>;
    config::TaskTarget target;
    // lack admin address
    ASSERT_FALSE(task->handleTarget(target));
    target.addTargetDescription(BS_TASK_BATCH_CONTROL_ADDRESS, "1.2.3.4");
    // lack port or invalid port
    ASSERT_FALSE(task->handleTarget(target));
    target.addTargetDescription(BS_TASK_BATCH_CONTROL_PORT, "");
    ASSERT_FALSE(task->handleTarget(target));
    target.updateTargetDescription(BS_TASK_BATCH_CONTROL_PORT, "8888");
    // lack topicname
    ASSERT_FALSE(task->handleTarget(target));
    target.addTargetDescription(BS_TASK_BATCH_CONTROL_TOPIC, "topic1");
    // lack swift root
    ASSERT_FALSE(task->handleTarget(target));
    target.addTargetDescription(BS_TASK_BATCH_CONTROL_SWIFT_ROOT, "swiftzk");

    EXPECT_CALL(*task, initSwiftReader("topic1", "swiftzk")).WillOnce(Return(false)).WillRepeatedly(Return(true));
    // create swift reader failed
    MockBatchControlWorker* worker = new ::testing::StrictMock<MockBatchControlWorker>;
    task->_worker.reset(worker);
    ASSERT_FALSE(task->handleTarget(target));
    // start worker failed
    EXPECT_CALL(*worker, start(_, 0, "1.2.3.4", 8888)).WillOnce(Return(false)).WillOnce(Return(true));
    ASSERT_FALSE(task->handleTarget(target));
    ASSERT_TRUE(task->handleTarget(target));
    ASSERT_TRUE(task->_hasStarted);
    ASSERT_EQ(8888, task->_port);
    ASSERT_EQ("1.2.3.4", task->_adminAddress);
    delete task;
}
}} // namespace build_service::task_base
