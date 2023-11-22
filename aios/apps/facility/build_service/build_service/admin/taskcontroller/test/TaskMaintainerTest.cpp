#include "build_service/admin/taskcontroller/TaskMaintainer.h"

#include <ext/alloc_traits.h>
#include <memory>
#include <string>
#include <vector>

#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/admin/controlflow/TaskResourceManager.h"
#include "build_service/admin/taskcontroller/GeneralTaskController.h"
#include "build_service/common/CpuSpeedEstimater.h"
#include "build_service/config/ConfigDefine.h"
#include "build_service/config/ConfigReaderAccessor.h"
#include "build_service/config/CounterConfig.h"
#include "build_service/config/ResourceReaderManager.h"
#include "build_service/config/TaskConfig.h"
#include "build_service/config/TaskTarget.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/proto/Heartbeat.pb.h"
#include "build_service/proto/JsonizableProtobuf.h"
#include "build_service/proto/WorkerNode.h"
#include "build_service/test/unittest.h"
#include "unittest/unittest.h"

using namespace build_service::proto;
using namespace build_service::config;
using namespace build_service::common;

namespace build_service { namespace admin {

class TaskMaintainerTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp();
    void tearDown();

private:
    BuildId _buildId;
    TaskResourceManagerPtr _resMgr;
    std::string _configPath;
};

void TaskMaintainerTest::setUp()
{
    _buildId.set_generationid(1);
    _buildId.set_datatable("simple");

    _resMgr.reset(new TaskResourceManager);
    config::ConfigReaderAccessorPtr configResource(new config::ConfigReaderAccessor("simple"));
    _configPath = GET_TEST_DATA_PATH() + "admin_test/config_with_backup_node";
    ResourceReaderPtr configReader = ResourceReaderManager::getResourceReader(_configPath);
    configResource->addConfig(configReader, true);
    _resMgr->addResource(configResource);
}

void TaskMaintainerTest::tearDown() {}

TEST_F(TaskMaintainerTest, TestBackupNodeSimple)
{
    TaskMaintainer task(_buildId, "123", "general_task", _resMgr);
    task._nodesStartTimestamp = 0;

    GeneralTaskParam param;
    param.taskEpochId = "777";
    param.partitionIndexRoot = "index_root";
    param.sourceVersionIds = {0};

    proto::OperationPlan plan;
    plan.set_taskname("full_merge");
    plan.set_tasktype("merge");
    auto op0 = plan.add_ops();
    op0->set_id(0);
    op0->set_type("testtype");
    op0->set_memory(1);
    auto op1 = plan.add_ops();
    op1->set_id(1);
    op1->set_type("testtype");
    op1->set_memory(1);
    auto op2 = plan.add_ops();
    op2->set_id(2);
    op2->set_type("testtype");
    op2->set_memory(1);
    op2->add_depends(0);
    auto op3 = plan.add_ops();
    op3->set_id(3);
    op3->set_type("testtype");
    op3->set_memory(1);
    op3->add_depends(1);
    auto op4 = plan.add_ops();
    op4->set_id(4);
    op4->set_type("testtype");
    op4->set_memory(1);
    op4->add_depends(2);
    op4->add_depends(3);

    param.plan = proto::JsonizableProtobuf<proto::OperationPlan>(plan);
    std::string taskId = "task_id";
    std::string taskName = "task_name";
    ASSERT_TRUE(task.init("cluster1", _configPath, autil::legacy::ToJsonString(param)));
    // 2 normal node
    auto generalTaskController = std::dynamic_pointer_cast<GeneralTaskController>(task._taskController);
    ASSERT_EQ(2, generalTaskController->_parallelNum);
    ASSERT_EQ(1, generalTaskController->_threadNum);

    WorkerNodes nodes;
    // round 1
    ASSERT_FALSE(task.run(nodes));
    // 2 normal node + 2 backup node
    ASSERT_EQ(nodes.size(), 4);

    TaskCurrent taskCurrent;
    std::string taskCurrentStr;
    taskCurrent.set_configpath(_configPath);
    proto::OperationCurrent current;
    current.set_workerepochid("worker");
    current.set_availablememory(100);
    std::string content;
    ASSERT_TRUE(current.SerializeToString(&content));
    taskCurrent.set_statusdescription(content);
    taskCurrent.SerializeToString(&taskCurrentStr);

    for (auto node : nodes) {
        TaskNode* taskNode = (TaskNode*)node.get();
        taskNode->setCurrentStatusStr(taskCurrentStr, "");
    }

    // round 2
    ASSERT_FALSE(task.run(nodes));

    config::TaskTarget target0;
    FromJsonString(target0, ((proto::TaskNode*)nodes[0].get())->getTargetStatus().targetdescription());
    std::string targetInfoStr0;
    target0.getTargetDescription(BS_GENERAL_TASK_OPERATION_TARGET, targetInfoStr0);

    config::TaskTarget target1;
    FromJsonString(target1, ((proto::TaskNode*)nodes[1].get())->getTargetStatus().targetdescription());
    std::string targetInfoStr1;
    target1.getTargetDescription(BS_GENERAL_TASK_OPERATION_TARGET, targetInfoStr1);

    ASSERT_NE(targetInfoStr0, targetInfoStr1);

    config::TaskTarget target2;
    FromJsonString(target2, ((proto::TaskNode*)nodes[2].get())->getTargetStatus().targetdescription());
    std::string targetInfoStr2;
    target2.getTargetDescription(BS_GENERAL_TASK_OPERATION_TARGET, targetInfoStr2);

    config::TaskTarget target3;
    FromJsonString(target3, ((proto::TaskNode*)nodes[3].get())->getTargetStatus().targetdescription());
    std::string targetInfoStr3;
    target3.getTargetDescription(BS_GENERAL_TASK_OPERATION_TARGET, targetInfoStr3);
    // node 2 is the backup node of node 0
    ASSERT_EQ(targetInfoStr0, targetInfoStr2);
    // node 3 is the backup node of node 1
    ASSERT_EQ(targetInfoStr1, targetInfoStr3);
}

}} // namespace build_service::admin
