#include "build_service/admin/taskcontroller/SingleGeneralTaskManager.h"

#include "autil/StringUtil.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/ConfigReaderAccessor.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/config/ResourceReaderManager.h"
#include "build_service/test/unittest.h"

using namespace build_service::config;

namespace build_service::admin {

class SingleGeneralTaskManagerTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp() override;
    void tearDown() override;

private:
    proto::OperationTarget getOpTarget(const config::TaskTarget& target);
    std::vector<int64_t> getNodeTargetOps(const TaskController::Node& node);
    void finishNodeOps(const std::string& workerEpoch, const std::vector<int64_t>& ops, TaskController::Node* node);
    proto::OperationPlan prepareOpPlanWithBackupNode() const;
    void addBackupNode(TaskController::Nodes& nodes, size_t srcNodeIdx, uint32_t backupId) const;
    void mockDoMerge(SingleGeneralTaskManager* manager, TaskController::Nodes& nodes);
    void mockEndMerge(SingleGeneralTaskManager* manager, TaskController::Nodes& nodes);
    std::string nextWorkerEpochId() { return std::string("epochId_") + std::to_string(_epochId++); }

private:
    TaskResourceManagerPtr _resMgr;
    size_t _epochId = 0;
};

void SingleGeneralTaskManagerTest::setUp()
{
    _resMgr.reset(new TaskResourceManager);
    config::ConfigReaderAccessorPtr configResource(new config::ConfigReaderAccessor("simple"));
    std::string configPath = GET_TEST_DATA_PATH() + "admin_test/config";
    config::ResourceReaderPtr configReader = config::ResourceReaderManager::getResourceReader(configPath);
    configResource->addConfig(configReader, true);
    _resMgr->addResource(configResource);
    _epochId = 0;
}

void SingleGeneralTaskManagerTest::tearDown() {}

proto::OperationTarget SingleGeneralTaskManagerTest::getOpTarget(const config::TaskTarget& target)
{
    std::string content;
    proto::OperationTarget opTarget;
    if (!target.getTargetDescription(BS_GENERAL_TASK_OPERATION_TARGET, content)) {
        return opTarget;
    }
    EXPECT_TRUE(opTarget.ParseFromString(content));
    return opTarget;
}

std::vector<int64_t> SingleGeneralTaskManagerTest::getNodeTargetOps(const TaskController::Node& node)
{
    auto target = getOpTarget(node.taskTarget);
    std::vector<int64_t> ret;
    for (const auto& op : target.ops()) {
        ret.push_back(op.id());
    }
    return ret;
}

void SingleGeneralTaskManagerTest::finishNodeOps(const std::string& workerEpoch, const std::vector<int64_t>& ops,
                                                 TaskController::Node* node)
{
    proto::OperationCurrent current;
    current.set_workerepochid(workerEpoch);
    current.set_availablememory(100000);
    for (const auto& opId : ops) {
        auto result = current.add_opresults();
        result->set_id(opId);
        result->set_status(proto::OP_FINISHED);
        result->set_resultinfo("");
    }
    std::string content;
    ASSERT_TRUE(current.SerializeToString(&content));
    node->statusDescription = content;
}

/*
level0        op0
             /  \
level1    op1      op2
          / \      / \
level2  op3 op4  op5 op6
          \   \  /   /
level3         op7
 */
proto::OperationPlan SingleGeneralTaskManagerTest::prepareOpPlanWithBackupNode() const
{
    const std::string type("test_type");
    proto::OperationPlan plan;
    auto addOp = [&plan, &type](int64_t opId, const std::vector<int64_t>& depends) {
        auto op = plan.add_ops();
        op->set_id(opId);
        op->set_type(type);
        op->set_memory(1);
        for (auto id : depends) {
            op->add_depends(id);
        }
    };
    // level 0
    addOp(0, {});
    // level 1
    addOp(1, {0});
    addOp(2, {0});
    // level 2
    addOp(3, {0, 1});
    addOp(4, {0, 1});
    addOp(5, {0, 2});
    addOp(6, {0, 2});
    // level 3
    addOp(7, {0, 1, 2, 3, 4, 5, 6});

    return plan;
}

// total three nodes: srcNode1 srcNode2 backupNode1
void SingleGeneralTaskManagerTest::mockDoMerge(SingleGeneralTaskManager* manager, TaskController::Nodes& nodes)
{
    // srcNode1 finish op0
    finishNodeOps(nextWorkerEpochId(), /*ops*/ {0}, &(nodes[0]));
    // srcNode1 get op1, backupNode1 get op1, srcNode2 get op2
    ASSERT_FALSE(manager->operate(&nodes, /*parallelNum*/ 2, 2));

    finishNodeOps(nextWorkerEpochId(), /*ops*/ {0, 1}, &(nodes[2]));
    // srcNode1 hold op3, backupNode1 get op3, srcNode2 get op2/op4
    ASSERT_FALSE(manager->operate(&nodes, /*parallelNum*/ 2, 2));
}
// all ops before EndMerge have been finished
void SingleGeneralTaskManagerTest::mockEndMerge(SingleGeneralTaskManager* manager, TaskController::Nodes& nodes)
{
    // srcNode1 finish op0
    finishNodeOps(nextWorkerEpochId(), /*ops*/ {0}, &(nodes[0]));
    // srcNode1 get op1, backupNode1 get op1, srcNode2 get op2
    ASSERT_FALSE(manager->operate(&nodes, /*parallelNum*/ 2, 2));

    finishNodeOps(nextWorkerEpochId(), /*ops*/ {0, 1}, &(nodes[2]));
    // srcNode1 get op3, backupNode1 get op3, srcNode2 get op2/op4
    ASSERT_FALSE(manager->operate(&nodes, /*parallelNum*/ 2, 2));

    finishNodeOps(nextWorkerEpochId(), /*ops*/ {3}, &(nodes[2]));
    finishNodeOps(nextWorkerEpochId(), /*ops*/ {2, 4}, &(nodes[1]));
    ASSERT_FALSE(manager->operate(&nodes, /*parallelNum*/ 2, 2));

    finishNodeOps(nextWorkerEpochId(), /*ops*/ {5}, &(nodes[0]));
    finishNodeOps(nextWorkerEpochId(), /*ops*/ {6}, &(nodes[1]));
    ASSERT_FALSE(manager->operate(&nodes, /*parallelNum*/ 2, 2));
}

// append backup to nodes
void SingleGeneralTaskManagerTest::addBackupNode(TaskController::Nodes& nodes, size_t srcNodeIdx,
                                                 uint32_t backupId) const
{
    TaskController::Node backupNode = nodes[srcNodeIdx];
    backupNode.backupId = backupId;
    backupNode.sourceNodeId = srcNodeIdx;
    backupNode.nodeId = 0;
    nodes.emplace_back(std::move(backupNode));
}

TEST_F(SingleGeneralTaskManagerTest, testSimplePlan)
{
    auto resourceManager = std::make_shared<TaskResourceManager>();
    SingleGeneralTaskManager manager(/*name*/ "test", resourceManager);
    KeyValueMap param;
    param["task_work_root"] = GET_TEMP_DATA_PATH() + "task";
    param["base_version_id"] = "0";
    param["cluster_name"] = "simple";

    proto::OperationPlan plan;
    auto op = plan.add_ops();
    op->set_id(1);
    op->set_type("test");
    op->set_memory(100);
    ASSERT_EQ(SingleGeneralTaskManager::State::RUNNING, manager._currentState);

    ASSERT_TRUE(manager.init(param, &plan));
    ASSERT_EQ("test", manager.getId());

    TaskController::Nodes nodes;
    ASSERT_FALSE(manager.operate(&nodes, /*parallelNum*/ 2, 2));
    ASSERT_EQ(2u, nodes.size());
    ASSERT_EQ(0, nodes[0].nodeId);
    ASSERT_EQ(1, nodes[1].nodeId);

    auto target = nodes[0].taskTarget;
    auto opTarget = getOpTarget(target);
    ASSERT_EQ(0u, opTarget.ops_size());

    opTarget = getOpTarget(nodes[1].taskTarget);
    ASSERT_EQ(0u, opTarget.ops_size());

    proto::OperationCurrent opCurrent;
    opCurrent.set_workerepochid("1234");
    opCurrent.set_availablememory(1000);
    opCurrent.SerializeToString(&(nodes[0].statusDescription));
    ASSERT_FALSE(manager.operate(&nodes, /*parallelNum*/ 2, 2));
    ASSERT_EQ(SingleGeneralTaskManager::State::RUNNING, manager._currentState);
    ASSERT_EQ(2u, nodes.size());
    ASSERT_EQ(0, nodes[0].nodeId);
    ASSERT_EQ(1, nodes[1].nodeId);

    opTarget = getOpTarget(nodes[0].taskTarget);
    ASSERT_EQ(1u, opTarget.ops_size());
    ASSERT_EQ(1, opTarget.ops(0).id());
    ASSERT_EQ("test", opTarget.ops(0).type());
    ASSERT_EQ(100, opTarget.ops(0).memory());

    opTarget = getOpTarget(nodes[1].taskTarget);
    ASSERT_EQ(0u, opTarget.ops_size());
}

TEST_F(SingleGeneralTaskManagerTest, testRecover)
{
    auto resourceManager = std::make_shared<TaskResourceManager>();
    SingleGeneralTaskManager manager(/*name*/ "test", resourceManager);
    KeyValueMap param;
    param["task_work_root"] = GET_TEMP_DATA_PATH() + "task";
    param["base_version_id"] = "0";
    param["cluster_name"] = "simple";

    proto::OperationPlan plan;
    plan.set_taskname("fullMerge");
    plan.set_tasktype("merge");
    auto op0 = plan.add_ops();
    op0->set_id(0);
    op0->set_type("testtype");
    op0->set_memory(1);
    auto op1 = plan.add_ops();
    op1->set_id(1);
    op1->set_type("testtype");
    op1->set_memory(1);
    op1->add_depends(0);

    ASSERT_TRUE(manager.init(param, &plan));
    ASSERT_EQ("test", manager.getId());

    TaskController::Nodes nodes;
    ASSERT_FALSE(manager.operate(&nodes, /*parallelNum*/ 2, 2));
    ASSERT_EQ(SingleGeneralTaskManager::State::RUNNING, manager._currentState);

    ASSERT_EQ(2u, nodes.size());
    finishNodeOps("abcd", {}, &nodes[0]);
    finishNodeOps("efgh", {}, &nodes[1]);

    ASSERT_FALSE(manager.operate(&nodes, /*parallelNum*/ 2, 2));
    auto target1 = getOpTarget(nodes[0].taskTarget);
    ASSERT_EQ(target1.taskname(), std::string("fullMerge"));
    ASSERT_EQ(target1.tasktype(), std::string("merge"));

    auto op0TargetOps = getNodeTargetOps(nodes[0]);
    EXPECT_THAT(op0TargetOps, testing::UnorderedElementsAre(0));
    auto op1TargetOps = getNodeTargetOps(nodes[1]);
    EXPECT_TRUE(op1TargetOps.empty());

    finishNodeOps(/*workerEpoch*/ "5678", /*ops*/ {0}, &(nodes[0]));
    ASSERT_FALSE(manager.operate(&nodes, /*parallelNum*/ 2, 2));
    ASSERT_EQ(SingleGeneralTaskManager::State::RUNNING, manager._currentState);

    SingleGeneralTaskManager recoveredManager(/*name*/ "test", resourceManager);
    ASSERT_TRUE(recoveredManager.init(param, /*plan*/ nullptr));
    ASSERT_EQ("test", recoveredManager.getId());

    ASSERT_EQ(SingleGeneralTaskManager::State::RUNNING, recoveredManager._currentState);
    TaskController::Nodes recoveredNodes;
    ASSERT_FALSE(recoveredManager.operate(&recoveredNodes, /*parallelNum*/ 2, 2));
    auto target = getOpTarget(recoveredNodes[0].taskTarget);
    ASSERT_EQ(target.taskname(), std::string("fullMerge"));
    ASSERT_EQ(target.tasktype(), std::string("merge"));
    op0TargetOps = getNodeTargetOps(recoveredNodes[0]);
    ASSERT_THAT(op0TargetOps, testing::UnorderedElementsAre(1));
}

TEST_F(SingleGeneralTaskManagerTest, testRecoverWithBackupWithDoMerge)
{
    auto resourceManager = std::make_shared<TaskResourceManager>();
    SingleGeneralTaskManager manager(/*name*/ "test", resourceManager);
    KeyValueMap param;
    param["task_work_root"] = GET_TEMP_DATA_PATH() + "task";
    param["base_version_id"] = "0";
    param["cluster_name"] = "simple";

    proto::OperationPlan plan = prepareOpPlanWithBackupNode();
    ASSERT_TRUE(manager.init(param, &plan));
    ASSERT_EQ("test", manager.getId());

    TaskController::Nodes nodes;
    ASSERT_FALSE(manager.operate(&nodes, /*parallelNum*/ 2, 2));
    ASSERT_EQ(SingleGeneralTaskManager::State::RUNNING, manager._currentState);
    ASSERT_EQ(2u, nodes.size());

    finishNodeOps(nextWorkerEpochId(), {}, &nodes[0]);
    finishNodeOps(nextWorkerEpochId(), {}, &nodes[1]);
    ASSERT_FALSE(manager.operate(&nodes, /*parallelNum*/ 2, 2));

    addBackupNode(nodes, /*source node idx*/ 0, /*backupId*/ 1);
    ASSERT_FALSE(manager.operate(&nodes, /*parallelNum*/ 2, /*running op limit*/ 2));

    mockDoMerge(&manager, nodes);

    SingleGeneralTaskManager recoveredManager(/*name*/ "test", resourceManager);
    ASSERT_TRUE(recoveredManager.init(param, /*plan*/ nullptr));
    ASSERT_EQ("test", recoveredManager.getId());

    TaskController::Nodes recoveredNodes;
    ASSERT_FALSE(recoveredManager.operate(&recoveredNodes, /*parallelNum*/ 2, 2));
    ASSERT_EQ(2, recoveredNodes.size());
    auto workNode0targetOps = getNodeTargetOps(recoveredNodes[0]);
    EXPECT_THAT(workNode0targetOps, testing::UnorderedElementsAre(3));
    auto workNode1targetOps = getNodeTargetOps(recoveredNodes[1]);
    EXPECT_THAT(workNode1targetOps, testing::UnorderedElementsAre(2, 4));
}

TEST_F(SingleGeneralTaskManagerTest, testRecoverWithBackupWithEndMerge)
{
    auto resourceManager = std::make_shared<TaskResourceManager>();
    SingleGeneralTaskManager manager(/*name*/ "test", resourceManager);
    KeyValueMap param;
    param["task_work_root"] = GET_TEMP_DATA_PATH() + "task";
    param["base_version_id"] = "0";
    param["cluster_name"] = "simple";

    proto::OperationPlan plan = prepareOpPlanWithBackupNode();
    ASSERT_TRUE(manager.init(param, &plan));
    ASSERT_EQ("test", manager.getId());

    TaskController::Nodes nodes;
    ASSERT_FALSE(manager.operate(&nodes, /*parallelNum*/ 2, 2));
    ASSERT_EQ(SingleGeneralTaskManager::State::RUNNING, manager._currentState);
    ASSERT_EQ(2u, nodes.size());

    finishNodeOps(nextWorkerEpochId(), {}, &nodes[0]);
    finishNodeOps(nextWorkerEpochId(), {}, &nodes[1]);
    ASSERT_FALSE(manager.operate(&nodes, /*parallelNum*/ 2, 2));

    addBackupNode(nodes, /*source node idx*/ 0, /*backupId*/ 1);
    ASSERT_FALSE(manager.operate(&nodes, /*parallelNum*/ 2, /*running op limit*/ 2));

    mockEndMerge(&manager, nodes);

    SingleGeneralTaskManager recoveredManager(/*name*/ "test", resourceManager);
    ASSERT_TRUE(recoveredManager.init(param, /*plan*/ nullptr));
    ASSERT_EQ("test", recoveredManager.getId());

    TaskController::Nodes recoveredNodes;
    ASSERT_FALSE(recoveredManager.operate(&recoveredNodes, /*parallelNum*/ 2, 2));
    ASSERT_EQ(2, recoveredNodes.size());
    auto workNode0targetOps = getNodeTargetOps(recoveredNodes[0]);
    EXPECT_THAT(workNode0targetOps, testing::UnorderedElementsAre(7));
    auto workNode1targetOps = getNodeTargetOps(recoveredNodes[1]);
    EXPECT_THAT(workNode1targetOps, testing::UnorderedElementsAre());
}

TEST_F(SingleGeneralTaskManagerTest, testUpdateConfig)
{
    auto resourceManager = std::make_shared<TaskResourceManager>();
    SingleGeneralTaskManager manager(/*name*/ "test", resourceManager);
    KeyValueMap param;
    param["task_work_root"] = GET_TEMP_DATA_PATH() + "task";
    param["base_version_id"] = "0";
    param["cluster_name"] = "simple";

    proto::OperationPlan plan;
    auto op0 = plan.add_ops();
    op0->set_id(0);
    op0->set_type("testtype");
    op0->set_memory(1);
    auto op1 = plan.add_ops();
    op1->set_id(1);
    op1->set_type("testtype");
    op1->set_memory(1);
    op1->add_depends(0);
    auto op2 = plan.add_ops();
    op2->set_id(2);
    op2->set_type("testtype");
    op2->set_memory(1);
    op2->add_depends(0);
    auto op3 = plan.add_ops();
    op3->set_id(3);
    op3->set_type("testtype");
    op3->set_memory(1);
    op3->add_depends(0);
    auto op4 = plan.add_ops();
    op4->set_id(4);
    op4->set_type("testtype");
    op4->set_memory(1);
    op4->add_depends(0);

    ASSERT_TRUE(manager.init(param, &plan));
    ASSERT_EQ("test", manager.getId());

    TaskController::Nodes nodes;
    ASSERT_FALSE(manager.operate(&nodes, /*parallelNum*/ 2, 2));
    ASSERT_EQ(SingleGeneralTaskManager::State::RUNNING, manager._currentState);
    ASSERT_EQ(2u, nodes.size());
    ASSERT_EQ(0, nodes[0].nodeId);
    ASSERT_EQ(1, nodes[1].nodeId);
    auto opTarget = getOpTarget(nodes[0].taskTarget);
    ASSERT_EQ(0u, opTarget.ops_size());
    opTarget = getOpTarget(nodes[1].taskTarget);
    ASSERT_EQ(0u, opTarget.ops_size());

    finishNodeOps("1234", {}, &nodes[0]);
    finishNodeOps("5678", {}, &nodes[1]);

    ASSERT_FALSE(manager.operate(&nodes, /*parallelNum*/ 2, 2));
    auto op0TargetOps = getNodeTargetOps(nodes[0]);
    EXPECT_THAT(op0TargetOps, testing::UnorderedElementsAre(0));
    auto op1TargetOps = getNodeTargetOps(nodes[1]);
    EXPECT_TRUE(op1TargetOps.empty());

    finishNodeOps(/*workerEpoch*/ "1234", /*ops*/ {0}, &(nodes[0]));
    finishNodeOps("5678", {}, &nodes[1]);

    ASSERT_FALSE(manager.operate(&nodes, /*parallelNum*/ 2, 2));
    auto opTarget0 = getOpTarget(nodes[0].taskTarget);
    ASSERT_EQ(2u, opTarget0.ops_size());
    auto opTarget1 = getOpTarget(nodes[1].taskTarget);
    ASSERT_EQ(2u, opTarget1.ops_size());

    ASSERT_FALSE(manager.operate(&nodes, /*parallelNum*/ 4, 2));
    ASSERT_EQ(4u, nodes.size());
    opTarget0 = getOpTarget(nodes[0].taskTarget);
    ASSERT_EQ(0u, opTarget0.ops_size());
    opTarget1 = getOpTarget(nodes[1].taskTarget);
    ASSERT_EQ(0u, opTarget1.ops_size());
    auto opTarget2 = getOpTarget(nodes[2].taskTarget);
    ASSERT_EQ(0u, opTarget2.ops_size());
    auto opTarget3 = getOpTarget(nodes[3].taskTarget);
    ASSERT_EQ(0u, opTarget3.ops_size());

    finishNodeOps("1234", {}, &nodes[0]);
    finishNodeOps("5678", {}, &nodes[1]);
    finishNodeOps("4321", {}, &nodes[2]);
    finishNodeOps("8765", {}, &nodes[3]);

    ASSERT_FALSE(manager.operate(&nodes, /*parallelNum*/ 4, 2));
    ASSERT_EQ(4u, nodes.size());
    opTarget0 = getOpTarget(nodes[0].taskTarget);
    ASSERT_EQ(1u, opTarget0.ops_size());
    opTarget1 = getOpTarget(nodes[1].taskTarget);
    ASSERT_EQ(1u, opTarget1.ops_size());
    opTarget2 = getOpTarget(nodes[2].taskTarget);
    ASSERT_EQ(1u, opTarget2.ops_size());
    opTarget3 = getOpTarget(nodes[3].taskTarget);
    ASSERT_EQ(1u, opTarget3.ops_size());
}

TEST_F(SingleGeneralTaskManagerTest, testSetTaskNameAndTaskType)
{
    auto resourceManager = std::make_shared<TaskResourceManager>();
    SingleGeneralTaskManager manager(/*name*/ "test", resourceManager);
    KeyValueMap param;
    param["task_work_root"] = GET_TEMP_DATA_PATH() + "task";
    param["base_version_id"] = "0";
    param["cluster_name"] = "simple";

    proto::OperationPlan plan;
    plan.set_taskname("task_name_111");
    plan.set_tasktype("task_type_222");

    ASSERT_TRUE(manager.init(param, &plan));
    ASSERT_EQ("test", manager.getId());
    ASSERT_EQ("task_name_111", manager._taskName);
    ASSERT_EQ("task_type_222", manager._taskType);

    SingleGeneralTaskManager recoveredManager(/*name*/ "test", resourceManager);
    ASSERT_TRUE(recoveredManager.init(param, /*plan*/ nullptr));
    ASSERT_EQ("test", recoveredManager.getId());
    ASSERT_EQ("task_name_111", recoveredManager._taskName);
    ASSERT_EQ("task_type_222", recoveredManager._taskType);
}

TEST_F(SingleGeneralTaskManagerTest, testHandlePlan)
{
    auto resourceManager = std::make_shared<TaskResourceManager>();
    SingleGeneralTaskManager manager(/*name*/ "test", resourceManager);
    KeyValueMap param;
    param["task_work_root"] = GET_TEMP_DATA_PATH() + "task";
    param["base_version_id"] = "0";
    param["cluster_name"] = "simple";

    proto::OperationPlan plan;
    auto op0 = plan.add_ops();
    op0->set_id(0);
    op0->set_type("testtype");
    op0->set_memory(1);

    auto op1 = plan.add_ops();
    op1->set_id(1);
    op1->set_type("testtype");
    op1->add_depends(0);
    op1->set_memory(1);

    auto op2 = plan.add_ops();
    op2->set_id(2);
    op2->set_type("testtype");
    op2->add_depends(0);
    op2->set_memory(1);

    auto op3 = plan.add_ops();
    op3->set_id(3);
    op3->set_type("testtype");
    op3->add_depends(1);
    op3->add_depends(0);
    op3->set_memory(1);

    auto op4 = plan.add_ops();
    op4->set_id(4);
    op4->set_type("testtype");
    op4->add_depends(0);
    op4->set_memory(1);

    auto op5 = plan.add_ops();
    op5->set_id(5);
    op5->set_type("testtype");
    op5->add_depends(0);
    op5->set_memory(1);

    ASSERT_TRUE(manager.init(param, &plan));
    ASSERT_EQ("test", manager.getId());

    TaskController::Nodes nodes;
    ASSERT_FALSE(manager.operate(&nodes, /*parallelNum*/ 2, 2));
    ASSERT_EQ(SingleGeneralTaskManager::State::RUNNING, manager._currentState);
    ASSERT_EQ(2u, nodes.size());
    ASSERT_EQ(0, nodes[0].nodeId);
    ASSERT_EQ(1, nodes[1].nodeId);
    auto opTarget = getOpTarget(nodes[0].taskTarget);
    ASSERT_EQ(0u, opTarget.ops_size());
    opTarget = getOpTarget(nodes[1].taskTarget);
    ASSERT_EQ(0u, opTarget.ops_size());

    finishNodeOps(/*workerEpoch*/ "1234", {}, &nodes[0]);
    finishNodeOps("5678", {}, &nodes[1]);

    ASSERT_FALSE(manager.operate(&nodes, /*parallelNum*/ 2, 2));
    auto node0TargetOps = getNodeTargetOps(nodes[0]);
    auto node1TargetOps = getNodeTargetOps(nodes[1]);
    EXPECT_THAT(node0TargetOps, testing::UnorderedElementsAre(0));
    ASSERT_TRUE(node1TargetOps.empty());

    finishNodeOps("1234", {0}, &nodes[0]);
    finishNodeOps("5678", {}, &nodes[1]);

    ASSERT_FALSE(manager.operate(&nodes, /*parallelNum*/ 2, 2));
    node0TargetOps = getNodeTargetOps(nodes[0]);
    node1TargetOps = getNodeTargetOps(nodes[1]);

    // 2 op for each
    ASSERT_EQ(2u, node0TargetOps.size());
    ASSERT_EQ(2u, node1TargetOps.size());

    finishNodeOps("1234", node0TargetOps, &nodes[0]);
    finishNodeOps("5678", node1TargetOps, &nodes[1]);

    ASSERT_FALSE(manager.operate(&nodes, /*parallelNum*/ 2, 2));
    node0TargetOps = getNodeTargetOps(nodes[0]);
    ASSERT_EQ(1u, node0TargetOps.size());

    finishNodeOps("1234", node0TargetOps, &nodes[0]);
    ASSERT_FALSE(manager.operate(&nodes, /*parallelNum*/ 4, 2));
}

} // namespace build_service::admin
