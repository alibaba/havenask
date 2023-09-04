#include "build_service/admin/taskcontroller/GeneralNodeGroup.h"

#include "build_service/admin/taskcontroller/OperationTopoManager.h"
#include "build_service/test/unittest.h"

using namespace build_service::config;

namespace build_service::admin {

class GeneralNodeGroupTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp() override;
    void tearDown() override {};

private:
    GeneralNodeGroup createNodeGroup(uint32_t nodeId);
    void generateCurrentAndTarget(const std::string& workerEpoch,
                                  const std::vector<std::pair<int64_t, proto::OperationStatus>>& ops,
                                  TaskController::Node* node);
    bool checkTarget(const proto::OperationTarget& target, const std::vector<int32_t>& opIds);

private:
    const std::string _taskType = "ut";
    const std::string _taskName = "ut";
    std::unique_ptr<OperationTopoManager> _topoMgr;
};

void GeneralNodeGroupTest::setUp() { _topoMgr = std::make_unique<OperationTopoManager>(); }

GeneralNodeGroup GeneralNodeGroupTest::createNodeGroup(uint32_t nodeId)
{
    return GeneralNodeGroup(_taskType, _taskName, nodeId, _topoMgr.get());
}

void GeneralNodeGroupTest::generateCurrentAndTarget(const std::string& workerEpoch,
                                                    const std::vector<std::pair<int64_t, proto::OperationStatus>>& ops,
                                                    TaskController::Node* node)
{
    proto::OperationTarget target;
    proto::OperationCurrent current;
    current.set_workerepochid(workerEpoch);
    current.set_availablememory(100000);
    for (const auto& [opId, status] : ops) {
        auto result = current.add_opresults();
        result->set_id(opId);
        result->set_status(status);
        result->set_resultinfo("");

        auto t = target.add_ops();
        t->set_id(opId);
        t->set_type(/*required type*/ "ut");
        t->set_memory(100);

        if (status == proto::OP_FINISHED) {
            _topoMgr->finish(opId, workerEpoch, /*resultInfo*/ "");
        }
    }
    std::string content;
    [[maybe_unused]] auto r = target.SerializeToString(&content);
    node->taskTarget.updateTargetDescription(BS_GENERAL_TASK_OPERATION_TARGET, content);

    ASSERT_TRUE(current.SerializeToString(&content));
    node->statusDescription = content;
}

TEST_F(GeneralNodeGroupTest, testAddNode)
{
    TaskController::Node node;
    auto nodeGroup = createNodeGroup(/*nodeId*/ 0);
    ASSERT_EQ(_taskType, nodeGroup._taskType);
    ASSERT_EQ(_taskName, nodeGroup._taskName);
    ASSERT_EQ(0, nodeGroup._nodeId);
    ASSERT_EQ(0, nodeGroup._nodes.size());
    ASSERT_EQ(0, nodeGroup._newTargets.size());
    ASSERT_EQ(0, nodeGroup._runningOpCounts.size());

    nodeGroup.addNode(&node);
    ASSERT_EQ(1, nodeGroup._nodes.size());
    ASSERT_EQ(1, nodeGroup._newTargets.size());
    ASSERT_EQ(1, nodeGroup._runningOpCounts.size());
    ASSERT_EQ(0, nodeGroup.getMinRunningOpCount());

    nodeGroup.addNode(&node);
    ASSERT_EQ(2, nodeGroup._nodes.size());
    ASSERT_EQ(2, nodeGroup._newTargets.size());
    ASSERT_EQ(2, nodeGroup._runningOpCounts.size());
    ASSERT_EQ(0, nodeGroup.getMinRunningOpCount());
    ASSERT_TRUE(nodeGroup.handleFinishedOp(/*walRecord*/ nullptr));
}

TEST_F(GeneralNodeGroupTest, testHandleFinishedOp)
{
    const uint32_t nodeId = 0;
    auto nodeGroup = createNodeGroup(nodeId);

    TaskController::Node mainNode;
    mainNode.nodeId = nodeId;
    nodeGroup.addNode(&mainNode);

    TaskController::Node backupNode1;
    nodeGroup.addNode(&backupNode1);

    TaskController::Node backupNode2;
    nodeGroup.addNode(&backupNode2);

    proto::OperationPlan plan;
    for (size_t i = 0; i < 9; ++i) {
        auto op = plan.add_ops();
        op->set_id(i);
        std::string type = std::string("test_") + std::to_string(i);
        op->set_type(type);
        op->set_memory(100);
    }
    ASSERT_TRUE(_topoMgr->init(plan));

    generateCurrentAndTarget(/*epoch*/ "0", {{0, proto::OP_FINISHED}, {1, proto::OP_ERROR}, {2, proto::OP_RUNNING}},
                             &mainNode);
    generateCurrentAndTarget(/*epoch*/ "1", {{3, proto::OP_ERROR}, {4, proto::OP_FINISHED}, {5, proto::OP_FINISHED}},
                             &backupNode1);
    generateCurrentAndTarget(/*epoch*/ "2", {{6, proto::OP_ERROR}, {7, proto::OP_RUNNING}, {8, proto::OP_RUNNING}},
                             &backupNode2);
    proto::GeneralTaskWalRecord walRecord;
    ASSERT_TRUE(nodeGroup.handleFinishedOp(&walRecord));
    ASSERT_EQ(3, nodeGroup._runningOpCounts.size());
    ASSERT_EQ(2, nodeGroup._runningOpCounts[0]);
    ASSERT_EQ(1, nodeGroup._runningOpCounts[1]);
    ASSERT_EQ(3, nodeGroup._runningOpCounts[2]);

    ASSERT_EQ(2, nodeGroup._newTargets[0].ops_size());
    ASSERT_EQ(1, nodeGroup._newTargets[1].ops_size());
    ASSERT_EQ(3, nodeGroup._newTargets[2].ops_size());
}

TEST_F(GeneralNodeGroupTest, testHandleFinishedOpAbnormal)
{
    const uint32_t nodeId = 0;
    auto nodeGroup = createNodeGroup(nodeId);
    // empty _nodes
    ASSERT_TRUE(nodeGroup.handleFinishedOp(/*walRecord*/ nullptr));
    // empty status desc
    TaskController::Node mainNode;
    nodeGroup.addNode(&mainNode);
    ASSERT_TRUE(nodeGroup.handleFinishedOp(/*walRecord*/ nullptr));
    // invalid status desc, parse failed
    mainNode.statusDescription = "{invalid_desc";
    ASSERT_FALSE(nodeGroup.handleFinishedOp(/*walRecord*/ nullptr));
    // invalid status desc, parse failed
    mainNode.statusDescription = "";
    TaskController::Node backupNode;
    backupNode.statusDescription = "{invalid_desc";
    nodeGroup.addNode(&backupNode);
    ASSERT_FALSE(nodeGroup.handleFinishedOp(/*walRecord*/ nullptr));
    // invalid old target
    proto::OperationCurrent current;
    current.set_workerepochid(/*epoch*/ "0");
    current.set_availablememory(100000);
    ASSERT_TRUE(current.SerializeToString(&mainNode.statusDescription));

    backupNode.statusDescription = "";
    std::string invalidTaskTarget = "{invalid_task_target";
    mainNode.taskTarget.updateTargetDescription(BS_GENERAL_TASK_OPERATION_TARGET, invalidTaskTarget);
    ASSERT_FALSE(nodeGroup.handleFinishedOp(/*walRecord*/ nullptr));
}

TEST_F(GeneralNodeGroupTest, testDispatchExecutableOp)
{
    const uint32_t nodeId = 0;
    auto nodeGroup = createNodeGroup(nodeId);

    TaskController::Node mainNode;
    nodeGroup.addNode(&mainNode);

    TaskController::Node backupNode1;
    nodeGroup.addNode(&backupNode1);

    TaskController::Node backupNode2;
    nodeGroup.addNode(&backupNode2);

    proto::OperationDescription op;
    op.set_id(0);
    std::string type = std::string("test");
    op.set_type(type);
    op.set_memory(100);
    proto::GeneralTaskWalRecord walRecord;

    backupNode2.statusDescription = "meaningless";

    nodeGroup._runningOpCounts[0] = 3; // main node has 3 running ops
    nodeGroup._runningOpCounts[1] = 0; // backup node has 0 running ops
    nodeGroup._runningOpCounts[2] = 2; // backup node has 2 running ops
    nodeGroup.dispatchExecutableOp(&op, /*nodeRunningOpLimit*/ 3, &walRecord);
    ASSERT_EQ(3, nodeGroup._runningOpCounts[0]);
    ASSERT_EQ(0, nodeGroup._runningOpCounts[1]);
    ASSERT_EQ(3, nodeGroup._runningOpCounts[2]);

    ASSERT_EQ(0, nodeGroup._newTargets[0].ops_size());
    ASSERT_EQ(0, nodeGroup._newTargets[1].ops_size());
    ASSERT_EQ(1, nodeGroup._newTargets[2].ops_size());

    auto checkTarget = [&](const config::TaskTarget& taskTarget) -> bool {
        proto::OperationTarget target;
        std::string content;
        if (!taskTarget.getTargetDescription(BS_GENERAL_TASK_OPERATION_TARGET, content)) {
            std::cout << "[ERROR] BS_GENERAL_TASK_OPERATION_TARGET not exist" << std::endl;
            return false;
        }
        if (!target.ParseFromString(content)) {
            std::cout << "[ERROR] parse task target failed, content:" << content << std::endl;
            return false;
        }
        if (target.ops_size() != 1) {
            std::cout << "[ERROR] unexpect ops size:" << target.ops_size() << ", content:" << content << std::endl;
            return false;
        }
        if (target.ops(0).id() != 0) {
            std::cout << "[ERROR] unexpect op id:" << target.ops(0).id() << std::endl;
            return false;
        }
        return true;
    };
    mainNode.statusDescription = "meaningless";
    backupNode1.statusDescription = "meaningless";

    nodeGroup.serializeTarget();
    ASSERT_FALSE(checkTarget(nodeGroup._nodes[0]->taskTarget));
    ASSERT_FALSE(checkTarget(nodeGroup._nodes[1]->taskTarget));
    ASSERT_TRUE(checkTarget(nodeGroup._nodes[2]->taskTarget));
}

TEST_F(GeneralNodeGroupTest, testSyncPendingOpEmpty)
{
    const uint32_t nodeRunningOpLimit = 4;
    auto nodeGroup = createNodeGroup(/*nodeId*/ 0);

    TaskController::Node backupNode2;
    nodeGroup.addNode(&backupNode2);

    TaskController::Node backupNode1;
    nodeGroup.addNode(&backupNode1);

    TaskController::Node mainNode;
    nodeGroup.addNode(&mainNode);

    nodeGroup.syncPendingOp(nodeRunningOpLimit);

    for (size_t i = 0; i < nodeGroup._nodes.size(); ++i) {
        auto& newTarget = nodeGroup._newTargets[i];
        ASSERT_EQ(0, newTarget.ops_size());
        ASSERT_EQ(0, nodeGroup._runningOpCounts[i]);
    }
}
/*
  backup2  null
  backup1  3,4,5
  mainnode 0,1,2
 */
TEST_F(GeneralNodeGroupTest, testSyncPendingOpWithEmptyCurrent)
{
    const uint32_t nodeRunningOpLimit = 4;
    auto nodeGroup = createNodeGroup(/*nodeId*/ 0);

    TaskController::Node backupNode2;
    nodeGroup.addNode(&backupNode2);

    TaskController::Node backupNode1;
    backupNode1.statusDescription = "meaningless";
    nodeGroup.addNode(&backupNode1);

    TaskController::Node mainNode;
    mainNode.statusDescription = "meaningless";
    nodeGroup.addNode(&mainNode);

    proto::OperationPlan plan;
    for (size_t i = 0; i < 9; ++i) {
        auto op = plan.add_ops();
        op->set_id(i);
        std::string type = std::string("test_") + std::to_string(i);
        op->set_type(type);
        op->set_memory(100);
    }
    ASSERT_TRUE(_topoMgr->init(plan));

    generateCurrentAndTarget(/*epoch*/ "0", {{0, proto::OP_RUNNING}, {1, proto::OP_RUNNING}, {2, proto::OP_RUNNING}},
                             &mainNode);
    generateCurrentAndTarget(/*epoch*/ "1",
                             {{0, proto::OP_FINISHED},
                              {1, proto::OP_FINISHED},
                              {2, proto::OP_FINISHED},
                              {3, proto::OP_RUNNING},
                              {4, proto::OP_RUNNING},
                              {5, proto::OP_RUNNING}},
                             &backupNode1);
    proto::GeneralTaskWalRecord walRecord;
    ASSERT_TRUE(nodeGroup.handleFinishedOp(&walRecord));
    nodeGroup.syncPendingOp(nodeRunningOpLimit);
    // backup node 2
    ASSERT_EQ(0, nodeGroup._runningOpCounts[0]);
    ASSERT_EQ(0, nodeGroup._newTargets[0].ops_size());
    // backup node 1
    ASSERT_EQ(3, nodeGroup._runningOpCounts[1]);
    ASSERT_EQ(3, nodeGroup._newTargets[1].ops_size());
    ASSERT_TRUE(checkTarget(nodeGroup._newTargets[1], {3, 4, 5}));
    // main node
    ASSERT_EQ(3, nodeGroup._runningOpCounts[2]);
    ASSERT_EQ(3, nodeGroup._newTargets[2].ops_size());
    ASSERT_TRUE(checkTarget(nodeGroup._newTargets[2], {3, 4, 5}));
}

bool GeneralNodeGroupTest::checkTarget(const proto::OperationTarget& target, const std::vector<int32_t>& opIds)
{
    if (target.ops_size() != opIds.size()) {
        std::cout << "mismatch count, target:" << target.ops_size() << " opIds:" << opIds.size() << std::endl;
        return false;
    }
    for (size_t i = 0; i < opIds.size(); ++i) {
        if (target.ops(i).id() != opIds[i]) {
            std::cout << "mismatch opId:" << target.ops(i).id() << ":" << opIds[i] << std::endl;
            return false;
        }
    }
    return true;
}
/*
  backup2  3,4,5
  backup1  1,2,3
  mainnode 0,1,2
 */
TEST_F(GeneralNodeGroupTest, testSyncPendingOp)
{
    const uint32_t nodeRunningOpLimit = 4;
    auto nodeGroup = createNodeGroup(/*nodeId*/ 0);

    TaskController::Node backupNode2;
    nodeGroup.addNode(&backupNode2);

    TaskController::Node backupNode1;
    backupNode1.statusDescription = "meaningless";
    nodeGroup.addNode(&backupNode1);

    TaskController::Node mainNode;
    mainNode.statusDescription = "meaningless";
    nodeGroup.addNode(&mainNode);

    proto::OperationPlan plan;
    for (size_t i = 0; i < 9; ++i) {
        auto op = plan.add_ops();
        op->set_id(i);
        std::string type = std::string("test_") + std::to_string(i);
        op->set_type(type);
        op->set_memory(100);
    }
    ASSERT_TRUE(_topoMgr->init(plan));

    generateCurrentAndTarget(/*epoch*/ "0", {{0, proto::OP_RUNNING}, {1, proto::OP_RUNNING}, {2, proto::OP_RUNNING}},
                             &mainNode);
    generateCurrentAndTarget(
        /*epoch*/ "1",
        {{0, proto::OP_FINISHED}, {1, proto::OP_RUNNING}, {2, proto::OP_RUNNING}, {3, proto::OP_RUNNING}},
        &backupNode1);
    generateCurrentAndTarget(/*epoch*/ "1",
                             {{0, proto::OP_FINISHED},
                              {1, proto::OP_FINISHED},
                              {2, proto::OP_FINISHED},
                              {3, proto::OP_RUNNING},
                              {4, proto::OP_RUNNING},
                              {5, proto::OP_RUNNING}},
                             &backupNode2);
    proto::GeneralTaskWalRecord walRecord;
    ASSERT_TRUE(nodeGroup.handleFinishedOp(&walRecord));
    nodeGroup.syncPendingOp(nodeRunningOpLimit);
    // backup node 2
    ASSERT_EQ(3, nodeGroup._runningOpCounts[0]);
    ASSERT_EQ(3, nodeGroup._newTargets[0].ops_size());
    ASSERT_TRUE(checkTarget(nodeGroup._newTargets[0], {3, 4, 5}));
    // backup node 1
    ASSERT_EQ(3, nodeGroup._runningOpCounts[1]);
    ASSERT_EQ(3, nodeGroup._newTargets[1].ops_size());
    ASSERT_TRUE(checkTarget(nodeGroup._newTargets[1], {3, 4, 5}));
    // main node
    ASSERT_EQ(3, nodeGroup._runningOpCounts[2]);
    ASSERT_EQ(3, nodeGroup._newTargets[2].ops_size());
    ASSERT_TRUE(checkTarget(nodeGroup._newTargets[2], {3, 4, 5}));
}
} // namespace build_service::admin
