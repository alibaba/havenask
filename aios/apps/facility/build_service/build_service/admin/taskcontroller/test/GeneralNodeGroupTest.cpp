#include "build_service/admin/taskcontroller/GeneralNodeGroup.h"

#include <cstdint>
#include <iostream>
#include <memory>
#include <stddef.h>
#include <string>
#include <utility>
#include <vector>

#include "build_service/admin/taskcontroller/OperationTopoManager.h"
#include "build_service/admin/taskcontroller/TaskController.h"
#include "build_service/config/AgentGroupConfig.h"
#include "build_service/config/ConfigDefine.h"
#include "build_service/config/TaskTarget.h"
#include "build_service/proto/Admin.pb.h"
#include "build_service/proto/Heartbeat.pb.h"
#include "build_service/proto/ProtoUtil.h"
#include "build_service/test/unittest.h"
#include "unittest/unittest.h"

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
    const std::string _roleName = "test";
    std::unique_ptr<OperationTopoManager> _topoMgr;
};

void GeneralNodeGroupTest::setUp() { _topoMgr = std::make_unique<OperationTopoManager>(); }

GeneralNodeGroup GeneralNodeGroupTest::createNodeGroup(uint32_t nodeId)
{
    return GeneralNodeGroup(_taskType, _taskName, nodeId, _topoMgr.get(), /*opTargetBase64Encode=*/true,
                            "partitionWorkRoot");
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

TEST_F(GeneralNodeGroupTest, testSimple)
{
    auto node = createNodeGroup(0);
    ASSERT_EQ(_taskType, node.getTaskType());
    ASSERT_EQ(_taskName, node.getTaskName());
    ASSERT_EQ(node.getNodeId(), 0);
    ASSERT_EQ(node.getRunningOpCount(), 0);

    TaskController::Node taskNode;
    node.addNode(&taskNode);
    ASSERT_TRUE(node.collectFinishOp(nullptr));
}

TEST_F(GeneralNodeGroupTest, testAddNode)
{
    TaskController::Node node;
    auto nodeGroup = createNodeGroup(/*nodeId*/ 0);
    ASSERT_EQ(_taskType, nodeGroup._taskType);
    ASSERT_EQ(_taskName, nodeGroup._taskName);
    ASSERT_EQ(0, nodeGroup._nodeId);
    ASSERT_EQ(0, nodeGroup._nodes.size());
    ASSERT_EQ(0, nodeGroup._runningOpCount);

    nodeGroup.addNode(&node);
    ASSERT_EQ(1, nodeGroup._nodes.size());
    ASSERT_EQ(0, nodeGroup._runningOpCount);

    nodeGroup.addNode(&node);
    ASSERT_EQ(2, nodeGroup._nodes.size());
    ASSERT_EQ(0, nodeGroup._runningOpCount);
    ASSERT_TRUE(nodeGroup.collectFinishOp(/*walRecord*/ nullptr));
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
    for (size_t i = 0; i < 3; ++i) {
        auto op = plan.add_ops();
        op->set_id(i);
        std::string type = std::string("test_") + std::to_string(i);
        op->set_type(type);
        op->set_memory(100);
    }
    ASSERT_TRUE(_topoMgr->init(plan));

    generateCurrentAndTarget(/*epoch*/ "0", {{0, proto::OP_FINISHED}, {1, proto::OP_ERROR}, {2, proto::OP_RUNNING}},
                             &mainNode);

    generateCurrentAndTarget(/*epoch*/ "1", {{0, proto::OP_ERROR}, {1, proto::OP_FINISHED}, {2, proto::OP_FINISHED}},
                             &backupNode1);

    generateCurrentAndTarget(/*epoch*/ "2", {{0, proto::OP_ERROR}, {1, proto::OP_RUNNING}, {2, proto::OP_RUNNING}},
                             &backupNode2);

    proto::GeneralTaskWalRecord walRecord;
    ASSERT_TRUE(nodeGroup.collectFinishOp(&walRecord));
    ASSERT_TRUE(nodeGroup.updateTarget());
    ASSERT_EQ(0, nodeGroup._runningOpCount);
    ASSERT_EQ(0, nodeGroup._target.ops_size());
}

TEST_F(GeneralNodeGroupTest, testHandleFinishedOpAbnormal)
{
    const uint32_t nodeId = 0;
    auto nodeGroup = createNodeGroup(nodeId);
    // empty _nodes
    ASSERT_TRUE(nodeGroup.collectFinishOp(/*walRecord*/ nullptr));
    // empty status desc
    TaskController::Node mainNode;
    nodeGroup.addNode(&mainNode);
    ASSERT_TRUE(nodeGroup.collectFinishOp(/*walRecord*/ nullptr));
    // invalid status desc, parse failed
    mainNode.statusDescription = "{invalid_desc";
    ASSERT_FALSE(nodeGroup.collectFinishOp(/*walRecord*/ nullptr));
    // invalid status desc, parse failed
    mainNode.statusDescription = "";
    TaskController::Node backupNode;
    backupNode.statusDescription = "{invalid_desc";
    nodeGroup.addNode(&backupNode);
    ASSERT_FALSE(nodeGroup.collectFinishOp(/*walRecord*/ nullptr));
    // invalid old target
    proto::OperationCurrent current;
    current.set_workerepochid(/*epoch*/ "0");
    current.set_availablememory(100000);
    ASSERT_TRUE(current.SerializeToString(&mainNode.statusDescription));

    backupNode.statusDescription = "";
    std::string invalidTaskTarget = "{invalid_task_target";
    mainNode.taskTarget.updateTargetDescription(BS_GENERAL_TASK_OPERATION_TARGET, invalidTaskTarget);
    ASSERT_TRUE(nodeGroup.collectFinishOp(/*walRecord*/ nullptr));
    ASSERT_FALSE(nodeGroup.updateTarget());
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

    nodeGroup._runningOpCount = 3; // node group has 3 running ops
    nodeGroup.assignOp(&op, &walRecord);
    ASSERT_EQ(4, nodeGroup._runningOpCount);
    ASSERT_EQ(1, nodeGroup._target.ops_size());

    auto checkTarget = [&](const config::TaskTarget& taskTarget) -> bool {
        proto::OperationTarget target;
        std::string content;
        if (!taskTarget.getTargetDescription(BS_GENERAL_TASK_OPERATION_TARGET, content)) {
            return false;
        }
        if (!proto::ProtoUtil::parseBase64EncodedPbStr(content, &target)) {
            return false;
        }
        if (target.ops_size() != 1) {
            return false;
        }
        return true;
    };
    mainNode.statusDescription = "meaningless";
    backupNode1.statusDescription = "meaningless";

    nodeGroup.serializeTarget();
    ASSERT_TRUE(checkTarget(nodeGroup._nodes[0]->taskTarget));
    ASSERT_TRUE(checkTarget(nodeGroup._nodes[1]->taskTarget));
    ASSERT_TRUE(checkTarget(nodeGroup._nodes[2]->taskTarget));
}

} // namespace build_service::admin
