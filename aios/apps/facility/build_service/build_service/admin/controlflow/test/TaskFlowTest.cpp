#include "build_service/admin/controlflow/TaskFlow.h"

#include "build_service/admin/controlflow/LuaLoggerWrapper.h"
#include "build_service/admin/controlflow/TaskBase.h"
#include "build_service/admin/controlflow/TaskFactory.h"
#include "build_service/proto/WorkerNode.h"
#include "build_service/test/unittest.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace build_service::proto;
using namespace testing;

namespace build_service { namespace admin {

class TaskFlowTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp();
    void tearDown();

private:
    void syncAllTask(TaskFlowPtr& flow);
};

void TaskFlowTest::setUp() {}

void TaskFlowTest::tearDown() {}

TEST_F(TaskFlowTest, TestSimpleProcess)
{
    TaskFactoryPtr factory(new TaskFactory);
    string rootPath = TEST_ROOT_PATH() + "/build_service/admin/controlflow/test/lua_scripts";
    TaskFlowPtr flow(new TaskFlow(rootPath, factory));
    ASSERT_TRUE(flow->init("test_flow.flow", "full"));
    flow->setGlobalTaskParam("auto_finish", "true");

    ASSERT_FALSE(flow->isFlowRunning());

    flow->stepRun();
    ASSERT_FALSE(flow->isFlowRunning() || flow->isFlowFinish());
    flow->makeActive();

    syncAllTask(flow);
    ASSERT_TRUE(flow->isFlowRunning() || flow->isFlowFinish());
}

TEST_F(TaskFlowTest, TestImportTool)
{
    TaskFactoryPtr factory(new TaskFactory);
    string rootPath = TEST_ROOT_PATH() + "/build_service/admin/controlflow/test/lua_scripts";
    TaskFlowPtr flow(new TaskFlow(rootPath, factory));
    ASSERT_TRUE(flow->init("test_tool.flow", "test"));
    flow->makeActive();
    flow->stepRun();
    ASSERT_TRUE(flow->isFlowFinish());
}

TEST_F(TaskFlowTest, TestImportLog)
{
    TaskFactoryPtr factory(new TaskFactory);
    string rootPath = TEST_ROOT_PATH() + "/build_service/admin/controlflow/test/lua_scripts";
    TaskFlowPtr flow(new TaskFlow(rootPath, factory));
    ASSERT_TRUE(flow->init("test_log.flow", "test"));
    flow->makeActive();
    flow->stepRun();
    ASSERT_TRUE(flow->isFlowFinish());
}

void TaskFlowTest::syncAllTask(TaskFlowPtr& flow)
{
    WorkerNodes workerNodes;
    flow->stepRun();
    for (auto task : flow->getTasks()) {
        task->syncTaskProperty(workerNodes);
    }
    flow->stepRun();
}

}} // namespace build_service::admin
