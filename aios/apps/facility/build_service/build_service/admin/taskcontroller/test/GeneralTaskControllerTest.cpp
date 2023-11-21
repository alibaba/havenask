#include "build_service/admin/taskcontroller/GeneralTaskController.h"

#include <memory>
#include <string>
#include <vector>

#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/admin/controlflow/TaskResourceManager.h"
#include "build_service/config/AgentGroupConfig.h"
#include "build_service/config/ConfigReaderAccessor.h"
#include "build_service/config/ResourceReaderManager.h"
#include "build_service/config/TaskConfig.h"
#include "build_service/proto/Heartbeat.pb.h"
#include "build_service/proto/JsonizableProtobuf.h"
#include "build_service/test/unittest.h"
#include "unittest/unittest.h"

using namespace build_service::config;

namespace build_service::admin {

class GeneralTaskControllerTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp() override;
    void tearDown() override;

private:
    TaskResourceManagerPtr _resMgr;
};

void GeneralTaskControllerTest::setUp()
{
    _resMgr.reset(new TaskResourceManager);
    config::ConfigReaderAccessorPtr configResource(new config::ConfigReaderAccessor("simple"));
    std::string configPath = GET_TEST_DATA_PATH() + "admin_test/configV2";
    ResourceReaderPtr configReader = ResourceReaderManager::getResourceReader(configPath);
    configResource->addConfig(configReader, true);
    _resMgr->addResource(configResource);
}

void GeneralTaskControllerTest::tearDown() {}

TEST_F(GeneralTaskControllerTest, testInit)
{
    {
        GeneralTaskParam param;
        param.taskEpochId = "123";
        param.partitionIndexRoot = "index_root";
        param.sourceVersionIds = {0};
        proto::OperationPlan operationPlan;
        operationPlan.set_taskname("full_merge");
        operationPlan.set_tasktype("merge");
        param.plan = proto::JsonizableProtobuf<proto::OperationPlan>(operationPlan);
        std::string taskId = "task_id";
        std::string taskName = "task_name";
        GeneralTaskController controller(taskId, taskName, _resMgr);
        ASSERT_TRUE(controller.init("cluster1", "", autil::legacy::ToJsonString(param)));
        ASSERT_EQ(10, controller._parallelNum);
        ASSERT_EQ(11, controller._threadNum);
        ASSERT_EQ("fullMerge", controller._roleName);
    }
    {
        GeneralTaskParam param;
        param.taskEpochId = "123";
        param.partitionIndexRoot = "index_root";
        param.sourceVersionIds = {0};
        proto::OperationPlan operationPlan;
        operationPlan.set_taskname("inc");
        operationPlan.set_tasktype("merge");
        param.plan = proto::JsonizableProtobuf<proto::OperationPlan>(operationPlan);
        std::string taskId = "task_id";
        std::string taskName = "task_name";
        GeneralTaskController controller(taskId, taskName, _resMgr);
        ASSERT_TRUE(controller.init("cluster1", "", autil::legacy::ToJsonString(param)));
        ASSERT_EQ(1, controller._parallelNum);
        ASSERT_EQ(4, controller._threadNum);
        ASSERT_EQ("incMerge", controller._roleName);
    }
}

} // namespace build_service::admin
