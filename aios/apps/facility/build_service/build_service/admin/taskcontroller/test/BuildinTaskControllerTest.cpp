#include "build_service/admin/taskcontroller/BuildinTaskControllerFactory.h"
#include "build_service/test/unittest.h"

using namespace build_service::config;

namespace build_service::admin {

class BuildinTaskControllerTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp() override {}
    void tearDown() override {}
};

TEST_F(BuildinTaskControllerTest, testIsSupportBackup)
{
    const std::string taskId = "ut";
    BuildinTaskControllerFactory factory;
    const std::vector<std::string> buildinTaskControllerNames = {BS_TABLE_META_SYNCHRONIZER, BS_REPARTITION};
    for (auto& taskName : buildinTaskControllerNames) {
        auto taskController = factory.createTaskController(taskId, taskName, /*resource mgr*/ nullptr);
        ASSERT_FALSE(taskController->isSupportBackup());
    }
    auto taskController = factory.createTaskController(taskId, BS_GENERAL_TASK, /*resource mgr*/ nullptr);
    ASSERT_TRUE(taskController->isSupportBackup());
}

} // namespace build_service::admin
