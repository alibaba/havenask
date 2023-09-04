#include "build_service/admin/test/FakeBuildServiceTaskFactory.h"

#include "build_service/admin/taskcontroller/MergeCrontabTask.h"
#include "build_service/admin/test/FakeBuildServiceTask.h"

using namespace std;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, FakeBuildServiceTaskFactory);

FakeBuildServiceTaskFactory::FakeBuildServiceTaskFactory() {}

FakeBuildServiceTaskFactory::~FakeBuildServiceTaskFactory() {}

TaskBasePtr FakeBuildServiceTaskFactory::createTaskObject(const string& id, const string& kernalType,
                                                          const TaskResourceManagerPtr& resMgr)
{
    if (kernalType == MergeCrontabTask::MERGE_CRONTAB) {
        return TaskBasePtr(new MergeCrontabTask(id, kernalType, resMgr));
    }
    return TaskBasePtr(new FakeBuildServiceTask(id, kernalType, resMgr));
}

}} // namespace build_service::admin
