#include "build_service/admin/test/FakeBuildServiceTask.h"

#include <iosfwd>

using namespace std;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, FakeBuildServiceTask);

FakeBuildServiceTask::FakeBuildServiceTask(const std::string& id, const std::string& type,
                                           const TaskResourceManagerPtr& resMgr)
    : BuildServiceTask(id, type, resMgr)
{
}

FakeBuildServiceTask::~FakeBuildServiceTask() {}

}} // namespace build_service::admin
