#pragma once

#include <string>

#include "build_service/admin/controlflow/TaskResourceManager.h"
#include "build_service/admin/taskcontroller/BuildServiceTask.h"
#include "build_service/common/ResourceContainer.h"
#include "build_service/common_define.h"

namespace build_service { namespace admin {

class FakeBuildServiceTask : public BuildServiceTask
{
public:
    FakeBuildServiceTask(const std::string& id, const std::string& type, const TaskResourceManagerPtr& resMgr);
    ~FakeBuildServiceTask();

private:
    FakeBuildServiceTask(const FakeBuildServiceTask&);
    FakeBuildServiceTask& operator=(const FakeBuildServiceTask&);

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(FakeBuildServiceTask);

}} // namespace build_service::admin
