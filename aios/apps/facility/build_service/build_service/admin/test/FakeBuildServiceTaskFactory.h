#pragma once

#include <string>

#include "build_service/admin/controlflow/TaskFactory.h"
#include "build_service/common/ResourceContainer.h"
#include "build_service/common_define.h"

namespace build_service { namespace admin {

class FakeBuildServiceTaskFactory : public TaskFactory
{
public:
    FakeBuildServiceTaskFactory();
    ~FakeBuildServiceTaskFactory();

private:
    FakeBuildServiceTaskFactory(const FakeBuildServiceTaskFactory&);
    FakeBuildServiceTaskFactory& operator=(const FakeBuildServiceTaskFactory&);

public:
    TaskBasePtr createTaskObject(const std::string& id, const std::string& kernalType,
                                 const TaskResourceManagerPtr& resMgr) override;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(FakeBuildServiceTaskFactory);

}} // namespace build_service::admin
