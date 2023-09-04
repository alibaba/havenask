#ifndef ISEARCH_BS_FAKEBUILDSERVICETASK_H
#define ISEARCH_BS_FAKEBUILDSERVICETASK_H

#include "build_service/admin/taskcontroller/BuildServiceTask.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"

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

#endif // ISEARCH_BS_FAKEBUILDSERVICETASK_H
