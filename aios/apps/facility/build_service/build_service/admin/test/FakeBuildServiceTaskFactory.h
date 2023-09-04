#ifndef ISEARCH_BS_FAKEBUILDSERVICETASKFACTORY_H
#define ISEARCH_BS_FAKEBUILDSERVICETASKFACTORY_H

#include "build_service/admin/controlflow/TaskFactory.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"

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

#endif // ISEARCH_BS_FAKEBUILDSERVICETASKFACTORY_H
