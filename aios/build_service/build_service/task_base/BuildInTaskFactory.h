#ifndef ISEARCH_BS_BUILDINTASKFACTORY_H
#define ISEARCH_BS_BUILDINTASKFACTORY_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/task_base/TaskFactory.h"

namespace build_service {
namespace task_base {

class BuildInTaskFactory : public TaskFactory
{
public:
    BuildInTaskFactory();
    ~BuildInTaskFactory();
private:
    BuildInTaskFactory(const BuildInTaskFactory &);
    BuildInTaskFactory& operator=(const BuildInTaskFactory &);
    
public:
    TaskPtr createTask(const std::string& taskName) override;
    io::InputCreatorPtr getInputCreator(const config::TaskInputConfig& inputConfig) override;
    io::OutputCreatorPtr getOutputCreator(const config::TaskOutputConfig& outputConfig) override;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(BuildInTaskFactory);

}
}

#endif //ISEARCH_BS_BUILDINTASKFACTORY_H
