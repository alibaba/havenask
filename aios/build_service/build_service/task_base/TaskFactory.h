#ifndef ISEARCH_BS_TASKFACTORY_H
#define ISEARCH_BS_TASKFACTORY_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/task_base/Task.h"
#include "build_service/config/TaskInputConfig.h"
#include "build_service/config/TaskOutputConfig.h"
#include "build_service/plugin/ModuleFactory.h"

namespace build_service {
namespace task_base {

class TaskFactory: public plugin::ModuleFactory
{
public:
    TaskFactory() {}
    virtual ~TaskFactory() {}
private:
    TaskFactory(const TaskFactory &);
    TaskFactory& operator=(const TaskFactory &);
public:
    virtual TaskPtr createTask(const std::string& taskName) = 0;
    virtual io::InputCreatorPtr getInputCreator(const config::TaskInputConfig& inputConfig) = 0;
    virtual io::OutputCreatorPtr getOutputCreator(const config::TaskOutputConfig& outputConfig) = 0;

    void destroy() override { delete this; }
    
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(TaskFactory);

}
}

#endif //ISEARCH_BS_TASKFACTORY_H
