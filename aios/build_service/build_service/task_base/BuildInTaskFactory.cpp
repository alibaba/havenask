#include "build_service/task_base/BuildInTaskFactory.h"
#include "build_service/io/SwiftOutput.h"
#include "build_service/io/FileOutput.h"
#include "build_service/io/MultiFileOutput.h"
#include "build_service/io/IndexlibIndexInput.h"

using namespace std;
using namespace build_service::io;

namespace build_service {
namespace task_base {
BS_LOG_SETUP(task_base, BuildInTaskFactory);

BuildInTaskFactory::BuildInTaskFactory() {
}

BuildInTaskFactory::~BuildInTaskFactory() {
}

TaskPtr BuildInTaskFactory::createTask(const std::string& taskName) {
#define ENUM_TASK(task)                                         \
    if (taskName == task::TASK_NAME) {                            \
        return TaskPtr(new task());                             \
    }
    string errorMsg = "unknown build in task[" + taskName + "]";
    BS_LOG(ERROR, "%s", errorMsg.c_str());
    return TaskPtr();
#undef ENUM_TASK
}

io::InputCreatorPtr BuildInTaskFactory::getInputCreator(const TaskInputConfig& inputConfig) {
    auto type = inputConfig.getType();
    if (type == INDEXLIB_INDEX) {
        IndexlibIndexInputCreatorPtr inputCreator(new IndexlibIndexInputCreator());
        if (!inputCreator->init(inputConfig)) {
            return InputCreatorPtr();
        }
        return inputCreator;
    }
    return InputCreatorPtr();
}

io::OutputCreatorPtr BuildInTaskFactory::getOutputCreator(
    const TaskOutputConfig& outputConfig) {
    auto type = outputConfig.getType();
    OutputCreatorPtr outputCreator;
    if (type == SWIFT) {
        outputCreator.reset(new SwiftOutputCreator());
    } else if (type == io::FILE) {
        outputCreator.reset(new FileOutputCreator());
    } else if (type == io::MULTI_FILE) {
        outputCreator.reset(new MultiFileOutputCreator());
    } else {
        assert(false);
        return OutputCreatorPtr();
    }
    if (!outputCreator->init(outputConfig)) {
        return OutputCreatorPtr();
    }
    return outputCreator;
}

}
}
