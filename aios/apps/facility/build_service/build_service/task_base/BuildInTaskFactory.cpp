/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "build_service/task_base/BuildInTaskFactory.h"

#include "build_service/build_task/BuildTask.h"
#include "build_service/general_task/GeneralTask.h"
#include "build_service/io/FileOutput.h"
#include "build_service/io/IndexlibIndexInput.h"
#include "build_service/io/MultiFileOutput.h"
#include "build_service/io/SwiftOutput.h"

using namespace std;
using namespace build_service::io;

namespace build_service { namespace task_base {
BS_LOG_SETUP(task_base, BuildInTaskFactory);

BuildInTaskFactory::BuildInTaskFactory() {}

BuildInTaskFactory::~BuildInTaskFactory() {}

TaskPtr BuildInTaskFactory::createTask(const std::string& taskName)
{
#define ENUM_TASK(task)                                                                                                \
    if (taskName == task::TASK_NAME) {                                                                                 \
        return TaskPtr(new task());                                                                                    \
    }

    ENUM_TASK(GeneralTask);
    ENUM_TASK(build_task::BuildTask);

    string errorMsg = "unknown build in task[" + taskName + "]";
    BS_LOG(ERROR, "%s", errorMsg.c_str());
    return TaskPtr();

#undef ENUM_TASK
}

io::InputCreatorPtr BuildInTaskFactory::getInputCreator(const config::TaskInputConfig& inputConfig)
{
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

io::OutputCreatorPtr BuildInTaskFactory::getOutputCreator(const config::TaskOutputConfig& outputConfig)
{
    auto type = outputConfig.getType();
    OutputCreatorPtr outputCreator;
    if (type == SWIFT) {
        outputCreator.reset(new SwiftOutputCreator());
    } else if (type == io::FILE) {
        outputCreator.reset(new FileOutputCreator());
    } else if (type == io::MULTI_FILE) {
        outputCreator.reset(new MultiFileOutputCreator());
    } else {
        BS_LOG(ERROR, "unknown output type[%s]", type.c_str());
        assert(false);
        return OutputCreatorPtr();
    }
    if (!outputCreator->init(outputConfig)) {
        return OutputCreatorPtr();
    }
    return outputCreator;
}

}} // namespace build_service::task_base
