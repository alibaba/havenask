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
#include "build_service_tasks/factory/BuildServiceTaskFactory.h"

#include <iostream>
#include <memory>

#include "build_service_tasks/cloneIndex/CloneIndexTask.h"
#include "build_service_tasks/doc_reclaim/DocReclaimTask.h"
#include "build_service_tasks/endbuild/EndBuildTask.h"
#include "build_service_tasks/extract_doc/ExtractDocTask.h"
#include "build_service_tasks/extract_doc/RawDocumentOutput.h"
#include "build_service_tasks/io/MultiFileOutput.h"
#include "build_service_tasks/repartition/RepartitionTask.h"
#include "build_service_tasks/rollback/RollbackTask.h"
#include "build_service_tasks/script_task/RunScriptTask.h"
#include "build_service_tasks/syncIndex/SyncIndexTask.h"
// #include "build_service_tasks/table_meta_synchronizer/TableMetaSynchronizer.h"
#include "alog/Logger.h"
#include "build_service/io/Input.h"
#include "build_service/plugin/ModuleFactory.h"
#include "build_service_tasks/batch_control/BatchControlTask.h"
#include "build_service_tasks/io/IODefine.h"
#include "build_service_tasks/prepare_data_source/PrepareDataSourceTask.h"
#include "build_service_tasks/reset_version_task/ResetVersionTask.h"

using namespace std;
using namespace build_service::io;
using namespace build_service::config;

namespace build_service { namespace task_base {
BS_LOG_SETUP(task_base, BuildServiceTaskFactory);

BuildServiceTaskFactory::BuildServiceTaskFactory() {}

BuildServiceTaskFactory::~BuildServiceTaskFactory() { cout << "DELETING" << endl; }

TaskPtr BuildServiceTaskFactory::createTask(const std::string& taskName)
{
#define ENUM_TASK(task)                                                                                                \
    if (taskName == task::TASK_NAME) {                                                                                 \
        return TaskPtr(new task());                                                                                    \
    }
    ENUM_TASK(ExtractDocTask);
    ENUM_TASK(RepartitionTask);
    // ENUM_TASK(TableMetaSynchronizer);
    ENUM_TASK(PrepareDataSourceTask);
    ENUM_TASK(DocReclaimTask);
    ENUM_TASK(RunScriptTask);
    ENUM_TASK(RollbackTask);
    ENUM_TASK(EndBuildTask);
    ENUM_TASK(CloneIndexTask);
    ENUM_TASK(ResetVersionTask);
    ENUM_TASK(SyncIndexTask);
    ENUM_TASK(BatchControlTask);
    if (taskName == "drop_building_index") {
        return RollbackTaskPtr(new RollbackTask());
    }
    string errorMsg = "unknown build in task[" + taskName + "]";
    BS_LOG(ERROR, "%s", errorMsg.c_str());
    return TaskPtr();
#undef ENUM_TASK
}

InputCreatorPtr BuildServiceTaskFactory::getInputCreator(const TaskInputConfig& inputConfig)
{
    return InputCreatorPtr();
}

OutputCreatorPtr BuildServiceTaskFactory::getOutputCreator(const TaskOutputConfig& outputConfig)
{
    const auto& typeStr = outputConfig.getType();
    if (typeStr == build_service_tasks::RawDocumentOutput::OUTPUT_TYPE) {
        build_service_tasks::RawDocumentOutputCreatorPtr creator(new build_service_tasks::RawDocumentOutputCreator());
        if (!creator->init(outputConfig)) {
            return OutputCreatorPtr();
        }
        return creator;
    } else if (typeStr == build_service_tasks::MULTI_FILE) {
        build_service_tasks::MultiFileOutputCreatorPtr creator(new build_service_tasks::MultiFileOutputCreator());
        if (!creator->init(outputConfig)) {
            return OutputCreatorPtr();
        }
        return creator;
    }
    return OutputCreatorPtr();
}

extern "C" build_service::plugin::ModuleFactory* createFactory_Task() { return new BuildServiceTaskFactory(); }

extern "C" void destroyFactory_Task(build_service::plugin::ModuleFactory* factory) { delete factory; }

}} // namespace build_service::task_base
