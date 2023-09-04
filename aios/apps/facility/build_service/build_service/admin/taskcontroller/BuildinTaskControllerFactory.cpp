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
#include "build_service/admin/taskcontroller/BuildinTaskControllerFactory.h"

#include "build_service/admin/taskcontroller/BatchTaskController.h"
#include "build_service/admin/taskcontroller/CloneIndexTaskController.h"
#include "build_service/admin/taskcontroller/DefaultTaskController.h"
#include "build_service/admin/taskcontroller/DocReclaimTaskController.h"
#include "build_service/admin/taskcontroller/DropBuildingIndexTaskController.h"
#include "build_service/admin/taskcontroller/EndBuildTaskController.h"
#include "build_service/admin/taskcontroller/ExtractDocTaskController.h"
#include "build_service/admin/taskcontroller/GeneralTaskController.h"
#include "build_service/admin/taskcontroller/PrepareDataSourceTaskController.h"
#include "build_service/admin/taskcontroller/RepartitionTaskController.h"
#include "build_service/admin/taskcontroller/RollbackTaskController.h"
#include "build_service/admin/taskcontroller/RunScriptTaskController.h"
#include "build_service/admin/taskcontroller/SingleBuilderTaskV2.h"
#include "build_service/admin/taskcontroller/SyncIndexTaskController.h"
#include "build_service/admin/taskcontroller/TableMetaSynchronizerTaskController.h"
#include "build_service/config/ConfigDefine.h"
#include "build_service/config/TaskConfig.h"

using namespace std;
using namespace build_service::config;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, BuildinTaskControllerFactory);

BuildinTaskControllerFactory::BuildinTaskControllerFactory() {}

BuildinTaskControllerFactory::~BuildinTaskControllerFactory() {}

TaskControllerPtr BuildinTaskControllerFactory::createTaskController(const std::string& taskId,
                                                                     const std::string& taskName,
                                                                     const TaskResourceManagerPtr& resMgr)
{
    TaskControllerPtr taskController;
    if (taskName == BS_TABLE_META_SYNCHRONIZER) {
        taskController.reset(new TableMetaSyncronizerTaskController(taskId, taskName, resMgr));
    } else if (taskName == BS_REPARTITION) {
        taskController.reset(new RepartitionTaskController(taskId, taskName, resMgr));
    } else if (taskName == BS_EXTRACT_DOC) {
        taskController.reset(new ExtractDocTaskController(taskId, taskName, resMgr));
    } else if (taskName == BS_PREPARE_DATA_SOURCE) {
        taskController.reset(new PrepareDataSourceTaskController(taskId, taskName, resMgr));
    } else if (taskName == BS_ROLLBACK_INDEX) {
        taskController.reset(new RollbackTaskController(taskId, taskName, resMgr));
    } else if (taskName == BS_DROP_BUILDING_INDEX) {
        taskController.reset(new DropBuildingIndexTaskController(taskId, taskName, resMgr));
    } else if (taskName == BS_TASK_DOC_RECLAIM) {
        taskController.reset(new DocReclaimTaskController(taskId, taskName, resMgr));
    } else if (taskName == BS_TASK_RUN_SCRIPT) {
        taskController.reset(new RunScriptTaskController(taskId, taskName, resMgr));
    } else if (taskName == BS_TASK_END_BUILD) {
        taskController.reset(new EndBuildTaskController(taskId, taskName, resMgr));
    } else if (taskName == BS_TASK_CLONE_INDEX) {
        taskController.reset(new CloneIndexTaskController(taskId, taskName, resMgr));
    } else if (taskName == BS_TASK_SYNC_INDEX) {
        taskController.reset(new SyncIndexTaskController(taskId, taskName, resMgr));
    } else if (taskName == BS_TASK_BATCH_CONTROL) {
        taskController.reset(new BatchTaskController(taskId, taskName, resMgr));
    } else if (taskName == BS_GENERAL_TASK) {
        taskController.reset(new GeneralTaskController(taskId, taskName, resMgr));
    } else if (taskName == BS_BUILDER_TASK_V2) {
        taskController.reset(new SingleBuilderTaskV2(taskId, taskName, resMgr));
    } else {
        taskController.reset(new DefaultTaskController(taskId, taskName, resMgr));
    }
    return taskController;
}

}} // namespace build_service::admin
