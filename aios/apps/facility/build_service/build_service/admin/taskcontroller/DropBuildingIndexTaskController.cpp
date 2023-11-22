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
#include "build_service/admin/taskcontroller/DropBuildingIndexTaskController.h"

#include <iosfwd>
#include <map>
#include <memory>

#include "alog/Logger.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/common/CheckpointAccessor.h"
#include "build_service/common/IndexCheckpointFormatter.h"
#include "build_service/config/ConfigDefine.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;

using build_service::common::CheckpointAccessorPtr;
using build_service::common::IndexCheckpointFormatter;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, DropBuildingIndexTaskController);

DropBuildingIndexTaskController::~DropBuildingIndexTaskController() {}

bool DropBuildingIndexTaskController::doInit(const std::string& clusterName, const std::string& taskConfigPath,
                                             const std::string& initParam)
{
    CheckpointAccessorPtr checkPointAccessor;
    _resourceManager->getResource(checkPointAccessor);
    string checkpointId = IndexCheckpointFormatter::getIndexCheckpointId(true, clusterName);
    string checkPointName;
    string checkPointValue;
    if (!checkPointAccessor->getLatestCheckpoint(checkpointId, checkPointName, checkPointValue)) {
        BS_LOG(ERROR, "get checkpoint failed for task [%s], cluster is [%s]", _taskId.c_str(), clusterName.c_str());
        return false;
    }
    map<string, string> taskParam;
    taskParam[config::BS_ROLLBACK_SOURCE_VERSION] = checkPointName;
    string param = ToJsonString(taskParam);
    BS_LOG(INFO, "drop [%s] building index, rollback to version [%s]", clusterName.c_str(), checkPointName.c_str());
    return RollbackTaskController::doInit(clusterName, taskConfigPath, param);
}

}} // namespace build_service::admin
