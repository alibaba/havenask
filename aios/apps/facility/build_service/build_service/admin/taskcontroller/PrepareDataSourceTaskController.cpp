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
#include "build_service/admin/taskcontroller/PrepareDataSourceTaskController.h"

#include <map>
#include <ostream>
#include <stddef.h>
#include <utility>
#include <vector>

#include "alog/Logger.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/config/AgentGroupConfig.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/TaskTarget.h"
#include "build_service/proto/DataDescription.h"

using namespace std;
using namespace build_service::config;
using namespace autil::legacy;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, PrepareDataSourceTaskController);

PrepareDataSourceTaskController::~PrepareDataSourceTaskController() {}

bool PrepareDataSourceTaskController::doInit(const std::string& clusterName, const std::string& taskConfigPath,
                                             const std::string& taskParam)
{
    _clusterName = clusterName;
    _taskConfigFilePath = taskConfigPath;
    _partitionCount = 1;
    _parallelNum = 1;
    KeyValueMap taskParamMap;
    proto::DataDescriptions dataDescriptions;
    string dataDescriptionKvs;
    try {
        FromJsonString(taskParamMap, taskParam);
        dataDescriptionKvs = taskParamMap["dataSource"];
        FromJsonString(dataDescriptions.toVector(), dataDescriptionKvs);
    } catch (const autil::legacy::ExceptionBase& e) {
        stringstream ss;
        ss << "parse data descriptions from [" << taskParam << "] failed, exception[" << string(e.what()) << "]";
        string errorMsg = ss.str();
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }

    if (!needPrepareDataSource(dataDescriptions)) {
        return true;
    }
    TaskTarget target(DEFAULT_TARGET_NAME);
    target.setPartitionCount(_partitionCount);
    target.setParallelNum(_parallelNum);
    target.addTaskConfigPath(_taskConfigFilePath);
    target.addTargetDescription("dataDescriptions", dataDescriptionKvs);
    _targets.push_back(target);
    return true;
}

bool PrepareDataSourceTaskController::needPrepareDataSource(const proto::DataDescriptions& dataDescriptions)
{
    bool hasFileSource = false;
    for (size_t i = 0; i < dataDescriptions.size(); i++) {
        auto description = dataDescriptions[i];
        auto iter = description.find(READ_SRC_TYPE);
        if (iter != description.end() && iter->second == FILE_READ_SRC) {
            hasFileSource = true;
            break;
        }
    }
    return hasFileSource;
}

}} // namespace build_service::admin
