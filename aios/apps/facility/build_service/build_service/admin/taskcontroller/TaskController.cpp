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
#include "build_service/admin/taskcontroller/TaskController.h"

#include <iosfwd>

#include "build_service/config/AgentGroupConfig.h"

using namespace std;
using namespace build_service::config;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, TaskController);

bool TaskController::getTaskConfig(const std::string& taskConfigFile, const std::string& taskName,
                                   config::TaskConfig& taskConfig)
{
    if (!TaskConfig::loadFromFile(taskConfigFile, taskConfig)) {
        return false;
    }
    return true;
}

bool TaskController::start(const KeyValueMap& kvMap) { return true; }

void TaskController::setBeeperTags(const beeper::EventTagsPtr beeperTags) { _beeperTags = beeperTags; }

}} // namespace build_service::admin
