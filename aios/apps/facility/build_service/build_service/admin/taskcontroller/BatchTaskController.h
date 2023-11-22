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
#pragma once

#include <map>
#include <string>
#include <vector>

#include "autil/legacy/legacy_jsonizable_dec.h"
#include "build_service/admin/controlflow/TaskResourceManager.h"
#include "build_service/admin/taskcontroller/DefaultTaskController.h"
#include "build_service/admin/taskcontroller/TaskController.h"
#include "build_service/common/ResourceContainer.h"
#include "build_service/common_define.h"
#include "build_service/config/TaskTarget.h"

namespace build_service { namespace admin {

class BatchTaskController : public DefaultTaskController
{
public:
    BatchTaskController(const std::string& taskId, const std::string& taskName, const TaskResourceManagerPtr& resMgr)
        : DefaultTaskController(taskId, taskName, resMgr)
    {
    }
    ~BatchTaskController();

private:
    BatchTaskController(const BatchTaskController&);
    BatchTaskController& operator=(const BatchTaskController&);

public:
    bool doInit(const std::string& clusterName, const std::string& taskConfigPath,
                const std::string& initParam) override;
    bool operate(TaskController::Nodes& taskNodes) override;
    bool start(const KeyValueMap& kvMap) override;
    bool updateConfig() override;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

private:
    bool prepareTargetDescrition(const std::map<std::string, std::string>& taskParam, const std::string& initParam,
                                 config::TaskTarget& taskTarget);
    bool registerTopic();
    bool deregisterTopic();

private:
    std::vector<std::string> _clusters;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(BatchTaskController);

}} // namespace build_service::admin
