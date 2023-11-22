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

#include <string>

#include "build_service/admin/controlflow/TaskResourceManager.h"
#include "build_service/admin/taskcontroller/DefaultTaskController.h"
#include "build_service/common/ResourceContainer.h"
#include "build_service/common_define.h"
#include "build_service/proto/DataDescriptions.h"

namespace build_service { namespace admin {

class PrepareDataSourceTaskController : public DefaultTaskController
{
public:
    PrepareDataSourceTaskController(const std::string& taskId, const std::string& taskName,
                                    const TaskResourceManagerPtr& resMgr)
        : DefaultTaskController(taskId, taskName, resMgr)
    {
    }
    ~PrepareDataSourceTaskController();

private:
    PrepareDataSourceTaskController(const PrepareDataSourceTaskController&);
    PrepareDataSourceTaskController& operator=(const PrepareDataSourceTaskController&);

public:
    bool doInit(const std::string& clusterName, const std::string& taskConfigPath,
                const std::string& taskParam) override;

    bool needPrepareDataSource(const proto::DataDescriptions& dataDescriptions);

    bool updateConfig() override { return true; }

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(PrepareDataSourceTaskController);

}} // namespace build_service::admin
