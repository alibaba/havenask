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
#ifndef ISEARCH_BS_EXTRACTDOCTASKCONTROLLER_H
#define ISEARCH_BS_EXTRACTDOCTASKCONTROLLER_H

#include "build_service/admin/taskcontroller/DefaultTaskController.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service { namespace admin {

class ExtractDocTaskController : public DefaultTaskController
{
public:
    ExtractDocTaskController(const std::string& taskId, const std::string& taskName,
                             const TaskResourceManagerPtr& resMgr)
        : DefaultTaskController(taskId, taskName, resMgr)
    {
    }
    ~ExtractDocTaskController();

public:
    bool init(const std::string& clusterName, const std::string& taskConfigPath, const std::string& initParam) override;

    TaskController* clone() override { return new ExtractDocTaskController(*this); }

    bool updateConfig() override;
    bool operate(TaskController::Nodes& taskNodes) override;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    std::string getTaskInfo() override { return "{\"processorCheckpoint\":" + _processorCheckpoint + "}"; }

private:
    std::vector<std::string> _checkpoints;
    std::string _processorCheckpoint;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(ExtractDocTaskController);

}} // namespace build_service::admin

#endif // ISEARCH_BS_EXTRACTDOCTASKCONTROLLER_H
