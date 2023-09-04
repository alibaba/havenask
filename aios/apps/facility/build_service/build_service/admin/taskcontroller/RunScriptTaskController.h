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
#ifndef ISEARCH_BS_RUNSCRIPTTASKCONTROLLER_H
#define ISEARCH_BS_RUNSCRIPTTASKCONTROLLER_H

#include "build_service/admin/taskcontroller/DefaultTaskController.h"
#include "build_service/common_define.h"
#include "build_service/config/ScriptTaskConfig.h"
#include "build_service/util/Log.h"

namespace build_service { namespace admin {

class RunScriptTaskController : public DefaultTaskController
{
public:
    RunScriptTaskController(const std::string& taskId, const std::string& taskName,
                            const TaskResourceManagerPtr& resMgr);

    ~RunScriptTaskController();

private:
    RunScriptTaskController(const RunScriptTaskController&);
    RunScriptTaskController& operator=(const RunScriptTaskController&);

public:
    bool doInit(const std::string& clusterName, const std::string& taskConfigPath,
                const std::string& initParam) override;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    bool start(const KeyValueMap& kvMap) override;
    bool updateConfig() override;

private:
    bool prepareRunScriptTask(const std::string& configFileName, const KeyValueMap& kvMap,
                              config::ScriptTaskConfig& source);

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(RunScriptTaskController);

}} // namespace build_service::admin

#endif // ISEARCH_BS_RUNSCRIPTTASKCONTROLLER_H
