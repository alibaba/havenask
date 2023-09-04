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
#ifndef ISEARCH_BS_ROLLBACKTASKCONTROLLER_H
#define ISEARCH_BS_ROLLBACKTASKCONTROLLER_H

#include "build_service/admin/taskcontroller/DefaultTaskController.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service { namespace admin {

BS_DECLARE_REFERENCE_CLASS(config, ResourceReader);

class RollbackTaskController : public DefaultTaskController
{
public:
    RollbackTaskController(const std::string& taskId, const std::string& taskName, const TaskResourceManagerPtr& resMgr)
        : DefaultTaskController(taskId, taskName, resMgr)
        , _isFinished(false)
    {
    }
    ~RollbackTaskController();

private:
    RollbackTaskController(const RollbackTaskController&);
    RollbackTaskController& operator=(const RollbackTaskController&);

public:
    static config::ResourceReaderPtr getConfigReader(const std::string& clusterName,
                                                     const TaskResourceManagerPtr& resouceManager);
    static bool checkRollbackLegal(const std::string& clusterName, int64_t sourceVersion,
                                   const TaskResourceManagerPtr& resourceManager);
    bool doInit(const std::string& clusterName, const std::string& taskConfigPath,
                const std::string& initParam) override;
    bool start(const KeyValueMap& kvMap) override;
    bool updateConfig() override;
    bool operate(TaskController::Nodes& taskNodes) override;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

private:
    bool addCheckpoint(TaskController::Node& node);

private:
    BS_LOG_DECLARE();

protected:
    versionid_t _sourceVersion;
    bool _isFinished;
};

BS_TYPEDEF_PTR(RollbackTaskController);

}} // namespace build_service::admin

#endif // ISEARCH_BS_ROLLBACKTASKCONTROLLER_H
