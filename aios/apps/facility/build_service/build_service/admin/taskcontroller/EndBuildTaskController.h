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

#include <stdint.h>
#include <string>

#include "autil/legacy/legacy_jsonizable_dec.h"
#include "build_service/admin/controlflow/TaskResourceManager.h"
#include "build_service/admin/taskcontroller/DefaultTaskController.h"
#include "build_service/admin/taskcontroller/TaskController.h"
#include "build_service/common/ResourceContainer.h"
#include "build_service/common_define.h"
#include "indexlib/base/Types.h"
#include "indexlib/indexlib.h"

namespace build_service { namespace admin {

class EndBuildTaskController : public DefaultTaskController
{
public:
    EndBuildTaskController(const std::string& taskId, const std::string& taskName,
                           const TaskResourceManagerPtr& resMgr);
    ~EndBuildTaskController();

private:
    EndBuildTaskController(const EndBuildTaskController&);
    EndBuildTaskController& operator=(const EndBuildTaskController&);

public:
    bool doInit(const std::string& clusterName, const std::string& taskConfigPath,
                const std::string& initParam) override;
    bool updateConfig() override;
    bool start(const KeyValueMap& kvMap) override;
    bool operate(TaskController::Nodes& taskNodes) override;
    versionid_t getBuilderVersion() const;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

    void supplementLableInfo(KeyValueMap& info) const override;

private:
    bool getBuildVersion(TaskController::Node& node);

private:
    versionid_t _buildVersion;
    int32_t _workerPathVersion;
    std::string _opIds;
    int64_t _schemaId;
    uint32_t _buildParallelNum;
    int32_t _batchMask;
    bool _needAlignedBuildVersion;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(EndBuildTaskController);

}} // namespace build_service::admin
