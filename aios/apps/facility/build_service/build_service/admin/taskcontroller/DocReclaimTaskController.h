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

#include "alog/Logger.h"
#include "autil/legacy/legacy_jsonizable_dec.h"
#include "build_service/admin/controlflow/TaskResourceManager.h"
#include "build_service/admin/taskcontroller/DefaultTaskController.h"
#include "build_service/common/ResourceContainer.h"
#include "build_service/common_define.h"
#include "build_service/config/DocReclaimSource.h"

namespace build_service { namespace admin {

class DocReclaimTaskController : public DefaultTaskController
{
public:
    DocReclaimTaskController(const std::string& taskId, const std::string& taskName,
                             const TaskResourceManagerPtr& resMgr);
    ~DocReclaimTaskController();

private:
    DocReclaimTaskController(const DocReclaimTaskController&);
    DocReclaimTaskController& operator=(const DocReclaimTaskController&);

public:
    bool doInit(const std::string& clusterName, const std::string& taskConfigPath,
                const std::string& initParam) override;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    bool start(const KeyValueMap& kvMap) override;
    bool updateConfig() override
    {
        BS_LOG(WARN, "docReclaim task not support update config, ignore");
        return true;
    }

private:
    bool prepareDocReclaimTask(const std::string& mergeConfigName, config::DocReclaimSource& source);

private:
    int64_t _stopTs;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(DocReclaimTaskController);

}} // namespace build_service::admin
