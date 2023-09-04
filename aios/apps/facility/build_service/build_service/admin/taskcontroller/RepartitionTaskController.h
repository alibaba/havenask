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
#ifndef ISEARCH_BS_REPARTITIONTASKCONTROLLER_H
#define ISEARCH_BS_REPARTITIONTASKCONTROLLER_H

#include "build_service/admin/taskcontroller/DefaultTaskController.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "indexlib/config/index_schema.h"

namespace build_service { namespace admin {

struct RepartitionTaskParam : public autil::legacy::Jsonizable {
    RepartitionTaskParam() : partitionCount(0), parallelNum(1), generationId(-1) {}
    int32_t partitionCount;
    int32_t parallelNum;
    int32_t generationId;
    std::string indexPath;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
    {
        json.Jsonize("partitionCount", partitionCount, partitionCount);
        json.Jsonize("parallelNum", parallelNum, parallelNum);
        json.Jsonize("generationId", generationId, generationId);
        json.Jsonize("indexPath", indexPath, indexPath);
    }
    bool check(std::string& errorMsg);
};

class RepartitionTaskController : public DefaultTaskController
{
public:
    RepartitionTaskController(const std::string& taskId, const std::string& taskName,
                              const TaskResourceManagerPtr& resMgr)
        : DefaultTaskController(taskId, taskName, resMgr)
    {
    }
    ~RepartitionTaskController();
    RepartitionTaskController(const RepartitionTaskController&);

private:
    RepartitionTaskController& operator=(const RepartitionTaskController&);

public:
    bool doInit(const std::string& clusterName, const std::string& taskConfigPath,
                const std::string& initParam) override;

    TaskController* clone() override { return new RepartitionTaskController(*this); }
    bool validateConfig(const std::string& clusterName);
    bool prepareIndexPath(const RepartitionTaskParam& param, const std::string& taskName,
                          const std::string& clusterName);
    bool checkIndexSchema(const indexlib::config::IndexSchemaPtr& indexSchema);
    bool updateConfig() override
    {
        BS_LOG(WARN, "repartition task not support update config, ignore");
        return true;
    }
    std::string getTaskInfo() override { return "{\"processorCheckpoint\":" + _processorCheckpoint + "}"; }
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

private:
    std::string _processorCheckpoint;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(RepartitionTaskController);

}} // namespace build_service::admin

#endif // ISEARCH_BS_REPARTITIONTASKCONTROLLER_H
