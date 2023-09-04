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
#ifndef ISEARCH_BS_MERGECRONTABTASK_H
#define ISEARCH_BS_MERGECRONTABTASK_H

#include "build_service/admin/controlflow/TaskBase.h"
#include "build_service/admin/taskcontroller/SingleMergeTaskManager.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service { namespace admin {

class MergeCrontabTask : public TaskBase
{
public:
    MergeCrontabTask(const std::string& id, const std::string& type, const TaskResourceManagerPtr& resMgr)
        : TaskBase(id, type, resMgr)
    {
    }

    MergeCrontabTask();
    ~MergeCrontabTask();
    MergeCrontabTask(const MergeCrontabTask&);

private:
    MergeCrontabTask& operator=(const MergeCrontabTask&);

public:
    bool doInit(const KeyValueMap& kvMap) override;
    bool doStart(const KeyValueMap& kvMap) override;
    bool doFinish(const KeyValueMap& kvMap) override;
    void doSyncTaskProperty(proto::WorkerNodes& workerNodes) override;
    TaskBase* clone() override;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    bool updateConfig() override;
    bool CheckKeyAuthority(const std::string& key) override;
    bool createVersion(const std::string& mergeConfigName);
    bool isFinished(proto::WorkerNodes& workerNodes) override { return false; }
    bool executeCmd(const std::string& cmdName, const KeyValueMap& params) override;

public:
    std::vector<std::string> getPendingMergeTasks() const;

public:
    static const std::string MERGE_CRONTAB;

private:
    SingleMergeTaskManagerPtr _impl;
    proto::BuildStep _buildStep;
    std::string _clusterName;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(MergeCrontabTask);

}} // namespace build_service::admin

#endif // ISEARCH_BS_MERGECRONTABTASK_H
