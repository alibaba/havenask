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
#ifndef ISEARCH_BS_SINGLEMERGETASKMANAGER_H
#define ISEARCH_BS_SINGLEMERGETASKMANAGER_H

#include "autil/legacy/jsonizable.h"
#include "build_service/admin/controlflow/TaskResourceManager.h"
#include "build_service/common_define.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/util/Log.h"

namespace build_service { namespace admin {
class MergeCrontab;
BS_TYPEDEF_PTR(MergeCrontab);

class SingleMergeTaskManager : public autil::legacy::Jsonizable
{
public:
    SingleMergeTaskManager(const TaskResourceManagerPtr& resourceManager);
    ~SingleMergeTaskManager();

private:
    SingleMergeTaskManager& operator=(const SingleMergeTaskManager&);

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    bool loadFromConfig(const std::string& dataTable, const std::string& clusterName, const std::string& configPath,
                        bool startCrontab);
    bool startMergeCrontab(bool restart);
    void stopMergeCrontab();
    std::string getMergingTask() { return _mergingTask; }
    void syncVersion(const std::string& mergeConfig);
    const std::string& getDataTable() const { return _dataTable; }
    bool hasCrontab() const
    {
        if (_mergeCrontab) {
            return true;
        }
        return false;
    }
    void triggerFullMerge();
    bool generateMergeTask(std::string& mergeTask);
    void mergeTaskDone()
    {
        _hasMergingTask = false;
        _mergingTask = "";
    }
    void setHasMergingTask(bool hasMergingTask) { _hasMergingTask = hasMergingTask; }
    bool hasMergingTask() const { return _hasMergingTask; }
    bool createVersion(const std::string& mergeConfigName);
    std::vector<std::string> getPendingMergeTasks() const;
    void fillStatusForLegacy(std::map<std::string, std::string>& fullMergeConfigs,
                             std::map<std::string, std::string>& mergingTasks,
                             std::map<std::string, std::deque<std::string>>& pendingTasks,
                             std::map<std::string, MergeCrontabPtr>& mergeCrontabs);
    bool operator==(const SingleMergeTaskManager& other) const;
    void clearMergeTask();

private:
    bool checkMergeConfig(config::ResourceReader& resourceReader, const std::string& clusterName);
    config::ResourceReaderPtr getConfigReader();

private:
    std::string _clusterName;
    std::string _dataTable;
    std::string _configPath;
    std::string _fullMergeConfig;
    std::string _mergingTask;
    std::deque<std::string> _pendingTasks;
    MergeCrontabPtr _mergeCrontab;
    bool _hasMergingTask;
    TaskResourceManagerPtr _resourceManager;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(SingleMergeTaskManager);

}} // namespace build_service::admin

#endif // ISEARCH_BS_SINGLEMERGETASKMANAGER_H
