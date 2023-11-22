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

#include <memory>
#include <set>
#include <stdint.h>
#include <string>
#include <vector>

#include "build_service/common_define.h"
#include "build_service/config/ConfigDefine.h"
#include "build_service/config/TaskTarget.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/task_base/Task.h"
#include "build_service/util/Log.h"
#include "indexlib/base/Types.h"
#include "indexlib/config/index_schema.h"
#include "indexlib/framework/VersionCoord.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/counter/CounterMap.h"

namespace build_service { namespace task_base {

class ResetVersionTask : public Task
{
public:
    ResetVersionTask();
    ~ResetVersionTask();

    ResetVersionTask(const ResetVersionTask&) = delete;
    ResetVersionTask& operator=(const ResetVersionTask&) = delete;
    ResetVersionTask(ResetVersionTask&&) = delete;
    ResetVersionTask& operator=(ResetVersionTask&&) = delete;

public:
    inline static const std::string TASK_NAME = config::BS_TASK_RESET_VERSION;

public:
    bool init(Task::TaskInitParam& initParam) override;
    bool handleTarget(const config::TaskTarget& target) override;
    bool isDone(const config::TaskTarget& target) override;
    indexlib::util::CounterMapPtr getCounterMap() override;
    std::string getTaskStatus() override;
    bool hasFatalError() override { return false; }
    void handleFatalError() override;

private:
    bool constructPartitionIndexRoot();
    bool validateVersionInAllPartition() const;
    bool cleanPartitionLevelIndex(const std::string& rootDirStr) const;
    bool calculateReservedVersion(const std::shared_ptr<indexlib::file_system::Directory>& rootDir,
                                  indexlib::versionid_t resetVersionId,
                                  std::set<indexlibv2::framework::VersionCoord>& reservedVersionSet,
                                  indexlib::versionid_t& maxVersionId) const;
    bool cleanObsoleteSegments(const std::shared_ptr<indexlib::file_system::Directory>& rootDir) const;
    bool cleanIndex() const;
    bool fetchParamFromTaskTarget(const config::TaskTarget& target);

private:
    Task::TaskInitParam _taskInitParam;
    bool _isFinished = false;
    uint32_t _partitionCount = 0;
    proto::BuildId _buildId;
    std::string _clusterName;
    std::string _indexRoot;
    indexlib::versionid_t _versionId;
    std::vector<std::string> _partitionDir;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(ResetVersionTask);

}} // namespace build_service::task_base
