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
#ifndef ISEARCH_BS_REPARTITIONTASK_H
#define ISEARCH_BS_REPARTITIONTASK_H

#include "build_service/common_define.h"
#include "build_service/config/TaskConfig.h"
#include "build_service/task_base/Task.h"
#include "build_service/util/Log.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/merger/filtered_multi_partition_merger.h"

namespace build_service { namespace task_base {

class RepartitionTask : public Task
{
public:
    RepartitionTask();
    ~RepartitionTask();

public:
    static const std::string TASK_NAME;

private:
    RepartitionTask(const RepartitionTask&);
    RepartitionTask& operator=(const RepartitionTask&);

public:
    bool init(TaskInitParam& initParam) override;
    bool handleTarget(const config::TaskTarget& target) override;
    bool isDone(const config::TaskTarget& target) override;

    indexlib::util::CounterMapPtr getCounterMap() override;
    std::string getTaskStatus() override;
    bool hasFatalError() override { return false; }

private:
    virtual indexlib::merger::FilteredMultiPartitionMergerPtr
    prepareFilteredMultiPartitionMerger(versionid_t version, const std::string& targetIndexPath, uint32_t generationId);
    bool getFullMergeOptions(indexlib::config::IndexPartitionOptions& options);
    bool collectMergePartPaths(std::vector<std::string>& mergePartPaths);
    std::string getCheckpointFileName();
    bool parseTaskTarget(const std::string& targetStr, config::TaskTarget& target);

protected:
    virtual bool checkInCheckpoint(const std::string& targetGenerationPath, const config::TaskTarget& target);
    virtual bool readCheckpoint(const std::string& targetGenerationPath, config::TaskTarget& checkpoint);
    virtual bool doMerge(const indexlib::merger::FilteredMultiPartitionMergerPtr& merger,
                         const std::string& mergeMetaVersionDir);
    virtual bool endMerge(const indexlib::merger::FilteredMultiPartitionMergerPtr& merger,
                          const std::string& mergeMetaVersionDir);

private:
    mutable autil::RecursiveThreadMutex _lock;
    config::TaskTarget _currentFinishTarget;
    TaskInitParam _initParam;
    proto::Range _partitionRange;
    std::string _srcIndexRoot;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(RepartitionTask);

}} // namespace build_service::task_base

#endif // ISEARCH_BS_REPARTITIONTASK_H
