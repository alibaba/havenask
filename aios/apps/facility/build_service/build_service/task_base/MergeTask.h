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
#ifndef ISEARCH_BS_MERGETASK_H
#define ISEARCH_BS_MERGETASK_H

#include "build_service/common_define.h"
#include "build_service/task_base/TaskBase.h"
#include "build_service/util/Log.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/merger/index_partition_merger.h"

DECLARE_REFERENCE_CLASS(util, CounterMap);
BS_DECLARE_REFERENCE_CLASS(config, CounterConfig);

namespace build_service { namespace task_base {

class MergeTask : public TaskBase
{
private:
    enum MergeStep { MS_ALL, MS_INIT_MERGE, MS_DO_MERGE, MS_END_MERGE, MS_DO_NOTHING };
    static const uint32_t RESERVED_VERSION_NUM;

public:
    struct MergeStatus {
        versionid_t targetVersionId;
        bool isMergeFinished;
        MergeStatus() : targetVersionId(INVALID_VERSION), isMergeFinished(false) {}
    };

public:
    MergeTask(uint32_t backupId = 0, const std::string& epochId = "");
    MergeTask(indexlib::util::MetricProviderPtr metricProvider, uint32_t backupId = 0, const std::string& epochId = "");
    ~MergeTask();

private:
    MergeTask(const MergeTask&);
    MergeTask& operator=(const MergeTask&);

public:
    // virtual for mock
    virtual bool run(const std::string& jobParam, uint32_t instanceId, Mode mode, bool optimize);

    virtual MergeStatus getMergeStatus() { return _mergeStatus; }
    // use for jobMode
    virtual void cleanUselessResource() override;

private:
    bool mergeSinglePart(const proto::PartitionId& partitionId, const std::string& targetIndexPath,
                         const indexlib::config::IndexPartitionOptions& options,
                         const std::pair<MergeStep, uint32_t> mergeStep, bool optimize,
                         const config::CounterConfigPtr& counterConfig);

    bool mergeMultiPart(const proto::PartitionId& partitionId, const std::string& indexRoot,
                        const std::string& targetIndexPath, const indexlib::config::IndexPartitionOptions& options,
                        const std::pair<MergeStep, uint32_t> mergeStep, bool optimize,
                        const config::CounterConfigPtr& counterConfig);

    indexlib::merger::IndexPartitionMergerPtr
    createSinglePartMerger(const proto::PartitionId& partitionId, const std::string& targetIndexPath,
                           const indexlib::config::IndexPartitionOptions& options);

    bool doMerge(indexlib::merger::IndexPartitionMergerPtr& indexPartMerger,
                 const std::pair<MergeStep, uint32_t>& mergeStep, bool optimize, const proto::PartitionId& partitionId);
    std::string prepareBranchWorkerPath(const std::string& rootPath, MergeStep step);

    static void removeObsoleteMergeDir(const std::string& mergeMetaRoot,
                                       indexlib::file_system::FenceContext* fenceContext,
                                       uint32_t reservedVersionNum = RESERVED_VERSION_NUM);

    virtual indexlib::merger::IndexPartitionMergerPtr
    CreateIncParallelPartitionMerger(const proto::PartitionId& partitionId, const std::string& targetIndexPath,
                                     const indexlib::config::IndexPartitionOptions& options,
                                     uint32_t workerPathVersion);
    bool cleanFullStepUselessFile(const std::pair<MergeStep, uint32_t> mergeStep, const std::string& targetIndexPath);

    void ValidateLatestIndexVersion(const std::string& targetIndexPath, versionid_t targetVersionId);

private:
    bool isMultiPartMerge() const;
    std::pair<MergeStep, uint32_t> getMergeStep(Mode mode, uint32_t instanceId) const;
    std::string timeDiffToString(int64_t timeDiff);

protected:
    const static std::string TRASH_DIR_NAME;
    MergeStatus _mergeStatus;
    std::vector<std::string> _uselessPaths;
    uint32_t _backupId;
    indexlib::file_system::FenceContextPtr _fenceContext;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(MergeTask);

}} // namespace build_service::task_base

#endif // ISEARCH_BS_MERGETASK_H
