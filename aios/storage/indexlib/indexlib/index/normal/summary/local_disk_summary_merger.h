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

#include "indexlib/common_define.h"
#include "indexlib/index/normal/summary/summary_merger.h"
#include "indexlib/index/summary/config/SummaryConfig.h"
#include "indexlib/index_base/index_meta/merge_task_resource.h"
#include "indexlib/index_base/index_meta/parallel_merge_item.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(file_system, FileWriter);
DECLARE_REFERENCE_CLASS(config, SummaryGroupConfig);

namespace indexlib { namespace index {

class LocalDiskSummaryMerger : public SummaryMerger
{
    struct OutputData {
        VarLenDataWriterPtr dataWriter;
    };

public:
    LocalDiskSummaryMerger(
        const config::SummaryGroupConfigPtr& summaryGroupConfig,
        const index_base::MergeItemHint& hint = index_base::MergeItemHint(),
        const index_base::MergeTaskResourceVector& taskResources = index_base::MergeTaskResourceVector());
    virtual ~LocalDiskSummaryMerger();

public:
    void EndParallelMerge(const index_base::OutputSegmentMergeInfos& outputSegMergeInfos, int32_t totalParallelCount,
                          const std::vector<index_base::MergeTaskResourceVector>& instResourceVec) override;

protected:
    // TODO yiping.typ : parallel merge
    index_base::MergeItemHint mHint;
    index_base::MergeTaskResourceVector mTaskResources;

private:
    virtual VarLenDataParam CreateVarLenDataParam() const override;
    virtual file_system::DirectoryPtr CreateInputDirectory(const index_base::SegmentData& segmentData) const override;
    virtual file_system::DirectoryPtr
    CreateOutputDirectory(const index_base::OutputSegmentMergeInfo& outputSegMergeInfo) const override;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(LocalDiskSummaryMerger);
}} // namespace indexlib::index
