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
#include "indexlib/index/inverted_index/Common.h"
#include "indexlib/index/inverted_index/builtin_index/date/TimeRangeInfo.h"
#include "indexlib/index/normal/inverted_index/builtin_index/text/text_index_merger.h"
#include "indexlib/index/util/segment_directory_base.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index { namespace legacy {

class DateIndexMerger : public TextIndexMerger
{
public:
    DECLARE_INDEX_MERGER_IDENTIFIER(date);
    DECLARE_INDEX_MERGER_CREATOR(DateIndexMerger, it_datetime);

public:
    void Merge(const index::MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
               const index_base::OutputSegmentMergeInfos& outputSegMergeInfos) override
    {
        TextIndexMerger::Merge(resource, segMergeInfos, outputSegMergeInfos);
        MergeRangeInfo(resource, segMergeInfos, outputSegMergeInfos);
    }

    virtual void SortByWeightMerge(const index::MergerResource& resource,
                                   const index_base::SegmentMergeInfos& segMergeInfos,
                                   const index_base::OutputSegmentMergeInfos& outputSegMergeInfos) override
    {
        TextIndexMerger::SortByWeightMerge(resource, segMergeInfos, outputSegMergeInfos);
        MergeRangeInfo(resource, segMergeInfos, outputSegMergeInfos);
    }

private:
    void MergeRangeInfo(const index::MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
                        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DateIndexMerger);
/////////////////////////////////////////////
IE_LOG_SETUP(legacy, DateIndexMerger);

void DateIndexMerger::MergeRangeInfo(const index::MergerResource& resource,
                                     const index_base::SegmentMergeInfos& segMergeInfos,
                                     const index_base::OutputSegmentMergeInfos& outputSegMergeInfos)
{
    TimeRangeInfo info;
    index_base::PartitionDataPtr partitionData = mSegmentDirectory->GetPartitionData();
    assert(partitionData);
    for (size_t i = 0; i < segMergeInfos.size(); ++i) {
        index_base::SegmentData segData = partitionData->GetSegmentData(segMergeInfos[i].segmentId);
        file_system::DirectoryPtr indexDirectory = segData.GetIndexDirectory(mIndexConfig->GetIndexName(), false);
        if (!indexDirectory || !indexDirectory->IsExist(RANGE_INFO_FILE_NAME)) {
            continue;
        }
        TimeRangeInfo tmp;
        auto status = tmp.Load(indexDirectory->GetIDirectory());
        if (!status.IsOK()) {
            INDEXLIB_FATAL_ERROR(IndexCollapsed, "RangeInfo file load failed. [%s]",
                                 indexDirectory->GetPhysicalPath("").c_str());
        }

        if (tmp.IsValid()) {
            info.Update(tmp.GetMinTime());
            info.Update(tmp.GetMaxTime());
        }
    }
    for (size_t i = 0; i < outputSegMergeInfos.size(); ++i) {
        const file_system::DirectoryPtr& indexDirectory = outputSegMergeInfos[i].directory;
        const file_system::DirectoryPtr& mergeDir = GetMergeDir(indexDirectory, false);
        std::string timeRangeStr = autil::legacy::ToJsonString(info);
        mergeDir->Store(RANGE_INFO_FILE_NAME, timeRangeStr);
    }
}
}}} // namespace indexlib::index::legacy
