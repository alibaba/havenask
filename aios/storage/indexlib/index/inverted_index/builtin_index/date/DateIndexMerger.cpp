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
#include "indexlib/index/inverted_index/builtin_index/date/DateIndexMerger.h"

#include "indexlib/framework/Segment.h"
#include "indexlib/framework/SegmentMeta.h"
#include "indexlib/index/inverted_index/builtin_index/date/TimeRangeInfo.h"

namespace indexlib::index {
namespace {
} // namespace

AUTIL_LOG_SETUP(indexlib.index, DateIndexMerger);

DateIndexMerger::DateIndexMerger() {}

DateIndexMerger::~DateIndexMerger() {}

std::string DateIndexMerger::GetIdentifier() const { return "index.merger.date"; }

Status
DateIndexMerger::DoMerge(const SegmentMergeInfos& segMergeInfos,
                         const std::shared_ptr<indexlibv2::framework::IndexTaskResourceManager>& taskResourceManager)
{
    auto status = InvertedIndexMerger::DoMerge(segMergeInfos, taskResourceManager);
    RETURN_IF_STATUS_ERROR(status, "merge fail, indexType[%s] indexName[%s]", _indexConfig->GetIndexType().c_str(),
                           _indexName.c_str());
    return MergeRangeInfo(segMergeInfos);
}

Status DateIndexMerger::MergeRangeInfo(const SegmentMergeInfos& segMergeInfos)
{
    TimeRangeInfo info;
    for (const auto& srcSeg : segMergeInfos.srcSegments) {
        auto segDir = srcSeg.segment->GetSegmentDirectory();
        auto indexDir = GetIndexDirectory(segDir);
        if (!indexDir || !indexDir->IsExist(RANGE_INFO_FILE_NAME)) {
            continue;
        }
        TimeRangeInfo tmp;
        auto status = tmp.Load(indexDir->GetIDirectory());
        RETURN_IF_STATUS_ERROR(status, "load range info fail, directory[%s]", indexDir->GetLogicalPath().c_str());
        if (tmp.IsValid()) {
            info.Update(tmp.GetMinTime());
            info.Update(tmp.GetMaxTime());
        }
    }
    for (const auto& segMeta : segMergeInfos.targetSegments) {
        auto segDir = segMeta->segmentDir;
        auto indexDir = GetIndexDirectory(segDir);
        if (!indexDir) {
            continue;
        }
        auto status = info.Store(indexDir->GetIDirectory());
        RETURN_IF_STATUS_ERROR(status, "store range info fail, segmentDir[%s]", segDir->GetLogicalPath().c_str());
    }
    return Status::OK();
}

} // namespace indexlib::index
