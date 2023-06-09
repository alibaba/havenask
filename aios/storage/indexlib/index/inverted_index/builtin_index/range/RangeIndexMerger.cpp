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
#include "indexlib/index/inverted_index/builtin_index/range/RangeIndexMerger.h"

#include "indexlib/framework/Segment.h"
#include "indexlib/framework/SegmentMeta.h"
#include "indexlib/index/inverted_index/Common.h"
#include "indexlib/index/inverted_index/builtin_index/range/RangeInfo.h"
#include "indexlib/index/inverted_index/builtin_index/range/RangeLevelIndexMerger.h"
#include "indexlib/index/inverted_index/config/RangeIndexConfig.h"

namespace indexlib::index {
namespace {
using indexlibv2::config::IIndexConfig;
} // namespace

AUTIL_LOG_SETUP(indexlib.index, RangeIndexMerger);

RangeIndexMerger::RangeIndexMerger()
    : _bottomLevelIndexMerger(new RangeLevelIndexMerger())
    , _highLevelIndexMerger(new RangeLevelIndexMerger())
{
}

RangeIndexMerger::~RangeIndexMerger() {}

Status RangeIndexMerger::Init(const std::shared_ptr<IIndexConfig>& indexConfig,
                              const std::map<std::string, std::any>& params)
{
    _indexConfig = std::dynamic_pointer_cast<indexlibv2::config::InvertedIndexConfig>(indexConfig);
    assert(_indexConfig);
    _indexName = _indexConfig->GetIndexName();
    auto rangeIndexConfig = std::dynamic_pointer_cast<indexlibv2::config::RangeIndexConfig>(indexConfig);
    auto bottomLevelConfig = rangeIndexConfig->GetBottomLevelIndexConfig();
    auto status = _bottomLevelIndexMerger->Init(_indexName, bottomLevelConfig, params);
    RETURN_IF_STATUS_ERROR(status, "range index merger init fail, indexName[%s]", _indexName.c_str());
    auto highLevelConfig = rangeIndexConfig->GetHighLevelIndexConfig();
    status = _highLevelIndexMerger->Init(_indexName, highLevelConfig, params);
    RETURN_IF_STATUS_ERROR(status, "range index merger init fail, indexName[%s]", _indexName.c_str());
    return Status::OK();
}

std::string RangeIndexMerger::GetIdentifier() const { return "index.merger.range"; }

void RangeIndexMerger::PrepareIndexOutputSegmentResource(
    const std::vector<SourceSegment>& srcSegments,
    const std::vector<std::shared_ptr<indexlibv2::framework::SegmentMeta>>& targetSegments)
{
    for (size_t i = 0; i < targetSegments.size(); i++) {
        std::shared_ptr<file_system::Directory> segDir = targetSegments[i]->segmentDir;
        auto subDir = segDir->MakeDirectory(INVERTED_INDEX_PATH);
        // TODO(yonghao.fyh) : support parallel merge dir
        file_system::RemoveOption removeOption = file_system::RemoveOption::MayNonExist();
        subDir->RemoveDirectory(_indexName, /*mayNonExist=*/removeOption);
        subDir->MakeDirectory(_indexName);
    }
}

std::shared_ptr<OnDiskIndexIteratorCreator> RangeIndexMerger::CreateOnDiskIndexIteratorCreator()
{
    return std::shared_ptr<OnDiskIndexIteratorCreator>(
        new OnDiskRangeIndexIterator::Creator(_indexFormatOption.GetPostingFormatOption(), _ioConfig, _indexConfig));
}

Status
RangeIndexMerger::DoMerge(const SegmentMergeInfos& segMergeInfos,
                          const std::shared_ptr<indexlibv2::framework::IndexTaskResourceManager>& taskResourceManager)
{
    PrepareIndexOutputSegmentResource(segMergeInfos.srcSegments, segMergeInfos.targetSegments);
    auto status = _bottomLevelIndexMerger->Merge(segMergeInfos, taskResourceManager);
    RETURN_IF_STATUS_ERROR(status, "range index merge fail, indexName[%s]", _indexName.c_str());
    status = _highLevelIndexMerger->Merge(segMergeInfos, taskResourceManager);
    RETURN_IF_STATUS_ERROR(status, "range index merge fail, indexName[%s]", _indexName.c_str());
    return MergeRangeInfo(segMergeInfos);
}

Status RangeIndexMerger::MergeRangeInfo(const SegmentMergeInfos& segMergeInfos)
{
    RangeInfo info;
    for (const auto& srcSeg : segMergeInfos.srcSegments) {
        auto segDir = srcSeg.segment->GetSegmentDirectory();
        auto indexDir = GetIndexDirectory(segDir);
        if (!indexDir || !indexDir->IsExist(RANGE_INFO_FILE_NAME)) {
            continue;
        }
        RangeInfo tmp;
        auto status = tmp.Load(indexDir->GetIDirectory());
        RETURN_IF_STATUS_ERROR(status, "load range info fail, directory[%s]", indexDir->GetLogicalPath().c_str());
        if (tmp.IsValid()) {
            info.Update(tmp.GetMinNumber());
            info.Update(tmp.GetMaxNumber());
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
