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
#include "indexlib/index/inverted_index/builtin_index/range/RangeLevelIndexMerger.h"

#include "indexlib/framework/SegmentMeta.h"
#include "indexlib/index/inverted_index/Common.h"
#include "indexlib/index/inverted_index/IndexOutputSegmentResource.h"
#include "indexlib/index/inverted_index/builtin_index/range/OnDiskRangeIndexIteratorCreator.h"

namespace indexlib::index {
namespace {
using indexlibv2::config::IIndexConfig;
} // namespace

AUTIL_LOG_SETUP(indexlib.index, RangeLevelIndexMerger);

RangeLevelIndexMerger::RangeLevelIndexMerger() {}

RangeLevelIndexMerger::~RangeLevelIndexMerger() {}

Status RangeLevelIndexMerger::Init(const std::shared_ptr<IIndexConfig>& indexConfig,
                                   const std::map<std::string, std::any>& params)
{
    return InvertedIndexMerger::Init(indexConfig, params);
}

Status RangeLevelIndexMerger::Init(const std::string& parentIndexName, const std::shared_ptr<IIndexConfig>& indexConfig,
                                   const std::map<std::string, std::any>& params)
{
    auto status = InvertedIndexMerger::Init(indexConfig, params);
    RETURN_IF_STATUS_ERROR(status, "range level index merger init fail");
    _parentIndexName = parentIndexName;
    return Status::OK();
}

std::string RangeLevelIndexMerger::GetIdentifier() const { return "index.merger.range_level"; }

std::shared_ptr<file_system::Directory>
RangeLevelIndexMerger::GetIndexDirectory(const std::shared_ptr<file_system::Directory>& segDir) const
{
    if (!segDir) {
        return nullptr;
    }
    auto subDir = segDir->GetDirectory(INVERTED_INDEX_PATH, /*throwExceptionIfNotExist*/ false);
    if (!subDir) {
        return nullptr;
    }
    auto parentDir = subDir->GetDirectory(_parentIndexName, /*throwExceptionIfNotExist*/ false);
    if (!parentDir) {
        return nullptr;
    }
    auto indexDirectory = parentDir->GetDirectory(_indexName, /*throwExceptionIfNotExist*/ false);
    if (!indexDirectory) {
        return nullptr;
    }
    return indexDirectory;
}

void RangeLevelIndexMerger::PrepareIndexOutputSegmentResource(
    const std::vector<SourceSegment>& srcSegments,
    const std::vector<std::shared_ptr<indexlibv2::framework::SegmentMeta>>& targetSegments)
{
    for (size_t i = 0; i < targetSegments.size(); i++) {
        std::shared_ptr<file_system::Directory> segDir = targetSegments[i]->segmentDir;
        auto subDir = segDir->MakeDirectory(INVERTED_INDEX_PATH);
        auto parentDir = subDir->MakeDirectory(_parentIndexName);
        // TODO(yonghao.fyh) : support parallel merge dir
        file_system::RemoveOption removeOption = file_system::RemoveOption::MayNonExist();
        parentDir->RemoveDirectory(_indexConfig->GetIndexName(), /*mayNonExist=*/removeOption);
        auto mergeDir = parentDir->MakeDirectory(_indexConfig->GetIndexName());
        std::string optionString = IndexFormatOption::ToString(_indexFormatOption);
        mergeDir->Store(INDEX_FORMAT_OPTION_FILE_NAME, optionString);
        auto outputResource = std::make_shared<IndexOutputSegmentResource>();
        auto segmentInfo = targetSegments[i]->segmentInfo;
        assert(segmentInfo);
        auto [status, segmentStatistics] = segmentInfo->GetSegmentStatistics();
        auto segmentStatisticsPtr = std::make_shared<indexlibv2::framework::SegmentStatistics>(segmentStatistics);
        if (!status.IsOK()) {
            AUTIL_LOG(WARN, "segment statistics jsonize failed");
            segmentStatisticsPtr = nullptr;
        }
        outputResource->Init(mergeDir, _indexConfig, _ioConfig, /*SegmentStatistics*/ segmentStatisticsPtr,
                             &_simplePool, false);
        _indexOutputSegmentResources.push_back(outputResource);
    }
}

std::shared_ptr<OnDiskIndexIteratorCreator> RangeLevelIndexMerger::CreateOnDiskIndexIteratorCreator()
{
    return std::make_shared<OnDiskRangeIndexIteratorCreator>(_indexFormatOption.GetPostingFormatOption(), _ioConfig,
                                                             _indexConfig, _parentIndexName);
}

} // namespace indexlib::index
