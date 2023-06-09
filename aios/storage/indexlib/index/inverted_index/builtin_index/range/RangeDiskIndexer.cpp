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
#include "indexlib/index/inverted_index/builtin_index/range/RangeDiskIndexer.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/index/inverted_index/builtin_index/range/RangeLeafReader.h"
#include "indexlib/index/inverted_index/builtin_index/range/RangeLevelLeafReader.h"
#include "indexlib/index/inverted_index/config/RangeIndexConfig.h"

namespace indexlib::index {
namespace {
using indexlibv2::config::IIndexConfig;
using indexlibv2::config::RangeIndexConfig;
} // namespace

AUTIL_LOG_SETUP(indexlib.index, RangeDiskIndexer);

RangeDiskIndexer::RangeDiskIndexer(const indexlibv2::index::IndexerParameter& indexerParam)
    : InvertedDiskIndexer(indexerParam)
{
    _bottomLevelDiskIndexer = std::make_shared<InvertedDiskIndexer>(indexerParam);
    _highLevelDiskIndexer = std::make_shared<InvertedDiskIndexer>(indexerParam);
}

RangeDiskIndexer::~RangeDiskIndexer() {}

Status RangeDiskIndexer::Open(const std::shared_ptr<IIndexConfig>& indexConfig,
                              const std::shared_ptr<file_system::IDirectory>& indexDirectory)
{
    if (0 == _indexerParam.docCount) {
        // there is no document here, so do nothing
        AUTIL_LOG(INFO, "doc count is zero in index [%s] for segment [%d], just do nothing",
                  indexConfig->GetIndexName().c_str(), _indexerParam.segmentId);
        return Status::OK();
    }
    auto rangeConfig = std::dynamic_pointer_cast<RangeIndexConfig>(indexConfig);
    assert(rangeConfig);
    auto indexName = rangeConfig->GetIndexName();
    auto [statusSubDir, subDir] = indexDirectory->GetDirectory(indexName).StatusWith();
    if (!statusSubDir.IsOK() || !subDir) {
        AUTIL_LOG(ERROR, "fail to get subDir, indexName[%s] indexType[%s]", indexName.c_str(),
                  rangeConfig->GetIndexType().c_str());
        return Status::InternalError("directory for range index: %s does not exist", indexName.c_str());
    }

    auto status = _bottomLevelDiskIndexer->Open(rangeConfig->GetBottomLevelIndexConfig(), subDir);
    RETURN_IF_STATUS_ERROR(status, "bottom disk indexer open fail, indexName[%s]", indexName.c_str());
    status = _highLevelDiskIndexer->Open(rangeConfig->GetHighLevelIndexConfig(), subDir);
    RETURN_IF_STATUS_ERROR(status, "high disk indexer open fail, indexName[%s]", indexName.c_str());
    status = _rangeInfo.Load(subDir);
    RETURN_IF_STATUS_ERROR(status, "load range info failed, directory[%s]", subDir->GetLogicalPath().c_str());
    {
        auto bottomReader = _bottomLevelDiskIndexer->GetReader();
        auto bottomLevelLeafReader = std::make_shared<RangeLevelLeafReader>(
            bottomReader->GetIndexConfig(), bottomReader->GetIndexFormatOption(), bottomReader->GetDocCount(),
            bottomReader->GetDictionaryReader(), bottomReader->GetPostingReader());
        auto highReader = _highLevelDiskIndexer->GetReader();
        auto highLevelLeafReader = std::make_shared<RangeLevelLeafReader>(
            highReader->GetIndexConfig(), highReader->GetIndexFormatOption(), highReader->GetDocCount(),
            highReader->GetDictionaryReader(), highReader->GetPostingReader());
        _rangeLeafReader = std::make_shared<RangeLeafReader>(bottomLevelLeafReader, highLevelLeafReader, _rangeInfo);
    }
    return Status::OK();
}

size_t RangeDiskIndexer::EstimateMemUsed(const std::shared_ptr<IIndexConfig>& indexConfig,
                                         const std::shared_ptr<file_system::IDirectory>& indexDirectory)
{
    size_t memUsed = _bottomLevelDiskIndexer->EstimateMemUsed(indexConfig, indexDirectory);
    memUsed += _highLevelDiskIndexer->EstimateMemUsed(indexConfig, indexDirectory);
    return memUsed;
}

size_t RangeDiskIndexer::EvaluateCurrentMemUsed()
{
    size_t memUsed = _bottomLevelDiskIndexer->EvaluateCurrentMemUsed();
    memUsed += _highLevelDiskIndexer->EvaluateCurrentMemUsed();
    return memUsed;
}

std::shared_ptr<InvertedLeafReader> RangeDiskIndexer::GetReader() const { return _rangeLeafReader; }

} // namespace indexlib::index
