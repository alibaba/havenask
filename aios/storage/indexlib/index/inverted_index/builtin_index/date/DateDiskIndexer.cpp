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
#include "indexlib/index/inverted_index/builtin_index/date/DateDiskIndexer.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/index/inverted_index/builtin_index/date/DateLeafReader.h"
#include "indexlib/index/inverted_index/builtin_index/date/TimeRangeInfo.h"
#include "indexlib/index/inverted_index/config/DateIndexConfig.h"

namespace indexlib::index {
namespace {
using indexlibv2::config::IIndexConfig;
} // namespace

AUTIL_LOG_SETUP(indexlib.index, DateDiskIndexer);

DateDiskIndexer::DateDiskIndexer(const indexlibv2::index::IndexerParameter& indexerParam)
    : InvertedDiskIndexer(indexerParam)
{
}

DateDiskIndexer::~DateDiskIndexer() {}

Status DateDiskIndexer::Open(const std::shared_ptr<IIndexConfig>& indexConfig,
                             const std::shared_ptr<file_system::IDirectory>& indexDirectory)
{
    if (0 == _indexerParam.docCount) {
        // there is no document here, so do nothing
        AUTIL_LOG(INFO, "doc count is zero in index [%s] for segment [%d], just do nothing",
                  indexConfig->GetIndexName().c_str(), _indexerParam.segmentId);
        return Status::OK();
    }
    const std::string& indexName = indexConfig->GetIndexName();
    auto status = InvertedDiskIndexer::Open(indexConfig, indexDirectory);
    RETURN_IF_STATUS_ERROR(status, "inverted disk indexer open fail, indexName[%s]", indexName.c_str());
    auto [status2, subDir] = indexDirectory->GetDirectory(indexName).StatusWith();
    if (!status2.IsOK() || !subDir) {
        auto status = Status::InternalError("fail to get subDir, indexName[%s] indexType[%s]", indexName.c_str(),
                                            indexConfig->GetIndexType().c_str());
        AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
        return status;
    }
    TimeRangeInfo rangeInfo;
    status = rangeInfo.Load(subDir);
    RETURN_IF_STATUS_ERROR(status, "load time range info fail, directory[%s]", subDir->GetLogicalPath().c_str());
    auto dateIndexConfig = std::dynamic_pointer_cast<indexlibv2::config::DateIndexConfig>(indexConfig);
    assert(dateIndexConfig);
    auto invertedReader = InvertedDiskIndexer::GetReader();
    assert(invertedReader);
    _dateLeafReader = std::make_shared<DateLeafReader>(
        dateIndexConfig, invertedReader->GetIndexFormatOption(), invertedReader->GetDocCount(),
        invertedReader->GetDictionaryReader(), invertedReader->GetPostingReader(), rangeInfo);
    return Status::OK();
}

std::shared_ptr<InvertedLeafReader> DateDiskIndexer::GetReader() const { return _dateLeafReader; }

} // namespace indexlib::index
