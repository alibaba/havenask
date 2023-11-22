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

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "fslib/fslib.h"
#include "future_lite/coro/Lazy.h"
#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"
#include "indexlib/file_system/file/ReadOption.h"
#include "indexlib/index/DiskIndexerParameter.h"
#include "indexlib/index/common/ErrorCode.h"

namespace indexlib {
namespace file_system {
class IDirectory;
}
namespace document {
class SearchSummaryDocument;
}
} // namespace indexlib
namespace autil { namespace mem_pool {
class Pool;
}} // namespace autil::mem_pool

namespace indexlibv2::config {
class IIndexConfig;
class SummaryGroupConfig;
} // namespace indexlibv2::config
namespace indexlibv2::index {
class VarLenDataReader;

class LocalDiskSummaryDiskIndexer : private autil::NoCopyable
{
public:
    LocalDiskSummaryDiskIndexer(const DiskIndexerParameter& indexerParam);
    ~LocalDiskSummaryDiskIndexer() = default;
    using SearchSummaryDocVec = std::vector<indexlib::document::SearchSummaryDocument*>;

public:
    static std::pair<Status, std::shared_ptr<VarLenDataReader>> GetSummaryVarLenDataReader(
        int64_t docCount, const std::shared_ptr<indexlibv2::config::SummaryGroupConfig>& summaryGroupConfig,
        const std::shared_ptr<indexlib::file_system::IDirectory>& summaryDirectory, bool isOnline);
    Status Open(const std::shared_ptr<indexlibv2::config::SummaryGroupConfig>& summaryGroupConfig,
                const std::shared_ptr<indexlib::file_system::IDirectory>& summaryDirectory);
    size_t EstimateMemUsed(const std::shared_ptr<indexlibv2::config::SummaryGroupConfig>& summaryGroupConfig,
                           const std::shared_ptr<indexlib::file_system::IDirectory>& summaryDirectory);
    size_t EvaluateCurrentMemUsed();

    std::shared_ptr<VarLenDataReader> GetDataReader() const { return _dataReader; }

public:
    std::pair<Status, bool> GetDocument(docid_t localDocId,
                                        indexlib::document::SearchSummaryDocument* summaryDoc) const;
    future_lite::coro::Lazy<indexlib::index::ErrorCodeVec> GetDocument(const std::vector<docid_t>& docIds,
                                                                       autil::mem_pool::Pool* sessionPool,
                                                                       indexlib::file_system::ReadOption readOption,
                                                                       const SearchSummaryDocVec* docs) noexcept;

private:
    static std::pair<Status, std::shared_ptr<indexlib::file_system::IDirectory>>
    GetSummaryDirectory(const std::shared_ptr<indexlibv2::config::SummaryGroupConfig>& summaryGroupConfig,
                        const std::shared_ptr<indexlib::file_system::IDirectory>& summaryDirectory);

protected:
    std::shared_ptr<VarLenDataReader> _dataReader;
    std::shared_ptr<indexlibv2::config::SummaryGroupConfig> _summaryGroupConfig;
    DiskIndexerParameter _indexerParam;
    static constexpr size_t DEFAULT_SEGMENT_POOL_SIZE = 512 * 1024;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
