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
#include "future_lite/coro/Lazy.h"
#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"
#include "indexlib/file_system/file/ReadOption.h"
#include "indexlib/index/IDiskIndexer.h"
#include "indexlib/index/IndexerParameter.h"
#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/index/summary/LocalDiskSummaryDiskIndexer.h"
#include "indexlib/index/summary/Types.h"

namespace indexlib {
namespace file_system {
class Directory;
}
namespace document {
class SearchSummaryDocument;
}
} // namespace indexlib

namespace autil::mem_pool {
class Pool;
}

namespace indexlibv2::config {
class SummaryIndexConfig;
}

namespace indexlibv2::index {

class SummaryDiskIndexer : public IDiskIndexer
{
public:
    SummaryDiskIndexer(const IndexerParameter& indexerParam);
    ~SummaryDiskIndexer();

    using SummaryGroupIdVec = std::vector<summarygroupid_t>;
    using SearchSummaryDocVec = LocalDiskSummaryDiskIndexer::SearchSummaryDocVec;

public:
    // override from IDiskIndexer
    Status Open(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                const std::shared_ptr<indexlib::file_system::IDirectory>& summaryDirectory) override;
    size_t EstimateMemUsed(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                           const std::shared_ptr<indexlib::file_system::IDirectory>& summaryDirectory) override;
    size_t EvaluateCurrentMemUsed() override;

public:
    std::shared_ptr<LocalDiskSummaryDiskIndexer> GetLocalDiskSummaryDiskIndexer(summarygroupid_t groupId) const;

public:
    std::pair<Status, bool> GetDocument(docid_t docId, indexlib::document::SearchSummaryDocument* summaryDoc) const
    {
        return GetDocument(docId, _allGroupIds, summaryDoc);
    }

    std::pair<Status, bool> GetDocument(docid_t docId, const SummaryGroupIdVec& groupVec,
                                        indexlib::document::SearchSummaryDocument* summaryDoc) const;

public:
    future_lite::coro::Lazy<indexlib::index::ErrorCodeVec> GetDocument(const std::vector<docid_t>& docIds,
                                                                       autil::mem_pool::Pool* sessionPool,
                                                                       indexlib::file_system::ReadOption option,
                                                                       const SearchSummaryDocVec* docs) const noexcept
    {
        return GetDocument(docIds, _allGroupIds, sessionPool, option, docs);
    }

    future_lite::coro::Lazy<indexlib::index::ErrorCodeVec> GetDocument(const std::vector<docid_t>& docIds,
                                                                       const SummaryGroupIdVec& groupVec,
                                                                       autil::mem_pool::Pool* sessionPool,
                                                                       indexlib::file_system::ReadOption option,
                                                                       const SearchSummaryDocVec* docs) const noexcept;

private:
    std::vector<std::shared_ptr<LocalDiskSummaryDiskIndexer>> _summaryGroups;
    SummaryGroupIdVec _allGroupIds;
    std::shared_ptr<config::SummaryIndexConfig> _summaryIndexConfig;
    IndexerParameter _indexerParam;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
