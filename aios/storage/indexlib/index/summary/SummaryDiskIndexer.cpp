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
#include "indexlib/index/summary/SummaryDiskIndexer.h"

#include "autil/TimeoutTerminator.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/document/normal/SearchSummaryDocument.h"
#include "indexlib/document/normal/SummaryGroupFormatter.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/index/summary/Constant.h"
#include "indexlib/index/summary/config/SummaryGroupConfig.h"
#include "indexlib/index/summary/config/SummaryIndexConfig.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, SummaryDiskIndexer);

SummaryDiskIndexer::SummaryDiskIndexer(const DiskIndexerParameter& indexerParam) : _indexerParam(indexerParam) {}

SummaryDiskIndexer::~SummaryDiskIndexer() {}

Status SummaryDiskIndexer::Open(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory)
{
    _summaryIndexConfig = std::dynamic_pointer_cast<config::SummaryIndexConfig>(indexConfig);
    if (nullptr == _summaryIndexConfig) {
        AUTIL_LOG(ERROR, "can not find summary index config");
        return Status::ConfigError("can not find summary index config");
    }
    if (!_summaryIndexConfig->NeedStoreSummary()) {
        return Status::OK(); // in this case, there is no data in summary
    }

    if (0 == _indexerParam.docCount) {
        // there is no document here, so do nothing
        AUTIL_LOG(INFO, "doc count is zero in index [%s] for segment [%d], just do nothing",
                  indexConfig->GetIndexName().c_str(), _indexerParam.segmentId);
        return Status::OK();
    }

    if (nullptr == indexDirectory) {
        RETURN_STATUS_ERROR(InternalError, "can not get summary index directory");
    }
    for (summarygroupid_t groupId = 0; groupId < _summaryIndexConfig->GetSummaryGroupConfigCount(); ++groupId) {
        const auto& summaryGroupConfig = _summaryIndexConfig->GetSummaryGroupConfig(groupId);
        auto summaryGroupDiskIndexer = std::make_shared<LocalDiskSummaryDiskIndexer>(_indexerParam);
        if (!summaryGroupDiskIndexer->Open(summaryGroupConfig, indexDirectory).IsOK()) {
            AUTIL_LOG(ERROR, "open summary group reader[%s] failed", summaryGroupConfig->GetGroupName().c_str());
            return Status::InternalError("open summary group reader failed");
        }
        _summaryGroups.push_back(summaryGroupDiskIndexer);
        _allGroupIds.push_back(groupId);
    }
    return Status::OK();
}

size_t SummaryDiskIndexer::EstimateMemUsed(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                           const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory)
{
    if (!indexDirectory) {
        return 0;
    }
    auto summaryIndexConfig = std::dynamic_pointer_cast<config::SummaryIndexConfig>(indexConfig);
    if (nullptr == summaryIndexConfig) {
        AUTIL_LOG(ERROR, "can not find summary index config");
        return 0;
    }
    if (!summaryIndexConfig->NeedStoreSummary()) {
        return 0; // in this case, there is no data in summary
    }
    size_t totalMemUsed = 0;
    assert(indexDirectory);
    auto summaryIDir = indexDirectory;
    assert(summaryIDir);
    DiskIndexerParameter emptyIndexerParam;
    for (summarygroupid_t groupId = 0; groupId < summaryIndexConfig->GetSummaryGroupConfigCount(); ++groupId) {
        const auto& summaryGroupConfig = summaryIndexConfig->GetSummaryGroupConfig(groupId);
        auto summaryGroupDiskIndexer = std::make_shared<LocalDiskSummaryDiskIndexer>(emptyIndexerParam);
        totalMemUsed += summaryGroupDiskIndexer->EstimateMemUsed(summaryGroupConfig, summaryIDir);
    }
    return totalMemUsed;
}

size_t SummaryDiskIndexer::EvaluateCurrentMemUsed()
{
    size_t totalMemUsed = 0;
    for (auto& summaryGroupDiskIndexer : _summaryGroups) {
        totalMemUsed += summaryGroupDiskIndexer->EvaluateCurrentMemUsed();
    }
    return totalMemUsed;
}

std::shared_ptr<LocalDiskSummaryDiskIndexer>
SummaryDiskIndexer::GetLocalDiskSummaryDiskIndexer(summarygroupid_t groupId) const
{
    if (unlikely(groupId < 0 || groupId >= (summarygroupid_t)_summaryGroups.size())) {
        AUTIL_LOG(WARN, "invalid summary group id [%d], max group id [%d]", groupId,
                  (summarygroupid_t)_summaryGroups.size());
        return nullptr;
    }
    return _summaryGroups[groupId];
}

std::pair<Status, bool> SummaryDiskIndexer::GetDocument(docid_t docId, const SummaryGroupIdVec& groupVec,
                                                        indexlib::document::SearchSummaryDocument* summaryDoc) const
{
    for (size_t i = 0; i < groupVec.size(); ++i) {
        if (unlikely(groupVec[i] < 0 || groupVec[i] >= (summarygroupid_t)_summaryGroups.size())) {
            AUTIL_LOG(WARN, "invalid summary group id [%d], max group id [%d]", groupVec[i],
                      (summarygroupid_t)_summaryGroups.size());
            return std::make_pair(Status::OK(), false);
        }
        auto [status, ret] = _summaryGroups[groupVec[i]]->GetDocument(docId, summaryDoc);
        RETURN2_IF_STATUS_ERROR(status, false, "get document from summary disk indexer fail, doc id = [%d]", docId);
        if (!ret) {
            return std::make_pair(Status::OK(), false);
        }
    }
    return std::make_pair(Status::OK(), true);
}

future_lite::coro::Lazy<indexlib::index::ErrorCodeVec>
SummaryDiskIndexer::GetDocument(const std::vector<docid_t>& docIds, const SummaryGroupIdVec& groupVec,
                                autil::mem_pool::Pool* sessionPool, indexlib::file_system::ReadOption option,
                                const SearchSummaryDocVec* docs) const noexcept
{
    std::vector<indexlib::index::ErrorCode> ec;
    for (size_t i = 0; i < groupVec.size(); ++i) {
        if (unlikely(groupVec[i] < 0 || groupVec[i] >= (summarygroupid_t)_summaryGroups.size())) {
            AUTIL_LOG(WARN, "invalid summary group id [%d], max group id [%d]", groupVec[i],
                      (summarygroupid_t)_summaryGroups.size());
            co_return std::vector<indexlib::index::ErrorCode>(docIds.size(), indexlib::index::ErrorCode::Runtime);
        }
    }

    std::vector<future_lite::coro::Lazy<std::vector<indexlib::index::ErrorCode>>> tasks;
    for (size_t i = 0; i < groupVec.size(); ++i) {
        tasks.push_back(_summaryGroups[groupVec[i]]->GetDocument(docIds, sessionPool, option, docs));
    }
    auto taskResults = co_await future_lite::coro::collectAll(std::move(tasks));
    for (size_t i = 0; i < docIds.size(); ++i) {
        ec.push_back(indexlib::index::ErrorCode::OK);
    }
    for (auto& taskResult : taskResults) {
        assert(!taskResult.hasError());
        auto groupResult = taskResult.value();
        for (size_t docIdx = 0; docIdx < groupResult.size(); ++docIdx) {
            if (groupResult[docIdx] != indexlib::index::ErrorCode::OK) {
                ec[docIdx] = groupResult[docIdx];
            }
        }
    }
    co_return ec;
}

} // namespace indexlibv2::index
