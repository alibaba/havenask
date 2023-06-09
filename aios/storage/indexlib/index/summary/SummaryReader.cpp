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
#include "indexlib/index/summary/SummaryReader.h"

#include "indexlib/document/normal/SearchSummaryDocument.h"
#include "indexlib/document/normal/SummaryGroupFormatter.h"
#include "indexlib/index/attribute/AttributeIteratorBase.h"
#include "indexlib/index/attribute/AttributeReader.h"
#include "indexlib/index/summary/SummaryMemIndexer.h"
#include "indexlib/index/summary/SummaryMemReaderContainer.h"
#include "indexlib/index/summary/config/SummaryIndexConfig.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, SummaryReader);

Status SummaryReader::Open(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                           const framework::TabletData* tabletData)
{
    AUTIL_LOG(DEBUG, "Begin opening summary");
    std::vector<IndexerInfo> indexers;
    auto segments = tabletData->CreateSlice();
    for (auto it = segments.begin(); it != segments.end(); ++it) {
        const auto& segment = *it;
        auto [status, indexer] = segment->GetIndexer(indexConfig->GetIndexType(), indexConfig->GetIndexName());
        if (!status.IsOK()) {
            auto status = Status::InternalError("no indexer for [%s] in segment [%d]",
                                                indexConfig->GetIndexName().c_str(), segment->GetSegmentId());
            AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
            return status;
        }
        indexers.emplace_back(indexer, segment->GetSegmentId(), segment->GetSegmentStatus(),
                              segment->GetSegmentInfo()->docCount);
    }
    auto status = DoOpen(indexConfig, indexers);
    AUTIL_LOG(DEBUG, "End opening summary");
    return status;
}

Status SummaryReader::DoOpen(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                             const std::vector<IndexerInfo>& indexers)
{
    _summaryIndexConfig = std::dynamic_pointer_cast<config::SummaryIndexConfig>(indexConfig);
    for (summarygroupid_t groupId = 0; groupId < _summaryIndexConfig->GetSummaryGroupConfigCount(); ++groupId) {
        _allGroupIds.push_back(groupId);
    }
    std::vector<IndexerInfo> memIndexers;
    _buildingBaseDocId = 0;
    for (auto& [indexer, segId, segStatus, docCount] : indexers) {
        if (segStatus == framework::Segment::SegmentStatus::ST_BUILT && docCount == 0) {
            continue; // there is no document in current segment, so do nothing
        }
        if (framework::Segment::SegmentStatus::ST_BUILT == segStatus) {
            auto diskIndexer = std::dynamic_pointer_cast<SummaryDiskIndexer>(indexer);
            if (!diskIndexer) {
                AUTIL_LOG(ERROR, "no indexer for index [%s] segment [%d]", _summaryIndexConfig->GetIndexName().c_str(),
                          segId);
                return Status::InternalError("indexer for [%s] in segment [%d] has no OnDiskIndexer",
                                             _summaryIndexConfig->GetIndexName().c_str(), segId);
            }
            _diskIndexers.emplace_back(diskIndexer);
            _segmentDocCount.emplace_back(docCount);
            _segmentIds.emplace_back(segId);
            _buildingBaseDocId += docCount;
        } else if (framework::Segment::SegmentStatus::ST_DUMPING == segStatus ||
                   framework::Segment::SegmentStatus::ST_BUILDING == segStatus) {
            memIndexers.emplace_back(indexer, segId, segStatus, docCount);
            continue;
        }
    }
    auto st = InitBuildingSummaryReader(memIndexers);
    if (!st.IsOK()) {
        AUTIL_LOG(ERROR, "init building attr reader failed, error[%s]", st.ToString().c_str());
        return st;
    }
    _executor = indexlib::util::FutureExecutor::GetInternalExecutor();
    if (!_executor) {
        AUTIL_LOG(DEBUG, "internal executor not created, summary use serial mode");
    }
    return Status::OK();
}

Status SummaryReader::InitBuildingSummaryReader(const std::vector<IndexerInfo>& memIndexers)
{
    docid_t baseDocId = _buildingBaseDocId;
    for (auto& [indexer, segId, segStatus, docCount] : memIndexers) {
        auto memIndexer = std::dynamic_pointer_cast<SummaryMemIndexer>(indexer);
        if (memIndexer) {
            auto memReader = memIndexer->CreateInMemReader();
            assert(memReader);
            _buildingSummaryMemReaders.emplace_back(std::make_pair(baseDocId, memReader));
            baseDocId += docCount;
        } else {
            return Status::InternalError("indexer for [%s] in segment [%d] with no memindexer",
                                         _summaryIndexConfig->GetIndexName().c_str(), segId);
        }
    }
    return Status::OK();
}

void SummaryReader::AddAttrReader(fieldid_t fieldId, AttributeReader* attrReader)
{
    summarygroupid_t groupId = _summaryIndexConfig->FieldIdToSummaryGroupId(fieldId);
    _groupAttributeReaders[groupId].emplace_back(std::make_pair(fieldId, attrReader));
}

void SummaryReader::ClearAttrReaders() { _groupAttributeReaders.clear(); }

std::pair<Status, bool> SummaryReader::GetDocument(docid_t docId, const SummaryGroupIdVec& groupVec,
                                                   indexlib::document::SearchSummaryDocument* summaryDoc) const
{
    auto [status, ret] = GetDocumentFromSummary(docId, groupVec, summaryDoc);
    RETURN2_IF_STATUS_ERROR(status, false, "get document from summary fail.");
    if (ret) {
        ret = GetDocumentFromAttributes(docId, groupVec, summaryDoc);
        return std::make_pair(Status::OK(), ret);
    }
    return std::make_pair(Status::OK(), false);
}

std::pair<Status, bool>
SummaryReader::GetDocumentFromSummary(docid_t docId, const SummaryGroupIdVec& groupVec,
                                      indexlib::document::SearchSummaryDocument* summaryDoc) const
{
    if (docId < 0) {
        return std::make_pair(Status::OK(), false);
    }
    if (!_summaryIndexConfig->NeedStoreSummary()) {
        return std::make_pair(Status::OK(), true);
    }

    if (docId >= _buildingBaseDocId) {
        return GetDocumentFromBuildingSegments(docId, groupVec, summaryDoc);
    }

    docid_t baseDocId = 0;
    for (size_t i = 0; i < _segmentDocCount.size(); i++) {
        if (docId < (baseDocId + (docid_t)_segmentDocCount[i])) {
            return _diskIndexers[i]->GetDocument(docId - baseDocId, groupVec, summaryDoc);
        }
        baseDocId += _segmentDocCount[i];
    }
    return std::make_pair(Status::OK(), false);
}

bool SummaryReader::GetDocumentFromAttributes(docid_t docId, const SummaryGroupIdVec& groupVec,
                                              indexlib::document::SearchSummaryDocument* summaryDoc) const
{
    for (size_t i = 0; i < groupVec.size(); ++i) {
        auto attrReadersIter = _groupAttributeReaders.find(groupVec[i]);
        AttributeReaderInfo curAttrReaderInfo;
        if (attrReadersIter != _groupAttributeReaders.end()) {
            curAttrReaderInfo = attrReadersIter->second;
        } else {
            continue;
        }
        if (!GetDocumentFromAttributes(docId, curAttrReaderInfo, summaryDoc)) {
            AUTIL_LOG(ERROR, "read data from attribute for group [%d] fail", groupVec[i]);
            return false;
        }
    }
    return true;
}

bool SummaryReader::SetSummaryDocField(indexlib::document::SearchSummaryDocument* summaryDoc, fieldid_t fieldId,
                                       const std::string& value) const
{
    assert(summaryDoc);
    int32_t summaryFieldId = _summaryIndexConfig->GetSummaryFieldId(fieldId);
    assert(summaryFieldId != -1);
    return summaryDoc->SetFieldValue(summaryFieldId, value.data(), value.length());
}

bool SummaryReader::GetDocumentFromAttributes(docid_t docId, const AttributeReaderInfo& attrReaders,
                                              indexlib::document::SearchSummaryDocument* summaryDoc) const
{
    autil::mem_pool::Pool* pool = dynamic_cast<autil::mem_pool::Pool*>(summaryDoc->getPool());
    std::string attrValue;
    for (size_t i = 0; i < attrReaders.size(); ++i) {
        if (!(attrReaders[i].second)->Read(docId, attrValue, pool)) {
            return false;
        }
        if (!SetSummaryDocField(summaryDoc, attrReaders[i].first, attrValue)) {
            return false;
        }
    }
    // for (size_t i = 0; i < _packAttrReaders.size(); ++i) {
    //     std::shared_ptr<SummaryConfig> summaryConfig =
    //     _summaryIndexConfig->GetSummaryConfig(_packAttrReaders[i].first); assert(summaryConfig); const std::string&
    //     fieldName = summaryConfig->GetSummaryName(); if (!(_packAttrReaders[i].second)->Read(docId, fieldName,
    //     attrValue, pool)) {
    //         return false;
    //     }
    //     if (!SetSummaryDocField(summaryDoc, _packAttrReaders[i].first, attrValue)) {
    //         return false;
    //     }
    // }
    return true;
}

std::pair<Status, bool>
SummaryReader::GetDocumentFromBuildingSegments(docid_t docId, const SummaryGroupIdVec& groupVec,
                                               indexlib::document::SearchSummaryDocument* summaryDoc) const
{
    for (auto& [baseDocId, memReaderContainer] : _buildingSummaryMemReaders) {
        if (docId < baseDocId) {
            return std::make_pair(Status::OK(), false);
        }
        auto [status, ret] = memReaderContainer->GetDocument(docId - baseDocId, groupVec, summaryDoc);
        RETURN2_IF_STATUS_ERROR(status, false, "get document from building segment fail.");
        if (ret) {
            return std::make_pair(Status::OK(), true);
        }
    }
    return std::make_pair(Status::OK(), false);
}

future_lite::coro::Lazy<indexlib::index::ErrorCodeVec>
SummaryReader::GetDocument(const std::vector<docid_t>& docIds, const SummaryGroupIdVec& groupVec,
                           autil::mem_pool::Pool* sessionPool, indexlib::file_system::ReadOption option,
                           const SummaryReader::SearchSummaryDocVec* docs) const noexcept
{
    if (!sessionPool) {
        AUTIL_LOG(ERROR, "GetDocument fail, sessionPool is nullptr");
        co_return indexlib::index::ErrorCodeVec(docIds.size(), indexlib::index::ErrorCode::BadParameter);
    }

    assert(docIds.size() == docs->size());
    indexlib::index::ErrorCodeVec ec;
    auto executor = co_await future_lite::CurrentExecutor();
    if (_executor) {
        ec = co_await InnerGetDocumentAsync(docIds, groupVec, sessionPool, option, docs).via(_executor);
    } else if (executor) {
        ec = co_await InnerGetDocumentAsync(docIds, groupVec, sessionPool, option, docs);
    } else {
        size_t i = 0;
        for (; i < docIds.size(); ++i) {
            if (option.timeoutTerminator && option.timeoutTerminator->checkRestrictTimeout()) {
                break;
            }
            auto [status, ret] = GetDocument(docIds[i], groupVec, (*docs)[i]);
            if (status.IsOK() && ret) {
                ec.push_back(indexlib::index::ErrorCode::OK);
            } else {
                ec.push_back(indexlib::index::ErrorCode::BadParameter);
            }
        }
        for (; i < docIds.size(); ++i) {
            ec.push_back(indexlib::index::ErrorCode::Timeout);
        }
    }
    co_return ec;
}

future_lite::coro::Lazy<indexlib::index::ErrorCodeVec>
SummaryReader::InnerGetDocumentAsync(const std::vector<docid_t>& docIds, const SummaryGroupIdVec& groupVec,
                                     autil::mem_pool::Pool* sessionPool, indexlib::file_system::ReadOption option,
                                     const SummaryReader::SearchSummaryDocVec* docs) const noexcept
{
    if (docIds.empty()) {
        co_return indexlib::index::ErrorCodeVec();
    }
    if (std::is_sorted(docIds.begin(), docIds.end()) && docIds[0] >= 0) {
        co_return co_await InnerGetDocumentAsyncOrdered(docIds, groupVec, sessionPool, option, docs);
    }
    indexlib::index::ErrorCodeVec result(docIds.size());
    std::vector<size_t> sortIndex(docIds.size());
    size_t validCnt = 0;
    for (size_t i = 0; i < docIds.size(); ++i) {
        if (docIds[i] < 0) {
            result[i] = indexlib::index::ErrorCode::BadParameter;
        } else {
            sortIndex[validCnt++] = i;
        }
    }
    sortIndex.resize(validCnt);
    std::sort(sortIndex.begin(), sortIndex.end(),
              [&docIds](size_t lhs, size_t rhs) { return docIds[lhs] < docIds[rhs]; });
    std::vector<docid_t> orderedDocIds(validCnt);
    std::vector<indexlib::document::SearchSummaryDocument*> orderedDocs(validCnt);
    for (size_t i = 0; i < validCnt; ++i) {
        size_t idx = sortIndex[i];
        orderedDocIds[i] = docIds[idx];
        orderedDocs[i] = (*docs)[idx];
    }
    auto orderedResult =
        co_await InnerGetDocumentAsyncOrdered(orderedDocIds, groupVec, sessionPool, option, &orderedDocs);
    for (size_t i = 0; i < validCnt; ++i) {
        result[sortIndex[i]] = orderedResult[i];
    }
    co_return result;
}

future_lite::coro::Lazy<std::vector<future_lite::Try<indexlib::index::ErrorCodeVec>>>
SummaryReader::GetBuiltSegmentTasks(const std::vector<docid_t>& docIds, const SummaryGroupIdVec& groupVec,
                                    autil::mem_pool::Pool* sessionPool, indexlib::file_system::ReadOption readOption,
                                    const SummaryReader::SearchSummaryDocVec* docs) const noexcept
{
    std::vector<future_lite::coro::Lazy<std::vector<indexlib::index::ErrorCode>>> segmentTasks;

    // get value from built segment async
    docid_t currentSegDocIdEnd = 0;
    size_t docIdIdx = 0;

    std::deque<std::vector<docid_t>> segmentDocIds;
    std::deque<SummaryReader::SearchSummaryDocVec> segmentDocs;
    for (uint32_t i = 0; i < _segmentDocCount.size() && docIdIdx < docIds.size(); ++i) {
        std::vector<docid_t> currentSegmentDocId;
        SummaryReader::SearchSummaryDocVec currentSegmentDocs;
        docid_t baseDocId = currentSegDocIdEnd;
        currentSegDocIdEnd += _segmentDocCount[i];
        while (docIdIdx < docIds.size() && docIds[docIdIdx] < currentSegDocIdEnd) {
            currentSegmentDocId.push_back(docIds[docIdIdx] - baseDocId);
            currentSegmentDocs.push_back((*docs)[docIdIdx]);
            docIdIdx++;
        }
        if (!currentSegmentDocId.empty()) {
            size_t idx = segmentDocIds.size();
            segmentDocIds.push_back(std::move(currentSegmentDocId));
            segmentDocs.push_back(std::move(currentSegmentDocs));
            segmentTasks.push_back(_diskIndexers[i]->GetDocument(segmentDocIds[idx], groupVec, sessionPool, readOption,
                                                                 &segmentDocs[idx]));
        }
    }
    co_return co_await future_lite::coro::collectAll(std::move(segmentTasks));
}

future_lite::coro::Lazy<indexlib::index::ErrorCodeVec> SummaryReader::InnerGetDocumentAsyncOrdered(
    const std::vector<docid_t>& docIds, const SummaryGroupIdVec& groupVec, autil::mem_pool::Pool* sessionPool,
    indexlib::file_system::ReadOption readOption, const SummaryReader::SearchSummaryDocVec* docs) const noexcept
{
    if (readOption.timeoutTerminator && readOption.timeoutTerminator->checkRestrictTimeout()) {
        co_return std::vector<indexlib::index::ErrorCode>(docIds.size(), indexlib::index::ErrorCode::Timeout);
    }

    std::vector<indexlib::index::ErrorCode> result(docIds.size(), indexlib::index::ErrorCode::OK);
    std::vector<future_lite::coro::Lazy<std::vector<indexlib::index::ErrorCode>>> subTasks;
    subTasks.push_back(GetDocumentFromSummaryAsync(docIds, groupVec, sessionPool, readOption, docs));

    // TODO: support pack attribute
    std::vector<AttributeIteratorBase*> attributeIterators;
    std::map<summarygroupid_t, std::vector<std::vector<std::string>>> groupAttributeValues;

    for (size_t groupIndex = 0; groupIndex < groupVec.size(); ++groupIndex) {
        auto attrReadersIter = _groupAttributeReaders.find(groupVec[groupIndex]);
        if (attrReadersIter == _groupAttributeReaders.end()) {
            continue;
        }
        AttributeReaderInfo attrReaders = attrReadersIter->second;
        std::vector<std::vector<std::string>> attributeValues(attrReaders.size());
        groupAttributeValues[groupVec[groupIndex]] = attributeValues;
    }

    for (auto& [groupId, attributeValues] : groupAttributeValues) {
        auto attrReadersIter = _groupAttributeReaders.find(groupId);
        assert(attrReadersIter != _groupAttributeReaders.end());
        AttributeReaderInfo attrReaders = attrReadersIter->second;
        for (size_t i = 0; i < attrReaders.size(); ++i) {
            attributeValues[i].resize(docIds.size());
            auto iter = attrReaders[i].second->CreateIterator(sessionPool);
            attributeIterators.push_back(iter);
            subTasks.push_back(iter->BatchSeek(docIds, readOption, &attributeValues[i]));
        }
    }

    auto taskResult = co_await future_lite::coro::collectAll(std::move(subTasks));

    for (size_t docIdx = 0; docIdx < docIds.size(); ++docIdx) {
        assert(!taskResult[0].hasError());
        std::vector<indexlib::index::ErrorCode> summaryEcs = taskResult[0].value();
        if (summaryEcs[docIdx] != indexlib::index::ErrorCode::OK) {
            result[docIdx] = summaryEcs[docIdx];
        }
    }
    size_t fieldIdx = 1;

#define FILL_ATTRIBUTE_HELPER(readers, values)                                                                         \
    do {                                                                                                               \
        for (size_t i = 0; i < readers.size(); ++i, ++fieldIdx) {                                                      \
            assert(!taskResult[fieldIdx].hasError());                                                                  \
            auto& attributeEc = taskResult[fieldIdx].value();                                                          \
            for (size_t docIdx = 0; docIdx < docIds.size(); ++docIdx) {                                                \
                if (result[docIdx] != indexlib::index::ErrorCode::OK) {                                                \
                    continue;                                                                                          \
                }                                                                                                      \
                                                                                                                       \
                if (attributeEc[docIdx] != indexlib::index::ErrorCode::OK) {                                           \
                    result[docIdx] = attributeEc[docIdx];                                                              \
                } else if (!SetSummaryDocField((*docs)[docIdx], readers[i].first, values[i][docIdx])) {                \
                    result[docIdx] = indexlib::index::ErrorCode::Runtime;                                              \
                }                                                                                                      \
            }                                                                                                          \
        }                                                                                                              \
    } while (0);

    for (size_t groupIndex = 0; groupIndex < groupVec.size(); ++groupIndex) {
        auto attrReadersIter = _groupAttributeReaders.find(groupVec[groupIndex]);
        AttributeReaderInfo attrReaders;
        if (attrReadersIter != _groupAttributeReaders.end()) {
            attrReaders = attrReadersIter->second;
        } else {
            continue;
        }
        FILL_ATTRIBUTE_HELPER(attrReaders, groupAttributeValues[groupVec[groupIndex]]);
    }
#undef FILL_ATTRIBUTE_HELPER
    for (auto iter : attributeIterators) {
        indexlib::IE_POOL_COMPATIBLE_DELETE_CLASS(sessionPool, iter);
    }
    co_return result;
}

future_lite::coro::Lazy<indexlib::index::ErrorCodeVec> SummaryReader::GetDocumentFromSummaryAsync(
    const std::vector<docid_t>& docIds, const SummaryGroupIdVec& groupVec, autil::mem_pool::Pool* sessionPool,
    indexlib::file_system::ReadOption readOption, const SearchSummaryDocVec* docs) const noexcept
{
    if (readOption.timeoutTerminator && readOption.timeoutTerminator->checkRestrictTimeout()) {
        co_return std::vector<indexlib::index::ErrorCode>(docIds.size(), indexlib::index::ErrorCode::Timeout);
    }

    std::vector<indexlib::index::ErrorCode> ret(docIds.size(), indexlib::index::ErrorCode::OK);
    // fill result for built segment
    auto segmentResults = co_await GetBuiltSegmentTasks(docIds, groupVec, sessionPool, readOption, docs);
    size_t docIdx = 0;
    for (size_t i = 0; i < segmentResults.size(); ++i) {
        assert(!segmentResults[i].hasError());
        auto segmentResult = segmentResults[i].value();
        for (auto& ec : segmentResult) {
            ret[docIdx++] = ec;
        }
    }

    // fill result for building segment
    while (docIdx < docIds.size()) {
        docid_t docId = docIds[docIdx];
        assert(docId >= _buildingBaseDocId);
        auto [status, curRet] = GetDocumentFromBuildingSegments(docId, groupVec, (*docs)[docIdx]);
        if (!status.IsOK() || !curRet) {
            ret[docIdx] = indexlib::index::ErrorCode::BadParameter;
        }
        docIdx++;
    }
    co_return ret;
}

} // namespace indexlibv2::index
