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
#include "indexlib/index/source/SourceReader.h"

#include "indexlib/document/normal/SerializedSourceDocument.h"
#include "indexlib/document/normal/SourceDocument.h"
#include "indexlib/document/normal/SourceFormatter.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/index/source/Common.h"
#include "indexlib/index/source/SourceDiskIndexer.h"
#include "indexlib/index/source/SourceMemIndexer.h"
#include "indexlib/index/source/config/SourceGroupConfig.h"
#include "indexlib/index/source/config/SourceIndexConfig.h"

namespace indexlibv2 { namespace index {
AUTIL_LOG_SETUP(index, SourceReader);

SourceReader::SourceReader() {}

SourceReader::~SourceReader() {}

Status SourceReader::Open(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                          const framework::TabletData* tabletData)
{
    auto sourceIndexConfig = std::dynamic_pointer_cast<indexlibv2::config::SourceIndexConfig>(indexConfig);
    if (!sourceIndexConfig) {
        RETURN_STATUS_ERROR(Corruption, "open failed, failed to cast index config to source index config");
    }
    _sourceConfig = sourceIndexConfig;
    auto groupConfigs = sourceIndexConfig->GetGroupConfigs();
    for (const auto& groupConfig : groupConfigs) {
        if (groupConfig) {
            _allGroupIds.push_back(groupConfig->GetGroupId());
        }
    }
    docid_t baseDocId = 0;
    AUTIL_LOG(DEBUG, "Begin opening source");
    for (const auto& segment : tabletData->CreateSlice()) {
        size_t segmentDocCount = segment->GetDocCount();
        auto segmentId = segment->GetSegmentId();
        if (0 == segmentDocCount && segment->GetSegmentStatus() != framework::Segment::SegmentStatus::ST_BUILDING) {
            AUTIL_LOG(INFO, "ignore segment [%d] for doc count is [0]", segmentId);
            continue;
        }
        _segmentDocCount.push_back(segmentDocCount);
        auto [st, indexer] = segment->GetIndexer(SOURCE_INDEX_TYPE_STR, SOURCE_INDEX_NAME);
        RETURN_IF_STATUS_ERROR(st, "get source indexer failed");
        if (segment->GetSegmentStatus() == framework::Segment::SegmentStatus::ST_BUILT) {
            auto typedIndexer = std::dynamic_pointer_cast<SourceDiskIndexer>(indexer);
            if (!typedIndexer) {
                RETURN_STATUS_ERROR(Corruption, "cast disk indexer failed");
            }
            _diskIndexers.push_back(typedIndexer);
            _buildingBaseDocId += segmentDocCount;
        } else {
            auto typedIndexer = std::dynamic_pointer_cast<SourceMemIndexer>(indexer);
            if (!typedIndexer) {
                RETURN_STATUS_ERROR(Corruption, "cast to mem indexer failed");
            }
            _memIndexers.push_back(typedIndexer);
            _inMemSegmentBaseDocId.push_back(baseDocId);
        }
        baseDocId += segmentDocCount;
    }
    AUTIL_LOG(DEBUG, "End opening source");
    return Status::OK();
}

future_lite::coro::Lazy<indexlib::index::ErrorCodeVec>
SourceReader::GetDocumentAsync(const std::vector<docid_t>& docIds,
                               const std::vector<index::sourcegroupid_t>& requiredGroupdIds,
                               autil::mem_pool::PoolBase* sessionPool, indexlib::file_system::ReadOption option,
                               const std::vector<indexlib::document::SourceDocument*>* sourceDocs) const
{
    std::vector<indexlib::document::SerializedSourceDocument*> serDocs;
    std::vector<indexlib::document::SerializedSourceDocument> serDocsObjs;
    serDocsObjs.resize(docIds.size());
    serDocs.reserve(docIds.size());
    for (size_t i = 0; i < serDocsObjs.size(); ++i) {
        serDocs.push_back(&serDocsObjs[i]);
    }
    auto ret = co_await GetDocumentAsync(docIds, requiredGroupdIds, sessionPool, option, &serDocs);
    for (size_t i = 0; i < docIds.size(); ++i) {
        if (ret[i] != indexlib::index::ErrorCode::OK) {
            continue;
        }
        indexlibv2::document::SourceFormatter formatter;
        formatter.Init(_sourceConfig);
        if (!formatter.DeserializeSourceDocument(serDocs[i], (*sourceDocs)[i]).IsOK()) {
            ret[i] = indexlib::index::ErrorCode::Runtime;
        }
    }
    co_return ret;
}

future_lite::coro::Lazy<indexlib::index::ErrorCodeVec>
SourceReader::GetDocumentAsync(const std::vector<docid_t>& docIds,
                               const std::vector<index::sourcegroupid_t>& requiredGroupdIds,
                               autil::mem_pool::PoolBase* sessionPool, indexlib::file_system::ReadOption option,
                               const std::vector<indexlib::document::SerializedSourceDocument*>* sourceDocs) const
{
    assert(std::is_sorted(docIds.begin(), docIds.end()));
    indexlib::index::ErrorCodeVec ecVec;
    ecVec.reserve(docIds.size());
    // ecVec.assign(docIds.size(), indexlib::index::ErrorCode::OK);
    std::vector<future_lite::coro::Lazy<indexlib::index::ErrorCodeVec>> subTasks;
    size_t docidIdx = 0;
    // get from disk indexer
    docid_t currentSegmentEndDocId = 0;

    using SourceDocVec = std::vector<indexlib::document::SerializedSourceDocument*>;
    std::deque<std::vector<docid_t>> segmentDocIds;
    std::deque<SourceDocVec> segmentDocs;

    for (size_t i = 0; i < _diskIndexers.size(); ++i) {
        docid_t baseDocId = currentSegmentEndDocId;
        currentSegmentEndDocId += _segmentDocCount[i];
        std::vector<docid_t> currentSegmentDocIds;
        SourceDocVec currentSegmentDocs;
        while (docidIdx < docIds.size() && docIds[docidIdx] < currentSegmentEndDocId) {
            currentSegmentDocIds.push_back(docIds[docidIdx] - baseDocId);
            currentSegmentDocs.push_back(sourceDocs->at(docidIdx));
            docidIdx++;
        }
        if (!currentSegmentDocIds.empty()) {
            segmentDocIds.push_back(std::move(currentSegmentDocIds));
            segmentDocs.push_back(std::move(currentSegmentDocs));
            subTasks.push_back(_diskIndexers[i]->GetDocument(segmentDocIds.back(), requiredGroupdIds, sessionPool,
                                                             option, &segmentDocs.back()));
        }
    }
    auto result = co_await future_lite::coro::collectAll(std::move(subTasks));
    for (auto& segmentEcs : result) {
        assert(!segmentEcs.hasError());
        for (auto& ec : segmentEcs.value()) {
            ecVec.emplace_back(ec);
        }
    }

    // get from mem indexer
    while (docidIdx < docIds.size()) {
        auto sourceDocument = sourceDocs->at(docidIdx);
        auto docId = docIds[docidIdx++];
        auto ec = indexlib::index::ErrorCode::BadParameter;
        for (size_t i = 0; i < _memIndexers.size(); i++) {
            docid_t curBaseDocId = _inMemSegmentBaseDocId[i];
            assert(docId >= curBaseDocId);
            if (_memIndexers[i]->GetDocument(docId - curBaseDocId, requiredGroupdIds, sourceDocument)) {
                ec = indexlib::index::ErrorCode::OK;
                break;
            }
        }
        ecVec.emplace_back(ec);
    }
    co_return ecVec;
}

Status SourceReader::GetDocument(docid_t docId, indexlib::document::SourceDocument* sourceDocument) const
{
    return GetDocument(docId, _allGroupIds, sourceDocument);
}

Status SourceReader::GetDocument(docid_t docId, const std::vector<index::sourcegroupid_t>& requiredGroupdIds,
                                 indexlib::document::SourceDocument* sourceDocument) const
{
    std::vector<indexlib::document::SourceDocument*> sourceDocs = {sourceDocument};
    auto ecVec = future_lite::coro::syncAwait(GetDocumentAsync({docId}, requiredGroupdIds, sourceDocument->GetPool(),
                                                               indexlib::file_system::ReadOption(), &sourceDocs));
    assert(ecVec.size() == 1);
    if (ecVec[0] != indexlib::index::ErrorCode::OK) {
        RETURN_STATUS_ERROR(InvalidArgs, "doc [%d] is invalid", docId);
    }
    return Status::OK();
}

}} // namespace indexlibv2::index
