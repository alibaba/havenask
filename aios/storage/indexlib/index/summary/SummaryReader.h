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
#include "indexlib/base/Status.h"
#include "indexlib/file_system/file/ReadOption.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/index/IIndexReader.h"
#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/index/primary_key/PrimaryKeyReader.h"
#include "indexlib/index/summary/SummaryDiskIndexer.h"

namespace indexlib { namespace document {
class SearchSummaryDocument;
}} // namespace indexlib::document

namespace indexlibv2 { namespace config {
class SummaryIndexConfig;
class IIndexConfig;
}} // namespace indexlibv2::config
// namespace indexlibv2::config

namespace indexlibv2::index {
class AttributeReader;
class PackAttributeReader;
class SummaryMemReaderContainer;

class SummaryReader : public IIndexReader
{
public:
    SummaryReader() : _executor(nullptr) {};
    virtual ~SummaryReader() = default;

    // indexer, segId, segStatus, docCount
    using IndexerInfo = std::tuple<std::shared_ptr<IIndexer>, segmentid_t, framework::Segment::SegmentStatus, size_t>;
    using SummaryGroupIdVec = SummaryDiskIndexer::SummaryGroupIdVec;
    using SearchSummaryDocVec = SummaryDiskIndexer::SearchSummaryDocVec;
    using AttributeReaderInfo = std::vector<std::pair<fieldid_t, AttributeReader*>>;
    using GroupAttributeReaderInfo = std::map<summarygroupid_t, AttributeReaderInfo>;
    using PackAttributeReaderInfo = std::vector<std::tuple<fieldid_t, std::string, PackAttributeReader*>>;
    using GroupPackAttributeReaderInfo = std::map<summarygroupid_t, PackAttributeReaderInfo>;

public:
    Status Open(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                const framework::TabletData* tabletData) override;

public:
    future_lite::coro::Lazy<indexlib::index::ErrorCodeVec> GetDocument(const std::vector<docid_t>& docIds,
                                                                       autil::mem_pool::Pool* sessionPool,
                                                                       indexlib::file_system::ReadOption option,
                                                                       const SearchSummaryDocVec* docs) const noexcept
    {
        return GetDocument(docIds, _allGroupIds, sessionPool, option, docs);
    }
    std::pair<Status, bool> GetDocument(docid_t docId, indexlib::document::SearchSummaryDocument* summaryDoc) const
    {
        return GetDocument(docId, _allGroupIds, summaryDoc);
    }
    future_lite::coro::Lazy<indexlib::index::ErrorCodeVec> GetDocument(const std::vector<docid_t>& docIds,
                                                                       const SummaryGroupIdVec& groupVec,
                                                                       autil::mem_pool::Pool* sessionPool,
                                                                       indexlib::file_system::ReadOption option,
                                                                       const SearchSummaryDocVec* docs) const noexcept;
    std::pair<Status, bool> GetDocument(docid_t docId, const SummaryGroupIdVec& groupVec,
                                        indexlib::document::SearchSummaryDocument* summaryDoc) const;

public:
    static std::string Identifier() { return std::string("summary.reader.group"); }
    std::string GetIdentifier() const { return Identifier(); }

    void AddAttrReader(fieldid_t fieldId, AttributeReader* attrReader);
    void AddPackAttrReader(fieldid_t fieldId, PackAttributeReader* packAttrReader);
    void ClearAttrReaders();
    void SetPrimaryKeyReader(indexlib::index::PrimaryKeyIndexReader* pkIndexReader) { _pkIndexReader = pkIndexReader; }

public:
    std::pair<Status, bool> GetDocumentByPkStr(const std::string& pkStr,
                                               indexlib::document::SearchSummaryDocument* summaryDoc) const;
    std::pair<Status, bool> GetDocumentByPkStr(const std::string& pkStr, const SummaryGroupIdVec& groupVec,
                                               indexlib::document::SearchSummaryDocument* summaryDoc) const;
    template <typename Key>
    std::pair<Status, bool> GetDocumentByPkHash(const Key& key, indexlib::document::SearchSummaryDocument* summaryDoc);

private:
    std::pair<Status, bool> GetDocumentFromSummary(docid_t docId, const SummaryGroupIdVec& groupVec,
                                                   indexlib::document::SearchSummaryDocument* summaryDoc) const;
    bool GetDocumentFromAttributes(docid_t docId, const SummaryGroupIdVec& groupVec,
                                   indexlib::document::SearchSummaryDocument* summaryDoc) const;
    bool GetDocumentFromAttributes(docid_t docId, const AttributeReaderInfo& attrReaders,
                                   indexlib::document::SearchSummaryDocument* summaryDoc) const;
    bool GetDocumentFromPackAttributes(docid_t docId, const PackAttributeReaderInfo& packAttrReaders,
                                       indexlib::document::SearchSummaryDocument* summaryDoc) const;
    bool SetSummaryDocField(indexlib::document::SearchSummaryDocument* summaryDoc, fieldid_t fieldId,
                            const std::string& value) const;

private:
    Status AssertPkIndexExist() const;
    Status DoOpen(const std::shared_ptr<config::IIndexConfig>& indexConfig, const std::vector<IndexerInfo>& indexers);
    Status InitBuildingSummaryReader(const std::vector<IndexerInfo>& memIndexers);
    std::pair<Status, bool>
    GetDocumentFromBuildingSegments(docid_t docId, const SummaryGroupIdVec& groupVec,
                                    indexlib::document::SearchSummaryDocument* summaryDoc) const;

private:
    future_lite::coro::Lazy<indexlib::index::ErrorCodeVec>
    InnerGetDocumentAsync(const std::vector<docid_t>& docIds, const SummaryGroupIdVec& groupVec,
                          autil::mem_pool::Pool* sessionPool, indexlib::file_system::ReadOption option,
                          const SearchSummaryDocVec* docs) const noexcept;
    future_lite::coro::Lazy<indexlib::index::ErrorCodeVec>
    InnerGetDocumentAsyncOrdered(const std::vector<docid_t>& docIds, const SummaryGroupIdVec& groupVec,
                                 autil::mem_pool::Pool* sessionPool, indexlib::file_system::ReadOption option,
                                 const SearchSummaryDocVec* docs) const noexcept;
    future_lite::coro::Lazy<std::vector<future_lite::Try<indexlib::index::ErrorCodeVec>>>
    GetBuiltSegmentTasks(const std::vector<docid_t>& docIds, const SummaryGroupIdVec& groupVec,
                         autil::mem_pool::Pool* sessionPool, indexlib::file_system::ReadOption readOption,
                         const SearchSummaryDocVec* docs) const noexcept;
    future_lite::coro::Lazy<indexlib::index::ErrorCodeVec>
    GetDocumentFromSummaryAsync(const std::vector<docid_t>& docIds, const SummaryGroupIdVec& groupVec,
                                autil::mem_pool::Pool* sessionPool, indexlib::file_system::ReadOption readOption,
                                const SearchSummaryDocVec* docs) const noexcept;

private:
    std::shared_ptr<config::SummaryIndexConfig> _summaryIndexConfig;
    indexlib::index::PrimaryKeyIndexReader* _pkIndexReader;
    std::vector<std::pair<docid_t, std::shared_ptr<SummaryMemReaderContainer>>> _buildingSummaryMemReaders;
    docid_t _buildingBaseDocId;
    std::vector<uint64_t> _segmentDocCount;
    std::vector<segmentid_t> _segmentIds;
    std::vector<std::shared_ptr<SummaryDiskIndexer>> _diskIndexers;
    GroupAttributeReaderInfo _groupAttributeReaders;
    GroupPackAttributeReaderInfo _groupPackAttributeReaders;
    SummaryGroupIdVec _allGroupIds;
    future_lite::Executor* _executor;

private:
    AUTIL_LOG_DECLARE();
};

////////////////////////////////////////////////////////////////

inline Status SummaryReader::AssertPkIndexExist() const
{
    if (unlikely(!_pkIndexReader)) {
        return Status::Corruption("primary key index may not be configured");
    }
    return Status::OK();
}

inline std::pair<Status, bool>
SummaryReader::GetDocumentByPkStr(const std::string& pkStr, indexlib::document::SearchSummaryDocument* summaryDoc) const
{
    RETURN2_IF_STATUS_ERROR(AssertPkIndexExist(), false, "assert primary key index fail");
    docid_t docId = _pkIndexReader->Lookup(pkStr);
    return GetDocument(docId, summaryDoc);
}

inline std::pair<Status, bool>
SummaryReader::GetDocumentByPkStr(const std::string& pkStr, const SummaryGroupIdVec& groupVec,
                                  indexlib::document::SearchSummaryDocument* summaryDoc) const
{
    RETURN2_IF_STATUS_ERROR(AssertPkIndexExist(), false, "assert primary key index fail");
    docid_t docId = _pkIndexReader->Lookup(pkStr);
    return GetDocument(docId, groupVec, summaryDoc);
}

template <typename Key>
inline std::pair<Status, bool> SummaryReader::GetDocumentByPkHash(const Key& key,
                                                                  indexlib::document::SearchSummaryDocument* summaryDoc)
{
    RETURN2_IF_STATUS_ERROR(AssertPkIndexExist(), false, "assert primary key index fail");
    auto pkReader = dynamic_cast<indexlibv2::index::PrimaryKeyReader<Key>*>(_pkIndexReader);
    if (pkReader == nullptr) {
        return std::make_pair(Status::Corruption("hash key type [%s], indexName[%s]", typeid(key).name(),
                                                 _pkIndexReader->GetIndexName().c_str()),
                              false);
    }

    docid_t docId = pkReader->Lookup(key);

    if (docId == INVALID_DOCID) {
        return std::make_pair(Status::OK(), false);
    }
    return GetDocument(docId, summaryDoc);
}

} // namespace indexlibv2::index
