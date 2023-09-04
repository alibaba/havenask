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
#include "ha3/search/IndexPartitionReaderWrapper.h"

#include <cstddef>
#include <utility>

#include "alog/Logger.h"
#include "autil/Log.h"
#include "autil/mem_pool/PoolBase.h"
#include "build_service/analyzer/Token.h"
#include "ha3/common/NumberTerm.h"
#include "ha3/common/Term.h"
#include "ha3/isearch.h"
#include "ha3/search/AithetaFilter.h"
#include "ha3/search/AttributeMetaInfo.h"
#include "ha3/search/FilterWrapper.h"
#include "ha3/search/LayerMetas.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/index/common/NumberTerm.h"
#include "indexlib/index/common/Term.h"
#include "indexlib/index/inverted_index/InvertedIndexReader.h"
#include "indexlib/index/inverted_index/MultiFieldIndexReader.h"
#include "indexlib/index/inverted_index/PostingIterator.h"
#include "indexlib/index/inverted_index/format/TermMeta.h"
#include "indexlib/index/normal/attribute/accessor/attribute_iterator_base.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader_adaptor.h"
#include "indexlib/index/normal/inverted_index/accessor/multi_field_index_reader.h"
#include "indexlib/index/normal/primarykey/legacy_primary_key_reader.h"
#include "indexlib/index/normal/summary/TabletSummaryReader.h"
#include "indexlib/index/partition_info.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_define.h"
#include "indexlib/partition/index_partition_reader.h"
#include "indexlib/partition/partition_reader_snapshot.h"
#include "indexlib/table/normal_table/NormalTabletSessionReader.h"

using namespace indexlib::index;
using namespace indexlib::index_base;
using namespace indexlib::partition;
using namespace isearch::common;
using namespace std;
namespace isearch {
namespace search {
AUTIL_LOG_SETUP(ha3, IndexPartitionReaderWrapper);

std::shared_ptr<indexlib::index::SummaryReader>
    IndexPartitionReaderWrapper::RET_EMPTY_SUMMARY_READER
    = std::shared_ptr<indexlib::index::SummaryReader>();
std::shared_ptr<indexlib::index::PrimaryKeyIndexReader>
    IndexPartitionReaderWrapper::RET_EMPTY_PRIMARY_KEY_READER
    = std::shared_ptr<indexlib::index::PrimaryKeyIndexReader>();
std::shared_ptr<indexlib::index::InvertedIndexReader>
    IndexPartitionReaderWrapper::RET_EMPTY_INDEX_READER
    = std::shared_ptr<indexlib::index::InvertedIndexReader>();
std::shared_ptr<indexlib::index::KKVReader> IndexPartitionReaderWrapper::RET_EMPTY_KKV_READER
    = std::shared_ptr<indexlib::index::KKVReader>();
std::shared_ptr<indexlib::index::KVReader> IndexPartitionReaderWrapper::RET_EMPTY_KV_READER
    = std::shared_ptr<indexlib::index::KVReader>();
std::shared_ptr<indexlib::index::AttributeReader> IndexPartitionReaderWrapper::RET_EMPTY_ATTR_READER
    = std::shared_ptr<indexlib::index::AttributeReader>();

IndexPartitionReaderWrapper::IndexPartitionReaderWrapper(
    const map<string, uint32_t> *indexName2IdMap,
    const map<string, uint32_t> *attrName2IdMap,
    const vector<IndexPartitionReaderPtr> *indexReaderVec,
    bool ownPointer)
    : _ownPointer(ownPointer)
    , _indexName2IdMap(indexName2IdMap)
    , _attrName2IdMap(attrName2IdMap)
    , _indexReaderVec(indexReaderVec)
    , _tracerCursor(0)
    , _topK(0)
    , _sessionPool(NULL)
    , _main2SubIt(NULL)
    , _sub2MainIt(NULL) {
    assert(indexReaderVec->size() > 0);
    auto partitionInfoPtr
        = (*_indexReaderVec)[IndexPartitionReader::MAIN_PART_ID]->GetPartitionInfo();

    if (indexReaderVec->size() > IndexPartitionReader::SUB_PART_ID
        && (*indexReaderVec)[IndexPartitionReader::SUB_PART_ID]) {
        _subPartitionInfoPtr = partitionInfoPtr->GetSubPartitionInfo();
    }
    _partitionInfoWrapperPtr = std::make_shared<PartitionInfoWrapper>(partitionInfoPtr);
}

IndexPartitionReaderWrapper::IndexPartitionReaderWrapper(
    const std::shared_ptr<indexlibv2::framework::ITabletReader> &tabletReader)
    : _ownPointer(false)
    , _indexName2IdMap(nullptr)
    , _attrName2IdMap(nullptr)
    , _indexReaderVec(nullptr)
    , _tabletReader(tabletReader)
    , _tracerCursor(0)
    , _topK(0)
    , _sessionPool(nullptr)
    , _main2SubIt(nullptr)
    , _sub2MainIt(nullptr) {
    assert(tabletReader);
    auto normalTabletReader
        = std::dynamic_pointer_cast<indexlibv2::table::NormalTabletSessionReader>(_tabletReader);
    if (normalTabletReader) {
        _partitionInfoWrapperPtr
            = std::make_shared<PartitionInfoWrapper>(normalTabletReader->GetNormalTabletInfo());
    }
}

IndexPartitionReaderWrapper::~IndexPartitionReaderWrapper() {
    clearObject();
    if (_ownPointer) {
        DELETE_AND_SET_NULL(_indexName2IdMap);
        DELETE_AND_SET_NULL(_attrName2IdMap);
        DELETE_AND_SET_NULL(_indexReaderVec);
    }
}

void IndexPartitionReaderWrapper::addDelPosting(const std::string &indexName,
                                                PostingIterator *postIter) {
    _delPostingVec.emplace_back(indexName, postIter);
}

LookupResult IndexPartitionReaderWrapper::lookup(const common::Term &term,
                                                 const LayerMeta *layerMeta) {
    LookupResult result;
    LookupResult originalResult;
    string key;
    getTermKeyStr(term, layerMeta, key);
    if (tryGetFromPostingCache(key, originalResult)) {
        result = originalResult;
        if (originalResult.postingIt != NULL) {
            PostingIterator *clonedPosting = originalResult.postingIt->Clone();
            _delPostingVec.emplace_back(term.getIndexName(), clonedPosting);
            result.postingIt = clonedPosting;
        }
        return result;
    }
    return doLookup(term, key, layerMeta);
}

LookupResult IndexPartitionReaderWrapper::doLookup(const common::Term &term,
                                                   const string &key,
                                                   const LayerMeta *layerMeta) {
    LookupResult result;
    std::shared_ptr<InvertedIndexReader> indexReaderPtr;
    bool isSubIndex = false;
    const auto &indexName = term.getIndexName();
    if (!getIndexReader(indexName, indexReaderPtr, isSubIndex)) {
        AUTIL_LOG(WARN,
                  "getIndexReader failed for term: %s:%s",
                  indexName.c_str(),
                  term.getWord().c_str());
        return result;
    }

    auto indexType = getIndexType(indexReaderPtr, indexName);
    indexlib::index::Term *indexTerm = createIndexTerm(term, indexType, isSubIndex);
    unique_ptr<indexlib::index::Term> indexTermPtr(indexTerm);
    PostingType pt1;
    PostingType pt2;
    truncateRewrite(term.getTruncateName(), *indexTermPtr, pt1, pt2);
    result = doLookupWithoutCache(indexReaderPtr, isSubIndex, *indexTermPtr, pt1, pt2, layerMeta);
    _postingCache[key] = result;
    return result;
}

InvertedIndexType
IndexPartitionReaderWrapper::getIndexType(std::shared_ptr<InvertedIndexReader> indexReaderPtr,
                                          const std::string &indexName) {
    auto legacyMultiIndexReader
        = DYNAMIC_POINTER_CAST(indexlib::index::legacy::MultiFieldIndexReader, indexReaderPtr);
    if (legacyMultiIndexReader) {
        indexReaderPtr = legacyMultiIndexReader->GetInvertedIndexReader(indexName);
    } else {
        auto multiIndexReader
            = DYNAMIC_POINTER_CAST(indexlib::index::MultiFieldIndexReader, indexReaderPtr);
        if (multiIndexReader) {
            indexReaderPtr = multiIndexReader->GetIndexReader(indexName);
        }
    }

    return indexReaderPtr->GetInvertedIndexType();
}

void IndexPartitionReaderWrapper::truncateRewrite(const std::string &truncateName,
                                                  indexlib::index::Term &indexTerm,
                                                  PostingType &pt1,
                                                  PostingType &pt2) {
    if (BITMAP_TRUNCATE_INDEX_NAME == truncateName) {
        pt1 = pt_bitmap;
        pt2 = pt_normal;
    } else {
        indexTerm.SetTruncateName(truncateName);
        pt1 = pt_normal;
        pt2 = pt_bitmap;
    }
}

LookupResult IndexPartitionReaderWrapper::doLookupWithoutCache(
    const std::shared_ptr<InvertedIndexReader> &indexReaderPtr,
    bool isSubIndex,
    const indexlib::index::Term &indexTerm,
    PostingType pt1,
    PostingType pt2,
    const LayerMeta *layerMeta) {
    PostingIterator *iter
        = doLookupByRanges(indexReaderPtr, &indexTerm, pt1, pt2, layerMeta, isSubIndex);
    return makeLookupResult(isSubIndex, iter, indexTerm.GetIndexName());
}

LookupResult IndexPartitionReaderWrapper::makeLookupResult(bool isSubIndex,
                                                           PostingIterator *postIter,
                                                           const std::string &indexName) {
    LookupResult result;
    _delPostingVec.emplace_back(indexName, postIter);
    result.postingIt = postIter;
    if (isSubIndex && result.postingIt != NULL) {
        result.main2SubIt = _main2SubIt;
        result.sub2MainIt = _sub2MainIt;
        result.isSubPartition = true;
    }
    return result;
}

void IndexPartitionReaderWrapper::makesureIteratorExist() {
#define CREATE_DOCID_ITERATOR(indexPartReader, attrName, retValue)                                 \
    do {                                                                                           \
        AttributeReaderPtr attrReader = indexPartReader->GetAttributeReader(attrName);             \
        if (!attrReader) {                                                                         \
            return;                                                                                \
        }                                                                                          \
        AttributeIteratorBase *attrItBase = attrReader->CreateIterator(_sessionPool);              \
        retValue = dynamic_cast<DocMapAttrIterator *>(attrItBase);                                 \
        assert(retValue);                                                                          \
    } while (0)

    if (!_indexReaderVec) {
        AUTIL_LOG(ERROR, "partition reader vec empty");
        return;
    }
    if (_main2SubIt == NULL || _sub2MainIt == NULL) {
        assert(_main2SubIt == NULL && _sub2MainIt == NULL);
        CREATE_DOCID_ITERATOR((*_indexReaderVec)[IndexPartitionReader::MAIN_PART_ID],
                              MAIN_DOCID_TO_SUB_DOCID_ATTR_NAME,
                              _main2SubIt);
        CREATE_DOCID_ITERATOR((*_indexReaderVec)[IndexPartitionReader::SUB_PART_ID],
                              SUB_DOCID_TO_MAIN_DOCID_ATTR_NAME,
                              _sub2MainIt);
    }

#undef CREATE_DOCID_ITERATOR
}

bool IndexPartitionReaderWrapper::getAttributeMetaInfo(const string &attrName,
                                                       AttributeMetaInfo &metaInfo) const {
    if (_tabletReader) {
        return false;
    }
    if (_attrName2IdMap->empty()) {
        metaInfo.setAttributeName(attrName);
        metaInfo.setAttributeType(AT_MAIN_ATTRIBUTE);
        metaInfo.setIndexPartReader((*_indexReaderVec)[IndexPartitionReader::MAIN_PART_ID]);
        return true;
    }
    map<string, uint32_t>::const_iterator iter = _attrName2IdMap->find(attrName);
    if (iter == _attrName2IdMap->end()) {
        return false;
    }
    uint32_t id = iter->second;
    metaInfo.setAttributeName(attrName);
    switch (id) {
    case IndexPartitionReader::MAIN_PART_ID:
        metaInfo.setIndexPartReader((*_indexReaderVec)[IndexPartitionReader::MAIN_PART_ID]);
        metaInfo.setAttributeType(AT_MAIN_ATTRIBUTE);
        break;
    case IndexPartitionReader::SUB_PART_ID:
        metaInfo.setIndexPartReader((*_indexReaderVec)[IndexPartitionReader::SUB_PART_ID]);
        metaInfo.setAttributeType(AT_SUB_ATTRIBUTE);
        break;
    default:
        return false;
    }
    return true;
}

const IndexPartitionReaderPtr &IndexPartitionReaderWrapper::getReader() const {
    assert(_indexReaderVec);
    assert(_indexReaderVec->size() > 0);
    return (*_indexReaderVec)[IndexPartitionReader::MAIN_PART_ID];
}
const IndexPartitionReaderPtr &IndexPartitionReaderWrapper::getSubReader() const {
    static IndexPartitionReaderPtr EMPTY;
    if (_tabletReader) {
        return EMPTY;
    }
    assert(_indexReaderVec->size() > 0);
    if (_indexReaderVec->size() > IndexPartitionReader::SUB_PART_ID) {
        return (*_indexReaderVec)[IndexPartitionReader::SUB_PART_ID];
    }
    return EMPTY;
}

std::shared_ptr<InvertedIndexReader> IndexPartitionReaderWrapper::getIndexReader() const {
    if (_tabletReader) {
        auto normalTabletReader
            = std::dynamic_pointer_cast<indexlibv2::table::NormalTabletSessionReader>(
                _tabletReader);
        if (normalTabletReader) {
            return normalTabletReader->GetMultiFieldIndexReader();
        }
    } else {
        auto partitionReader = getReader();
        if (partitionReader) {
            return partitionReader->GetInvertedIndexReader();
        }
    }
    return nullptr;
}

std::shared_ptr<indexlibv2::config::ITabletSchema> IndexPartitionReaderWrapper::getSchema() const {
    if (_tabletReader) {
        return _tabletReader->GetSchema();
    } else {
        auto partitionReader = getReader();
        if (partitionReader) {
            return partitionReader->GetTabletSchema();
        }
    }
    return nullptr;
}

const std::shared_ptr<KVReader> &
IndexPartitionReaderWrapper::getKVReader(regionid_t regionId) const {
    if (_tabletReader) {
        // not support yet
    } else {
        auto partitionReader = getReader();
        if (partitionReader) {
            return partitionReader->GetKVReader(regionId);
        }
    }
    return RET_EMPTY_KV_READER;
}

const std::shared_ptr<KKVReader> &
IndexPartitionReaderWrapper::getKKVReader(regionid_t regionId) const {
    if (_tabletReader) {
        // not support yet
    } else {
        auto partitionReader = getReader();
        if (partitionReader) {
            return partitionReader->GetKKVReader(regionId);
        }
    }
    return RET_EMPTY_KKV_READER;
}

const std::shared_ptr<SummaryReader> IndexPartitionReaderWrapper::getSummaryReader() const {
    if (_tabletReader) {
        auto normalTabletReader
            = std::dynamic_pointer_cast<indexlibv2::table::NormalTabletSessionReader>(
                _tabletReader);
        if (normalTabletReader) {
            const auto &summaryReader = normalTabletReader->GetSummaryReader();
            const auto &pkReader = normalTabletReader->GetPrimaryKeyReader();
            if (summaryReader) {
                return std::make_shared<indexlibv2::index::TabletSummaryReader>(summaryReader.get(),
                                                                                pkReader);
            }
        }
    } else {
        auto partitionReader = getReader();
        if (partitionReader) {
            return partitionReader->GetSummaryReader();
        }
    }
    return RET_EMPTY_SUMMARY_READER;
}

const std::shared_ptr<AttributeReader> &
IndexPartitionReaderWrapper::getAttributeReader(const std::string &field) const {
    if (_tabletReader) {
        // not support yet
    } else {
        auto partitionReader = getReader();
        if (partitionReader) {
            return partitionReader->GetAttributeReader(field);
        }
    }
    return RET_EMPTY_ATTR_READER;
}

const std::shared_ptr<indexlib::index::PrimaryKeyIndexReader> &
IndexPartitionReaderWrapper::getPrimaryKeyReader() const {
    if (_tabletReader) {
        auto normalTabletReader
            = std::dynamic_pointer_cast<indexlibv2::table::NormalTabletSessionReader>(
                _tabletReader);
        if (normalTabletReader) {
            return normalTabletReader->GetPrimaryKeyReader();
        }
    } else {
        auto partitionReader = getReader();
        if (partitionReader) {
            return partitionReader->GetPrimaryKeyReader();
        }
    }
    return RET_EMPTY_PRIMARY_KEY_READER;
}

std::shared_ptr<indexlib::index::DeletionMapReaderAdaptor>
IndexPartitionReaderWrapper::getDeletionMapReader() const {
    if (_tabletReader) {
        auto normalTabletReader
            = std::dynamic_pointer_cast<indexlibv2::table::NormalTabletSessionReader>(
                _tabletReader);
        if (normalTabletReader) {
            return std::make_shared<indexlib::index::DeletionMapReaderAdaptor>(
                normalTabletReader->GetDeletionMapReader());
        }
    } else {
        auto partitionReader = getReader();
        if (partitionReader) {
            return std::make_shared<indexlib::index::DeletionMapReaderAdaptor>(
                partitionReader->GetDeletionMapReader());
        }
    }
    return nullptr;
}

bool IndexPartitionReaderWrapper::getSortedDocIdRanges(
    const std::vector<std::shared_ptr<indexlib::table::DimensionDescription>> &dimensions,
    const indexlib::DocIdRange &rangeLimits,
    indexlib::DocIdRangeVector &resultRanges) const {
    if (_tabletReader) {
        auto normalTabletReader
            = std::dynamic_pointer_cast<indexlibv2::table::NormalTabletSessionReader>(
                _tabletReader);
        if (normalTabletReader) {
            return normalTabletReader->GetSortedDocIdRanges(dimensions, rangeLimits, resultRanges);
        }
    } else {
        auto partitionReader = getReader();
        if (partitionReader) {
            return partitionReader->GetSortedDocIdRanges(dimensions, rangeLimits, resultRanges);
        }
    }
    return false;
}

bool IndexPartitionReaderWrapper::getIndexReader(const string &indexName,
                                                 std::shared_ptr<InvertedIndexReader> &indexReader,
                                                 bool &isSubIndex) {
    if (_tabletReader) {
        auto normalTabletReader
            = std::dynamic_pointer_cast<indexlibv2::table::NormalTabletSessionReader>(
                _tabletReader);
        if (normalTabletReader) {
            indexReader = normalTabletReader->GetMultiFieldIndexReader();
            return true;
        }
        return false;
    }
    uint32_t id = IndexPartitionReader::MAIN_PART_ID;
    if (_indexReaderVec->size() == 1) {
        indexReader = (*_indexReaderVec)[0]->GetInvertedIndexReader();
        id = IndexPartitionReader::MAIN_PART_ID;
        return true;
    }
    // todo: for test, need remove
    if (_indexName2IdMap->empty()) {
        indexReader = (*_indexReaderVec)[0]->GetInvertedIndexReader();
        id = IndexPartitionReader::MAIN_PART_ID;
        return true;
    }
    map<string, uint32_t>::const_iterator it = _indexName2IdMap->find(indexName);
    if (it == _indexName2IdMap->end()) {
        return false;
    }
    id = it->second;
    assert(IndexPartitionReader::MAIN_PART_ID <= id && id <= IndexPartitionReader::SUB_PART_ID);
    indexReader = (*_indexReaderVec)[id]->GetInvertedIndexReader();
    isSubIndex = (id == IndexPartitionReader::SUB_PART_ID);
    if (isSubIndex) {
        makesureIteratorExist();
        assert(_main2SubIt);
        assert(_sub2MainIt);
    }
    return true;
}

df_t IndexPartitionReaderWrapper::getTermDF(const common::Term &term) {
    LookupResult result;
    string key;
    getTermKeyStr(term, NULL, key);

    if (!tryGetFromPostingCache(key, result)) {
        result = doLookup(term, key, NULL);
    }
    if (result.postingIt != NULL) {
        return result.postingIt->GetTermMeta()->GetDocFreq();
    } else {
        return 0;
    }
}

bool IndexPartitionReaderWrapper::tryGetFromPostingCache(const string &key, LookupResult &result) {
    PostingCache::const_iterator it = _postingCache.find(key);
    if (it == _postingCache.end()) {
        return false;
    }
    result = it->second;
    return true;
}

indexlib::index::Term *IndexPartitionReaderWrapper::createIndexTerm(const common::Term &term,
                                                                    InvertedIndexType indexType,
                                                                    bool isSubIndex) {
    if (term.getTermType() == TT_STRING) {
        assert(term.getTermName() == "Term");

        if (indexType == it_customized) {
            return createAithetaTerm(term, isSubIndex);
        }

        indexlib::index::Term *indexTerm = new indexlib::index::Term();
        indexTerm->SetWord(term.getWord());
        indexTerm->SetIndexName(term.getIndexName());
        return indexTerm;
    } else {
        assert(term.getTermName() == "NumberTerm");
        const common::NumberTerm *numberTerm = static_cast<const common::NumberTerm *>(&term);
        indexlib::index::Int64Term *indexTerm = new indexlib::index::Int64Term(0, "dummy");
        indexTerm->SetWord(numberTerm->getWord());
        indexTerm->SetLeftNumber(numberTerm->getLeftNumber());
        indexTerm->SetRightNumber(numberTerm->getRightNumber());
        indexTerm->SetIndexName(numberTerm->getIndexName());
        return indexTerm;
    }
}

indexlibv2::index::ann::AithetaTerm *
IndexPartitionReaderWrapper::createAithetaTerm(const common::Term &term, bool isSubIndex) {
    auto *indexTerm = new indexlibv2::index::ann::AithetaTerm();
    indexTerm->SetWord(term.getWord());
    indexTerm->SetIndexName(term.getIndexName());

    if (!isSubIndex) {
        Filter *filter = _filterWrapper ? _filterWrapper->getFilter() : nullptr;
        if (filter) {
            auto *aithetaFilter = new AithetaFilter();
            aithetaFilter->SetFilter([filter](docid_t docId) -> bool {
                matchdoc::MatchDoc doc(0, docId);
                return !filter->pass(doc);
            });
            auto *auxSearchInfo = new indexlibv2::index::ann::AithetaAuxSearchInfo(
                shared_ptr<indexlibv2::index::ann::AithetaFilterBase>(aithetaFilter));
            indexTerm->SetAithetaSearchInfo(
                shared_ptr<indexlibv2::index::ann::AithetaAuxSearchInfo>(auxSearchInfo));
        }
    }
    return indexTerm;
}

void IndexPartitionReaderWrapper::getTermKeyStr(const common::Term &term,
                                                const LayerMeta *layerMeta,
                                                string &keyStr) {
    size_t size = term.getToken().getNormalizedText().size() + term.getIndexName().size()
                  + term.getTruncateName().size();
    keyStr.reserve(size + 2);
    keyStr = term.getToken().getNormalizedText().c_str();
    keyStr.append(1, '\t');
    keyStr += term.getIndexName();
    keyStr.append(1, '\t');
    keyStr += term.getTruncateName();
}

versionid_t IndexPartitionReaderWrapper::getCurrentVersion() const {
    if (_tabletReader) {
        auto normalTabletReader
            = std::dynamic_pointer_cast<indexlibv2::table::NormalTabletSessionReader>(
                _tabletReader);
        assert(normalTabletReader);
        const indexlibv2::framework::Version &version = normalTabletReader->GetVersion();
        return version.GetVersionId();
    }
    assert(_indexReaderVec->size() > 0);
    const Version &version = (*_indexReaderVec)[0]->GetVersion();
    return version.GetVersionId();
}

bool IndexPartitionReaderWrapper::genFieldMapMask(const string &indexName,
                                                  const vector<string> &termFieldNames,
                                                  fieldmap_t &targetFieldMap) {
    std::shared_ptr<InvertedIndexReader> indexReader;
    bool isSubIndex;
    if (!getIndexReader(indexName, indexReader, isSubIndex)) {
        return false;
    }
    return indexReader->GenFieldMapMask(indexName, termFieldNames, targetFieldMap);
}

bool IndexPartitionReaderWrapper::getPartedDocIdRanges(const indexlib::DocIdRangeVector &rangeHint,
                                                       size_t totalWayCount,
                                                       size_t wayIdx,
                                                       indexlib::DocIdRangeVector &ranges) const {
    if (_tabletReader) {
        auto normalTabletReader
            = std::dynamic_pointer_cast<indexlibv2::table::NormalTabletSessionReader>(
                _tabletReader);
        if (normalTabletReader) {
            return normalTabletReader->GetPartedDocIdRanges(
                rangeHint, totalWayCount, wayIdx, ranges);
        }
    } else {
        auto indexPartitionReader = getReader();
        if (nullptr == indexPartitionReader) {
            return false;
        }
        return indexPartitionReader->GetPartedDocIdRanges(rangeHint, totalWayCount, wayIdx, ranges);
    }
    return false;
}

bool IndexPartitionReaderWrapper::getPartedDocIdRanges(
    const indexlib::DocIdRangeVector &rangeHint,
    size_t totalWayCount,
    std::vector<indexlib::DocIdRangeVector> &rangeVectors) const {
    if (_tabletReader) {
        auto normalTabletReader
            = std::dynamic_pointer_cast<indexlibv2::table::NormalTabletSessionReader>(
                _tabletReader);
        if (normalTabletReader) {
            return normalTabletReader->GetPartedDocIdRanges(rangeHint, totalWayCount, rangeVectors);
        }
    } else {
        auto indexPartitionReader = getReader();
        if (nullptr == indexPartitionReader) {
            return false;
        }
        return indexPartitionReader->GetPartedDocIdRanges(rangeHint, totalWayCount, rangeVectors);
    }
    return false;
}

bool IndexPartitionReaderWrapper::getSubRanges(const LayerMeta *mainLayerMeta,
                                               indexlib::DocIdRangeVector &subRanges) {
    docid_t mainBegin = INVALID_DOCID;
    docid_t mainEnd = INVALID_DOCID;
    docid_t subBegin = INVALID_DOCID;
    docid_t subEnd = INVALID_DOCID;
    for (auto &docIdRangeMeta : *mainLayerMeta) {
        mainBegin = docIdRangeMeta.begin;
        mainEnd = docIdRangeMeta.end;
        subBegin = INVALID_DOCID;
        subEnd = INVALID_DOCID;
        if (0 == mainBegin) {
            subBegin = 0;
        } else {
            if (!_main2SubIt->Seek(mainBegin - 1, subBegin)) {
                return false;
            }
        }
        if (!_main2SubIt->Seek(mainEnd, subEnd)) {
            return false;
        }
        subRanges.emplace_back(subBegin, subEnd);
    }
    return true;
}

PostingIterator *IndexPartitionReaderWrapper::doLookupByRanges(
    const std::shared_ptr<InvertedIndexReader> &indexReaderPtr,
    const indexlib::index::Term *indexTerm,
    PostingType pt1,
    PostingType pt2,
    const LayerMeta *layerMeta,
    bool isSubIndex) {
    PostingIterator *iter
        = indexReaderPtr->Lookup(*indexTerm, _topK, pt1, _sessionPool).ValueOrThrow();
    if (!iter) {
        iter = indexReaderPtr->Lookup(*indexTerm, _topK, pt2, _sessionPool).ValueOrThrow();
    }
    return iter;
}

IndexPartitionReaderWrapper::InvertedTracerVector
IndexPartitionReaderWrapper::getInvertedTracers() {
    InvertedTracerVector result;
    for (; _tracerCursor < _delPostingVec.size(); _tracerCursor++) {
        auto *postingIter = _delPostingVec[_tracerCursor].second;
        if (!postingIter) {
            continue;
        }
        auto *tracer = postingIter->GetSearchTracer();
        if (!tracer) {
            continue;
        }
        auto &indexName = _delPostingVec[_tracerCursor].first;
        result.emplace_back(&indexName, tracer);
    }
    return result;
}

void IndexPartitionReaderWrapper::clearObject() {
    POOL_COMPATIBLE_DELETE_CLASS(_sessionPool, _main2SubIt);
    POOL_COMPATIBLE_DELETE_CLASS(_sessionPool, _sub2MainIt);

    // clear posting cache
    for (size_t i = 0; i < _delPostingVec.size(); i++) {
        auto &posting = _delPostingVec[i].second;
        if (posting != NULL) {
            POOL_COMPATIBLE_DELETE_CLASS(posting->GetSessionPool(), posting);
            posting = NULL;
        }
    }

    _topK = 0;
    _tracerCursor = 0;
    _delPostingVec.clear();
    _postingCache.clear();
    _partitionInfoWrapperPtr.reset();
    _subPartitionInfoPtr.reset();
    // _sessionPool = nullptr;
    _main2SubIt = nullptr;
    _sub2MainIt = nullptr;
}

bool IndexPartitionReaderWrapper::pk2DocId(const primarykey_t &key,
                                           bool ignoreDelete,
                                           docid_t &docid) const {
    const auto &pkIndexReader = getPrimaryKeyReader();
    if (!pkIndexReader) {
        return false;
    }
    auto invertedType = pkIndexReader->GetInvertedIndexType();
    if (invertedType == it_primarykey64) {
        return pk2DocIdImpl<uint64_t>(pkIndexReader, key.value[1], ignoreDelete, docid);
    } else {
        assert(invertedType == it_primarykey128);
        return pk2DocIdImpl<autil::uint128_t>(pkIndexReader, key, ignoreDelete, docid);
    }
}

template <typename Key>
bool IndexPartitionReaderWrapper::pk2DocIdImpl(
    const std::shared_ptr<indexlib::index::PrimaryKeyIndexReader> &pkIndexReader,
    const Key &key,
    bool ignoreDelete,
    docid_t &docid) const {
    if (_tabletReader) {
        const auto &pkReader
            = std::dynamic_pointer_cast<indexlibv2::index::PrimaryKeyReader<Key>>(pkIndexReader);
        assert(pkReader);
        docid_t lastDocId = INVALID_DOCID;
        try {
            docid = pkReader->Lookup(key, lastDocId);
        } catch (...) { return false; }
        if (docid == INVALID_DOCID && ignoreDelete) {
            docid = lastDocId;
        }
        return true;
    }
    const auto &legacyPkReader
        = std::dynamic_pointer_cast<indexlib::index::LegacyPrimaryKeyReader<Key>>(pkIndexReader);
    assert(legacyPkReader);
    docid_t lastDocId = INVALID_DOCID;
    try {
        docid = legacyPkReader->Lookup(key, lastDocId);
    } catch (...) { return false; }
    if (docid == INVALID_DOCID && ignoreDelete) {
        docid = lastDocId;
    }
    return true;
}

} // namespace search
} // namespace isearch
