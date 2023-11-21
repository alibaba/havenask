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

#include <assert.h>
#include <map>
#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <unordered_map>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/isearch.h"
#include "ha3/search/PartitionInfoWrapper.h"
#include "indexlib/index/ann/aitheta2/AithetaTerm.h"
#include "indexlib/index/inverted_index/InvertedIndexReader.h"
#include "indexlib/index/inverted_index/PostingIterator.h"
#include "indexlib/index/normal/attribute/accessor/join_docid_attribute_iterator.h"
#include "indexlib/index/partition_info.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/common.h"
#include "indexlib/partition/partition_define.h"
#include "indexlib/table/normal_table/NormalTabletReader.h"

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil

namespace indexlib::index {
class DeletionMapReaderAdaptor;
}

namespace indexlibv2::config {
class ITabletSchema;
}

namespace indexlib {
namespace table {
struct DimensionDescription;
}

namespace partition {
class PartitionReaderSnapshot;
}

namespace util {
class Term;
} // namespace util

namespace index {
class KKVReader;
class KVReader;
class SummaryReader;
class PrimaryKeyIndexReader;
class AttributeReader;
class InvertedIndexSearchTracer;
class FieldMetaReader;
} // namespace index

} // namespace indexlib

namespace isearch {
namespace common {
class Term;
} // namespace common
namespace search {
class AttributeMetaInfo;
class LayerMeta;
class FilterWrapper;
} // namespace search
} // namespace isearch

namespace isearch {
namespace search {

struct LookupResult {
    LookupResult()
        : postingIt(NULL)
        , main2SubIt(NULL)
        , sub2MainIt(NULL)
        , isSubPartition(false) {}

    indexlib::index::PostingIterator *postingIt;
    indexlib::index::JoinDocidAttributeIterator *main2SubIt;
    indexlib::index::JoinDocidAttributeIterator *sub2MainIt;
    bool isSubPartition;
};

typedef std::shared_ptr<indexlib::index::FieldMetaReader> FieldMetaReaderPtr;
typedef std::shared_ptr<std::map<std::string, FieldMetaReaderPtr>> FieldMetaReadersMapPtr;

class PartitionInfoWrapper;

class IndexPartitionReaderWrapper {
public:
    typedef indexlib::index::JoinDocidAttributeIterator DocMapAttrIterator;
    typedef indexlib::index::PostingIterator PostingIterator;
    typedef std::unordered_map<std::string, LookupResult> PostingCache;
    typedef std::vector<std::pair<std::string, indexlib::index::PostingIterator *>>
        DelPostingVector;
    typedef indexlib::partition::IndexPartitionReaderPtr IndexPartitionReaderPtr;
    typedef indexlib::index::PartitionInfoPtr PartitionInfoPtr;
    typedef std::vector<
        std::pair<const std::string *, const indexlib::index::InvertedIndexSearchTracer *>>
        InvertedTracerVector;

public:
    IndexPartitionReaderWrapper(const std::map<std::string, uint32_t> *indexName2IdMap,
                                const std::map<std::string, uint32_t> *attrName2IdMap,
                                const std::vector<IndexPartitionReaderPtr> *indexReaderVec,
                                bool ownPointer);
    explicit IndexPartitionReaderWrapper(
        const std::shared_ptr<indexlibv2::framework::ITabletReader> &tabletReader);

    // for test
    IndexPartitionReaderWrapper() {}
    virtual ~IndexPartitionReaderWrapper();

private:
    IndexPartitionReaderWrapper(const IndexPartitionReaderWrapper &);
    IndexPartitionReaderWrapper &operator=(const IndexPartitionReaderWrapper &);

public:
    LookupResult lookup(const common::Term &term, const LayerMeta *layerMeta);

    bool pk2DocId(const primarykey_t &key, bool ignoreDelete, docid_t &docid) const;

    const IndexPartitionReaderPtr &getSubReader() const;
    const std::shared_ptr<PartitionInfoWrapper> &getPartitionInfo() const {
        return _partitionInfoWrapperPtr;
    }
    const PartitionInfoPtr &getSubPartitionInfo() const {
        return _subPartitionInfoPtr;
    }
    bool genFieldMapMask(const std::string &indexName,
                         const std::vector<std::string> &termFieldNames,
                         fieldmap_t &targetFieldMap);
    df_t getTermDF(const common::Term &term);
    versionid_t getCurrentVersion() const;
    DocMapAttrIterator *getMainToSubIter() {
        makesureIteratorExist();
        return _main2SubIt;
    }
    DocMapAttrIterator *getSubToMainIter() {
        makesureIteratorExist();
        return _sub2MainIt;
    }
    void addDelPosting(const std::string &indexName, indexlib::index::PostingIterator *postIter);
    bool getPartedDocIdRanges(const indexlib::DocIdRangeVector &rangeHint,
                              size_t totalWayCount,
                              size_t wayIdx,
                              indexlib::DocIdRangeVector &ranges) const;
    bool getPartedDocIdRanges(const indexlib::DocIdRangeVector &rangeHint,
                              size_t totalWayCount,
                              std::vector<indexlib::DocIdRangeVector> &rangeVectors) const;

    std::shared_ptr<indexlib::index::InvertedIndexReader> getIndexReader() const;
    std::shared_ptr<indexlibv2::config::ITabletSchema> getSchema() const;
    const std::shared_ptr<indexlib::index::KVReader> &getKVReader(regionid_t regionId
                                                                  = DEFAULT_REGIONID) const;
    const std::shared_ptr<indexlib::index::KKVReader> &getKKVReader(regionid_t regionId
                                                                    = DEFAULT_REGIONID) const;
    const std::shared_ptr<indexlib::index::SummaryReader> getSummaryReader() const;
    const std::shared_ptr<indexlib::index::AttributeReader> &
    getAttributeReader(const std::string &field) const;
    const std::shared_ptr<indexlib::index::PrimaryKeyIndexReader> &getPrimaryKeyReader() const;
    std::shared_ptr<indexlib::index::DeletionMapReaderAdaptor> getDeletionMapReader() const;
    const FieldMetaReadersMapPtr getFieldMetaReadersMap();

    virtual bool getSortedDocIdRanges(
        const std::vector<std::shared_ptr<indexlib::table::DimensionDescription>> &dimensions,
        const indexlib::DocIdRange &rangeLimits,
        indexlib::DocIdRangeVector &resultRanges) const;

public:
    void setTopK(uint32_t topK) {
        _topK = topK;
    }
    uint32_t getTopK() const {
        return _topK;
    }
    void setSessionPool(autil::mem_pool::Pool *sessionPool) {
        _sessionPool = sessionPool;
    }
    void setFilterWrapper(FilterWrapper *filterWrapper) {
        _filterWrapper = filterWrapper;
    }

protected:
    virtual void
    getTermKeyStr(const common::Term &term, const LayerMeta *layerMeta, std::string &keyStr);

public:
    // for test
    const std::vector<IndexPartitionReaderPtr> *getIndexPartReaders() const {
        return _indexReaderVec;
    }
    std::shared_ptr<indexlib::index::InvertedIndexReader>
    getIndexReader(const std::string &indexName) {
        bool unused;
        std::shared_ptr<indexlib::index::InvertedIndexReader> indexReader;
        getIndexReader(indexName, indexReader, unused);
        return indexReader;
    }
    bool getAttributeMetaInfo(const std::string &attrName, AttributeMetaInfo &metaInfo) const;
    bool getIndexReader(const std::string &indexName,
                        std::shared_ptr<indexlib::index::InvertedIndexReader> &indexReader,
                        bool &isSubIndex);
    void truncateRewrite(const std::string &truncateName,
                         indexlib::index::Term &indexTerm,
                         PostingType &pt1,
                         PostingType &pt2);
    LookupResult doLookupWithoutCache(
        const std::shared_ptr<indexlib::index::InvertedIndexReader> &indexReaderPtr,
        bool isSubIndex,
        const indexlib::index::Term &indexTerm,
        PostingType pt1,
        PostingType pt2,
        const LayerMeta *layerMeta);
    // virtual for test
    virtual InvertedTracerVector getInvertedTracers();
    size_t getTracerCursor() {
        return _tracerCursor;
    }
    void clearObject();

    const std::map<std::string, double> getFieldLengthMetaMap();

private:
    const IndexPartitionReaderPtr &getReader() const;

    bool tryGetFromPostingCache(const std::string &key, LookupResult &result);
    indexlib::index::Term *
    createIndexTerm(const common::Term &term, InvertedIndexType indexType, bool isSubIndex);
    indexlibv2::index::ann::AithetaTerm *createAithetaTerm(const common::Term &term,
                                                           bool isSubIndex);
    LookupResult
    doLookup(const common::Term &term, const std::string &key, const LayerMeta *layerMeta);
    LookupResult makeLookupResult(bool isSubIndex,
                                  indexlib::index::PostingIterator *postIter,
                                  const std::string &indexName);
    void makesureIteratorExist();
    template <typename Key>
    bool pk2DocIdImpl(const std::shared_ptr<indexlib::index::PrimaryKeyIndexReader> &pkIndexReader,
                      const Key &key,
                      bool ignoreDelete,
                      docid_t &docid) const;
    InvertedIndexType
    getIndexType(std::shared_ptr<indexlib::index::InvertedIndexReader> indexReaderPtr,
                 const std::string &indexName);

protected:
    virtual PostingIterator *
    doLookupByRanges(const std::shared_ptr<indexlib::index::InvertedIndexReader> &indexReaderPtr,
                     const indexlib::index::Term *indexTerm,
                     PostingType pt1,
                     PostingType pt2,
                     const LayerMeta *layerMeta,
                     bool isSubIndex);
    bool getSubRanges(const LayerMeta *mainLayerMeta, indexlib::DocIdRangeVector &subRanges);

protected:
    bool _ownPointer;
    const std::map<std::string, uint32_t> *_indexName2IdMap = nullptr;
    const std::map<std::string, uint32_t> *_attrName2IdMap = nullptr;
    const std::vector<IndexPartitionReaderPtr> *_indexReaderVec = nullptr;
    std::shared_ptr<indexlibv2::framework::ITabletReader> _tabletReader;
    std::shared_ptr<PartitionInfoWrapper> _partitionInfoWrapperPtr;

    PartitionInfoPtr _subPartitionInfoPtr;
    PostingCache _postingCache;
    DelPostingVector _delPostingVec;
    size_t _tracerCursor = 0;
    uint32_t _topK;
    autil::mem_pool::Pool *_sessionPool = nullptr;
    // for sub partition seek
    DocMapAttrIterator *_main2SubIt = nullptr;
    DocMapAttrIterator *_sub2MainIt = nullptr;

    FilterWrapper *_filterWrapper = nullptr;

    static std::shared_ptr<indexlib::index::SummaryReader> RET_EMPTY_SUMMARY_READER;
    static std::shared_ptr<indexlib::index::PrimaryKeyIndexReader> RET_EMPTY_PRIMARY_KEY_READER;
    static std::shared_ptr<indexlib::index::InvertedIndexReader> RET_EMPTY_INDEX_READER;
    static std::shared_ptr<indexlib::index::KKVReader> RET_EMPTY_KKV_READER;
    static std::shared_ptr<indexlib::index::KVReader> RET_EMPTY_KV_READER;
    static std::shared_ptr<indexlib::index::AttributeReader> RET_EMPTY_ATTR_READER;

private:
    friend class IndexPartitionReaderWrapperTest;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<IndexPartitionReaderWrapper> IndexPartitionReaderWrapperPtr;

} // namespace search
} // namespace isearch
