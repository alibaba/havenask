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
#include <stddef.h>
#include <stdint.h>
#include <list>
#include <unordered_map>

#include "autil/CommonMacros.h"
#include "autil/mem_pool/PoolVector.h"
#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/index/normal/attribute/accessor/join_docid_attribute_iterator.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader_adaptor.h"
#include "indexlib/index/partition_info.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/common.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/Reference.h"
#include "suez/turing/expression/framework/JoinDocIdConverterBase.h"

#include "ha3/search/HashJoinInfo.h"
#include "ha3/common/TimeoutTerminator.h"
#include "ha3/isearch.h"
#include "ha3/search/FilterWrapper.h"
#include "ha3/search/LayerMetas.h"
#include "ha3/search/MatchDataManager.h"
#include "ha3/search/QueryExecutor.h"
#include "autil/HashUtil.h"
#include "autil/Log.h" // IWYU pragma: keep

namespace suez {
namespace turing {
class AttributeExpression;
template <typename T> class AttributeExpressionTyped;
}  // namespace turing
}  // namespace suez

namespace isearch {
namespace search {

class SingleLayerSearcher
{
public:
    typedef indexlib::index::JoinDocidAttributeIterator DocMapAttrIterator;

public:
    SingleLayerSearcher(QueryExecutor *queryExecutor,
                        LayerMeta *layerMeta,
                        FilterWrapper *filterWrapper,
                        indexlib::index::DeletionMapReaderAdaptor *deletionMapReader,
                        matchdoc::MatchDocAllocator *matchDocAllocator,
                        common::TimeoutTerminator *timeoutTerminator,
                        DocMapAttrIterator *main2subIt,
                        indexlib::index::DeletionMapReader *subDeletionMapReader,
                        MatchDataManager *manager = NULL,
             	        bool getAllSubDoc = false,
	const search::HashJoinInfo *hashJoinInfo = nullptr,
	suez::turing::AttributeExpression *joinAttrExpr = nullptr
	);
    ~SingleLayerSearcher();
private:
    SingleLayerSearcher(const SingleLayerSearcher &);
    SingleLayerSearcher& operator=(const SingleLayerSearcher &);
public:
    indexlib::index::ErrorCode seek(
        bool needSubDoc, matchdoc::MatchDoc& matchDoc) __attribute__((always_inline));
    template <typename T>
    indexlib::index::ErrorCode
    seekAndJoin(bool needSubDoc, matchdoc::MatchDoc &matchDocs);

    template <typename T>
    static void doJoin(matchdoc::MatchDocAllocator *matchDocAllocator,
            matchdoc::MatchDoc matchDoc,
            std::list<matchdoc::MatchDoc> &matchDocs,
            const search::HashJoinInfo *hashJoinInfo,
            suez::turing::AttributeExpressionTyped<T> *joinAttrExpr);

    matchdoc::MatchDocAllocator* getMatchDocAllocator() const {
        return _matchDocAllocator;
    }
    // only call it when seek to the end.
    uint32_t getLeftQuotaInTheEnd() const;
    uint32_t getSeekedCount() const;
    uint32_t getSeekTimes() const {
        return _seekTimes;
    }
    uint32_t getSeekDocCount() const {
        return _queryExecutor->getSeekDocCount();
    }
private:
    bool moveToNextRange();
    bool moveBack();
    bool moveToCorrectRange(docid_t &docId);
    inline bool tryToMakeItInRange(docid_t &docId);
    indexlib::index::ErrorCode constructSubMatchDocs(matchdoc::MatchDoc matchDoc);
private:
    docid_t _curDocId;
    docid_t _curBegin;
    docid_t _curEnd;
    uint32_t _curQuota;
    docid_t _cousorNextBegin;
    size_t _rangeCousor;
    common::TimeoutTerminator *_timeoutTerminator;
    QueryExecutor *_queryExecutor;
    FilterWrapper *_filterWrapper;
    LayerMeta *_layerMeta;
    indexlib::index::DeletionMapReaderAdaptor *_deletionMapReader;
    matchdoc::MatchDocAllocator *_matchDocAllocator;
    DocMapAttrIterator* _main2SubIt;
    indexlib::index::DeletionMapReader *_subDeletionMapReader;
    MatchDataManager *_matchDataManager;
    bool _getAllSubDoc;
    uint32_t _seekTimes;
    const search::HashJoinInfo *_hashJoinInfo;
    suez::turing::AttributeExpression *_joinAttrExpr;
    std::list<matchdoc::MatchDoc> _matchDocBuffer;

private:
    friend class SingleLayerSearcherTest;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SingleLayerSearcher> SingleLayerSearcherPtr;

////////////////////////////////////////////////////////////////////////////

inline uint32_t SingleLayerSearcher::getLeftQuotaInTheEnd() const {
    if (_layerMeta->quotaMode == QM_PER_LAYER) {
        assert(_layerMeta->maxQuota >= _curQuota);
        uint32_t usedQuota = _layerMeta->maxQuota - _curQuota;
        return usedQuota >= _layerMeta->quota ? 0 : _layerMeta->quota - usedQuota;
    } else {
        return _curQuota;
    }
}

inline uint32_t SingleLayerSearcher::getSeekedCount() const {
    uint32_t seekedDocCount = 0;
    for (size_t cousor = 0; cousor < _layerMeta->size(); ++cousor) {
        if (cousor != _rangeCousor) {
            seekedDocCount += (*_layerMeta)[cousor].nextBegin -
                              (*_layerMeta)[cousor].begin;
        } else {
            seekedDocCount += _cousorNextBegin -
                              (*_layerMeta)[cousor].begin;
        }
    }

    return seekedDocCount;
}

inline bool SingleLayerSearcher::tryToMakeItInRange(docid_t &docId) {
    if (_curQuota > 0 && _curEnd >= docId) {
        _cousorNextBegin = docId + 1;
        return true;
    }
    return moveToCorrectRange(docId);
}

inline indexlib::index::ErrorCode SingleLayerSearcher::seek(
    bool needSubDoc, matchdoc::MatchDoc& matchDoc) {
    docid_t docId = _curDocId;
    assert(docId >= _curBegin);
    // if filter need MatchData, fetch MatchData before filter
    bool needMatchData = _matchDataManager && _matchDataManager->hasMatchData();
    bool needMatchValues = _matchDataManager && _matchDataManager->hasMatchValues();
    bool needMatchDataBeforeFilter = needMatchData && _matchDataManager->filterNeedMatchData();
    bool needMatchDataAfterFilter = needMatchData && !_matchDataManager->filterNeedMatchData();
    bool needMatchedRowInfo = _matchDataManager && !_matchDataManager->getMatchDataCollectorCenter().isEmpty();
    while (true) {
        if (_timeoutTerminator && _timeoutTerminator->checkTimeout()) {
            break;
        }
        auto ec = _queryExecutor->seekWithoutCheck(docId, docId);
        IE_RETURN_CODE_IF_ERROR(ec);
        if (unlikely(!tryToMakeItInRange(docId))) {
            if (docId == END_DOCID) {
                break;
            }
            continue;
        }
        ++_seekTimes;
        if (_deletionMapReader && _deletionMapReader->IsDeleted(docId)) {
            ++docId;
            continue;
        }
        matchDoc = _matchDocAllocator->allocate(docId);
        if (unlikely(needSubDoc)) {
            auto ec = constructSubMatchDocs(matchDoc);
            if (unlikely(ec != indexlib::index::ErrorCode::OK)) {
                _matchDocAllocator->deallocate(matchDoc);
                return ec;
            }
        }
        if (needMatchDataBeforeFilter) {
            auto ec = _matchDataManager->fillMatchData(matchDoc);
            if (unlikely(ec != indexlib::index::ErrorCode::OK)) {
                _matchDocAllocator->deallocate(matchDoc);
                return ec;
            }
        }
        docId++;
        if (!_filterWrapper || _filterWrapper->pass(matchDoc)) {
            --_curQuota;
            _curDocId = docId;
            if (needMatchDataAfterFilter) {
                auto ec = _matchDataManager->fillMatchData(matchDoc);
                if (unlikely(ec != indexlib::index::ErrorCode::OK)) {
                    _matchDocAllocator->deallocate(matchDoc);
                    return ec;
                }
            }

            if (needMatchValues) {
                auto ec = _matchDataManager->fillMatchValues(matchDoc);
                if (unlikely(ec != indexlib::index::ErrorCode::OK)) {
                    _matchDocAllocator->deallocate(matchDoc);
                    return ec;
                }
            }

            if (needMatchedRowInfo) {
                _matchDataManager->getMatchDataCollectorCenter().collectAll(
                        _queryExecutor, matchDoc);
            }
            return indexlib::index::ErrorCode::OK;
        }
        _matchDocAllocator->deallocate(matchDoc);
    }
    _curDocId = docId;
    matchDoc = matchdoc::INVALID_MATCHDOC;
    return indexlib::index::ErrorCode::OK;
}

template <typename T>
indexlib::index::ErrorCode SingleLayerSearcher::seekAndJoin(
        bool needSubDoc, matchdoc::MatchDoc &matchDoc) {
    docid_t docId = _curDocId;
    assert(docId >= _curBegin);
    // if filter need MatchData, fetch MatchData before filter
    bool needMatchData = _matchDataManager && _matchDataManager->hasMatchData();
    bool needMatchValues = _matchDataManager && _matchDataManager->hasMatchValues();
    bool needMatchDataBeforeFilter = needMatchData && _matchDataManager->filterNeedMatchData();
    bool needMatchDataAfterFilter = needMatchData && !_matchDataManager->filterNeedMatchData();
    while (true) {
        if (_timeoutTerminator && _timeoutTerminator->checkTimeout()) {
            break;
        }
	matchdoc::MatchDoc tmpMatchDoc;
	if (_matchDocBuffer.empty()) {
            ++_seekTimes;
            auto ec = _queryExecutor->seekWithoutCheck(docId, docId);
            IE_RETURN_CODE_IF_ERROR(ec);
            if (unlikely(!tryToMakeItInRange(docId))) {
                if (docId == END_DOCID) {
                    break;
                }
                continue;
            }
            if (_deletionMapReader && _deletionMapReader->IsDeleted(docId)) {
                ++docId;
                continue;
            }
            tmpMatchDoc = _matchDocAllocator->allocate(docId);
	    docId++;
            if (unlikely(needSubDoc)) {
                auto ec = constructSubMatchDocs(tmpMatchDoc);
                if (unlikely(ec != indexlib::index::ErrorCode::OK)) {
                    _matchDocAllocator->deallocate(tmpMatchDoc);
                    return ec;
                }
            }
            doJoin<T>(_matchDocAllocator, tmpMatchDoc, _matchDocBuffer, _hashJoinInfo,
                    (suez::turing::AttributeExpressionTyped<T> *)_joinAttrExpr);
        } else {
	    tmpMatchDoc = _matchDocBuffer.front();
	    _matchDocBuffer.pop_front();
	}

        if (needMatchDataBeforeFilter) {
            auto ec = _matchDataManager->fillMatchData(tmpMatchDoc);
            if (unlikely(ec != indexlib::index::ErrorCode::OK)) {
                _matchDocAllocator->deallocate(tmpMatchDoc);
                return ec;
            }
        }

        if (!_filterWrapper || _filterWrapper->pass(tmpMatchDoc)) {
            --_curQuota;
            _curDocId = docId;
            if (needMatchDataAfterFilter) {
                auto ec = _matchDataManager->fillMatchData(tmpMatchDoc);
                if (unlikely(ec != indexlib::index::ErrorCode::OK)) {
                    _matchDocAllocator->deallocate(tmpMatchDoc);
                    return ec;
                }
            }
            if (needMatchValues) {
                auto ec = _matchDataManager->fillMatchValues(tmpMatchDoc);
                if (unlikely(ec != indexlib::index::ErrorCode::OK)) {
                    _matchDocAllocator->deallocate(tmpMatchDoc);
                    return ec;
                }
            }
	    matchDoc = tmpMatchDoc;
            return indexlib::index::ErrorCode::OK;
        }
        _matchDocAllocator->deallocate(tmpMatchDoc);
    }
    _curDocId = docId;
    matchDoc = matchdoc::INVALID_MATCHDOC;
    return indexlib::index::ErrorCode::OK;
}

template <typename T>
void SingleLayerSearcher::doJoin(matchdoc::MatchDocAllocator *matchDocAllocator,
        matchdoc::MatchDoc matchDoc,
        std::list<matchdoc::MatchDoc> &matchDocs,
        const search::HashJoinInfo *hashJoinInfo,
        suez::turing::AttributeExpressionTyped<T> *joinAttrExpr) {
    T joinAttrValue = joinAttrExpr->evaluateAndReturn(matchDoc);
    auto hashKey = autil::HashUtil::calculateHashValue(joinAttrValue);

    matchdoc::Reference<suez::turing::DocIdWrapper> *auxDocidRef =
            hashJoinInfo->getAuxDocidRef();

    const auto &hashJoinMap = hashJoinInfo->getHashJoinMap();
    auto joinIter = hashJoinMap.find(hashKey);
    if (joinIter != hashJoinMap.end()) {
	const auto &auxDocidVec = joinIter->second;
	assert(auxDocidVec.size() >= 1);
	auxDocidRef->set(matchDoc, auxDocidVec[0]);
        for (size_t i = 1; i < auxDocidVec.size(); i++) {
            matchdoc::MatchDoc joinMatchDoc =
                    matchDocAllocator->allocateAndCloneWithSub(matchDoc);
            auxDocidRef->set(joinMatchDoc, auxDocidVec[i]);
            matchDocs.push_back(joinMatchDoc);
        }
    } else {
	auxDocidRef->set(matchDoc, INVALID_DOCID);
    }
}

} // namespace search
} // namespace isearch
