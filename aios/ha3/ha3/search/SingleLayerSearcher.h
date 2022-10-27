#ifndef ISEARCH_SINGLELAYERSEARCHER_H
#define ISEARCH_SINGLELAYERSEARCHER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/LayerMetas.h>
#include <ha3/search/QueryExecutor.h>
#include <ha3/search/FilterWrapper.h>
#include <ha3/common/Ha3MatchDocAllocator.h>
#include <ha3/search/MatchDataManager.h>
#include <matchdoc/MatchDocAllocator.h>
#include <indexlib/index/normal/deletionmap/deletion_map_reader.h>
#include <indexlib/index/normal/attribute/accessor/join_docid_attribute_iterator.h>
#include <ha3/common/TimeoutTerminator.h>
#include <ha3/common/HashJoinInfo.h>
#include <suez/turing/expression/framework/AttributeExpression.h>
#include <suez/turing/expression/framework/JoinDocIdConverterBase.h>
#include <ha3/sql/data/TableUtil.h>

#include <list>

BEGIN_HA3_NAMESPACE(search);

class SingleLayerSearcher
{
public:
    typedef IE_NAMESPACE(index)::DeletionMapReaderPtr DeletionMapReaderPtr;
    typedef IE_NAMESPACE(index)::JoinDocidAttributeIterator DocMapAttrIterator;

public:
    SingleLayerSearcher(QueryExecutor *queryExecutor,
                        LayerMeta *layerMeta,
                        FilterWrapper *filterWrapper,
                        IE_NAMESPACE(index)::DeletionMapReader *deletionMapReader,
                        matchdoc::MatchDocAllocator *matchDocAllocator,
                        common::TimeoutTerminator *timeoutTerminator,
                        DocMapAttrIterator *main2subIt,
                        IE_NAMESPACE(index)::DeletionMapReader *subDeletionMapReader,
                        MatchDataManager *manager = NULL,
             	        bool getAllSubDoc = false,
	const common::HashJoinInfo *hashJoinInfo = nullptr,
	suez::turing::AttributeExpression *joinAttrExpr = nullptr
	);
    ~SingleLayerSearcher();
private:
    SingleLayerSearcher(const SingleLayerSearcher &);
    SingleLayerSearcher& operator=(const SingleLayerSearcher &);
public:
    IE_NAMESPACE(common)::ErrorCode seek(
        bool needSubDoc, matchdoc::MatchDoc& matchDoc) __attribute__((always_inline));
    template <typename T>
    IE_NAMESPACE(common)::ErrorCode
    seekAndJoin(bool needSubDoc, matchdoc::MatchDoc &matchDocs);

    template <typename T>
    static void doJoin(matchdoc::MatchDocAllocator *matchDocAllocator,
            matchdoc::MatchDoc matchDoc,
            std::list<matchdoc::MatchDoc> &matchDocs,
            const common::HashJoinInfo *hashJoinInfo,
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
    IE_NAMESPACE(common)::ErrorCode constructSubMatchDocs(matchdoc::MatchDoc matchDoc);
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
    IE_NAMESPACE(index)::DeletionMapReader *_deletionMapReader;
    matchdoc::MatchDocAllocator *_matchDocAllocator;
    DocMapAttrIterator* _main2SubIt;
    IE_NAMESPACE(index)::DeletionMapReader *_subDeletionMapReader;
    MatchDataManager *_matchDataManager;
    bool _getAllSubDoc;
    uint32_t _seekTimes;
    const common::HashJoinInfo *_hashJoinInfo;
    suez::turing::AttributeExpression *_joinAttrExpr;
    std::list<matchdoc::MatchDoc> _matchDocBuffer;

private:
    friend class SingleLayerSearcherTest;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(SingleLayerSearcher);

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

inline IE_NAMESPACE(common)::ErrorCode SingleLayerSearcher::seek(
    bool needSubDoc, matchdoc::MatchDoc& matchDoc) {
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
            if (unlikely(ec != IE_NAMESPACE(common)::ErrorCode::OK)) {
                _matchDocAllocator->deallocate(matchDoc);
                return ec;
            }
        }
        if (needMatchDataBeforeFilter) {
            auto ec = _matchDataManager->fillMatchData(matchDoc);
            if (unlikely(ec != IE_NAMESPACE(common)::ErrorCode::OK)) {
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
                if (unlikely(ec != IE_NAMESPACE(common)::ErrorCode::OK)) {
                    _matchDocAllocator->deallocate(matchDoc);
                    return ec;
                }
            }

            if (needMatchValues) {
                auto ec = _matchDataManager->fillMatchValues(matchDoc);
                if (unlikely(ec != IE_NAMESPACE(common)::ErrorCode::OK)) {
                    _matchDocAllocator->deallocate(matchDoc);
                    return ec;
                }
            }
            return IE_NAMESPACE(common)::ErrorCode::OK;
        }
        _matchDocAllocator->deallocate(matchDoc);
    }
    _curDocId = docId;
    matchDoc = matchdoc::INVALID_MATCHDOC;
    return IE_NAMESPACE(common)::ErrorCode::OK;
}

template <typename T>
IE_NAMESPACE(common)::ErrorCode SingleLayerSearcher::seekAndJoin(
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
                if (unlikely(ec != IE_NAMESPACE(common)::ErrorCode::OK)) {
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
            if (unlikely(ec != IE_NAMESPACE(common)::ErrorCode::OK)) {
                _matchDocAllocator->deallocate(tmpMatchDoc);
                return ec;
            }
        }

        if (!_filterWrapper || _filterWrapper->pass(tmpMatchDoc)) {
            --_curQuota;
            _curDocId = docId;
            if (needMatchDataAfterFilter) {
                auto ec = _matchDataManager->fillMatchData(tmpMatchDoc);
                if (unlikely(ec != IE_NAMESPACE(common)::ErrorCode::OK)) {
                    _matchDocAllocator->deallocate(tmpMatchDoc);
                    return ec;
                }
            }
            if (needMatchValues) {
                auto ec = _matchDataManager->fillMatchValues(tmpMatchDoc);
                if (unlikely(ec != IE_NAMESPACE(common)::ErrorCode::OK)) {
                    _matchDocAllocator->deallocate(tmpMatchDoc);
                    return ec;
                }
            }
	    matchDoc = tmpMatchDoc;
            return IE_NAMESPACE(common)::ErrorCode::OK;
        }
        _matchDocAllocator->deallocate(tmpMatchDoc);
    }
    _curDocId = docId;
    matchDoc = matchdoc::INVALID_MATCHDOC;
    return IE_NAMESPACE(common)::ErrorCode::OK;
}

template <typename T>
void SingleLayerSearcher::doJoin(matchdoc::MatchDocAllocator *matchDocAllocator,
        matchdoc::MatchDoc matchDoc,
        std::list<matchdoc::MatchDoc> &matchDocs,
        const common::HashJoinInfo *hashJoinInfo,
        suez::turing::AttributeExpressionTyped<T> *joinAttrExpr) {
    T joinAttrValue = joinAttrExpr->evaluateAndReturn(matchDoc);
    auto hashKey = HA3_NS(sql)::TableUtil::calculateHashValue(joinAttrValue);

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

END_HA3_NAMESPACE(search);

#endif //ISEARCH_SINGLELAYERSEARCHER_H
