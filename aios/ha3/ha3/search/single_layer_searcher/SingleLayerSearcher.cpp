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
#include "ha3/search/SingleLayerSearcher.h"

#include <algorithm>

#include "autil/Log.h"
#include "ha3/common/TimeoutTerminator.h"
#include "ha3/isearch.h"
#include "ha3/search/Filter.h"
#include "ha3/search/FilterWrapper.h"
#include "ha3/search/JoinFilter.h"
#include "ha3/search/LayerMetas.h"
#include "ha3/search/MatchDataManager.h"
#include "ha3/search/QueryExecutor.h"
#include "indexlib/index/normal/attribute/accessor/join_docid_attribute_iterator.h"
#include "matchdoc/MatchDocAllocator.h"

namespace suez {
namespace turing {
class AttributeExpression;
}  // namespace turing
}  // namespace suez

using namespace isearch::common;
namespace isearch {
namespace search {
AUTIL_LOG_SETUP(ha3, SingleLayerSearcher);

SingleLayerSearcher::SingleLayerSearcher(
        QueryExecutor *queryExecutor, LayerMeta *layerMeta,
        FilterWrapper *filterWrapper,
        indexlib::index::DeletionMapReaderAdaptor *deletionMapReader,
        matchdoc::MatchDocAllocator *matchDocAllocator,
        TimeoutTerminator *timeoutTerminator,
        DocMapAttrIterator *main2subIt,
        indexlib::index::DeletionMapReader *subDeletionMapReader,
        MatchDataManager *matchDataManager,
        bool getAllSubDoc,
        const search::HashJoinInfo *hashJoinInfo,
        suez::turing::AttributeExpression *joinAttrExpr)
    : _curDocId((*layerMeta)[0].nextBegin)
    , _curBegin((*layerMeta)[0].nextBegin)
    , _curEnd((*layerMeta)[0].end)
    , _curQuota((*layerMeta)[0].quota)
    , _cousorNextBegin(_curDocId)
    , _rangeCousor(0)
    , _timeoutTerminator(timeoutTerminator)
    , _queryExecutor(queryExecutor)
    , _filterWrapper(filterWrapper)
    , _layerMeta(layerMeta)
    , _deletionMapReader(deletionMapReader)
    , _matchDocAllocator(matchDocAllocator)
    , _main2SubIt(main2subIt)
    , _subDeletionMapReader(subDeletionMapReader)
    , _matchDataManager(matchDataManager)
    , _getAllSubDoc(getAllSubDoc)
    , _seekTimes(0)
    , _hashJoinInfo(hashJoinInfo)
    , _joinAttrExpr(joinAttrExpr)
{
    if (layerMeta->quotaMode == QM_PER_LAYER) {
        _curQuota = layerMeta->maxQuota;
    } else {
        (*layerMeta)[0].quota = 0;
    }
}

SingleLayerSearcher::~SingleLayerSearcher() {
}

bool SingleLayerSearcher::moveToCorrectRange(docid_t &docId) {
    while (true) {
        // need move to next range.
        if (!moveToNextRange()) {
            if (!moveBack()) {
                docId = END_DOCID;
            } else {
                docId = _curBegin;
            }
            return false;
        }
        if (docId > _curEnd || _curQuota == 0) {
            continue;
        }
        // not in range.
        if (docId < _curBegin) {
            docId = _curBegin;
            return false;
        }
        // in range.
        _cousorNextBegin = docId + 1;
        return true;
    }
}

bool SingleLayerSearcher::moveToNextRange() {
    if (_curQuota > 0) {
        (*_layerMeta)[_rangeCousor].nextBegin = _curEnd + 1;
    } else {
        (*_layerMeta)[_rangeCousor].nextBegin = _cousorNextBegin;
    }
    while (++_rangeCousor < _layerMeta->size()) {
        if ((*_layerMeta)[_rangeCousor].nextBegin <= (*_layerMeta)[_rangeCousor].end) {
            _curQuota += (*_layerMeta)[_rangeCousor].quota;
            _curBegin = (*_layerMeta)[_rangeCousor].nextBegin;
            _curEnd = (*_layerMeta)[_rangeCousor].end;
            _cousorNextBegin = _curBegin;
            (*_layerMeta)[_rangeCousor].quota = 0;
            return true;
        }
    }

    return false;
}

bool SingleLayerSearcher::moveBack() {
    if (_layerMeta->quotaMode != QM_PER_DOC || _curQuota == 0) {
        return false;
    }
    for (size_t i = 0; i < _layerMeta->size(); ++i) {
        const DocIdRangeMeta &meta = (*_layerMeta)[i];
        if (meta.nextBegin <= meta.end) {
            _rangeCousor = i;
            _curBegin = (*_layerMeta)[_rangeCousor].nextBegin;
            _curEnd = (*_layerMeta)[_rangeCousor].end;
            _curDocId = _curBegin;
            _cousorNextBegin = _curBegin;
            _queryExecutor->reset();
            if (_filterWrapper) {
                _filterWrapper->resetFilter();
            }
            return true;
        }
    }
    return false;
}

indexlib::index::ErrorCode SingleLayerSearcher::constructSubMatchDocs(matchdoc::MatchDoc matchDoc) {
    auto docId = matchDoc.getDocId();
    auto mainToSubIter = _main2SubIt;
    indexlib::index::DeletionMapReader *deletionMapReader = _subDeletionMapReader;
    docid_t curSubDocId;
    if (docId != 0) {
        bool ret = mainToSubIter->Seek(docId - 1, curSubDocId);
        assert(ret); (void)ret;
    } else {
        curSubDocId = 0;
    }

    docid_t endSubDocId;
    bool ret = mainToSubIter->Seek(docId, endSubDocId);
    assert(ret); (void)ret;
    QueryExecutor *queryExecutor = _queryExecutor;
    bool hasSubDocExecutor = queryExecutor->hasSubDocExecutor();
    bool isMainDocHit = !hasSubDocExecutor;
    if(hasSubDocExecutor) {
        isMainDocHit = _queryExecutor->isMainDocHit(docId);
    }
    JoinFilter *joinFilter = _filterWrapper ? _filterWrapper->getJoinFilter() : NULL;
    Filter *filter = _filterWrapper ? _filterWrapper->getSubExprFilter() : NULL;

    matchdoc::MatchDoc lastDoc;
    // if filter need MatchData, fetch MatchData before filter
    bool needSubMatchData = _matchDataManager && _matchDataManager->hasSubMatchData();
    bool needSubMatchDataBeforeFilter = needSubMatchData && _matchDataManager->filterNeedMatchData();
    bool needSubMatchDataAfterFilter = needSubMatchData && !_matchDataManager->filterNeedMatchData();
    bool needSubSeek = !isMainDocHit || needSubMatchData;
    docid_t preSubDocId = curSubDocId;
    while (true) {
        if (needSubSeek) {
            auto ec = queryExecutor->seekSub(docId, curSubDocId, endSubDocId,
                    needSubMatchData, curSubDocId);
            IE_RETURN_CODE_IF_ERROR(ec);
        }
        if (unlikely(_getAllSubDoc)) {
            for (docid_t id = preSubDocId; id < std::min(curSubDocId, endSubDocId); id++) {
                if (!deletionMapReader || !deletionMapReader->IsDeleted(id)) {
                    auto &deleteSubDoc = _matchDocAllocator->allocateSubReturnRef(matchDoc, id);
                    deleteSubDoc.setDeleted();
                    lastDoc = deleteSubDoc;
                }
            }
        }
        if (curSubDocId >= endSubDocId) {
            break;
        }

        if (!deletionMapReader || !deletionMapReader->IsDeleted(curSubDocId)) {
            auto &newSubDoc =
                _matchDocAllocator->allocateSubReturnRef(matchDoc, curSubDocId);

            auto releaseSubDoc = [] (const matchdoc::MatchDocAllocator* allocator,
                    bool getAllSubDoc, matchdoc::MatchDoc& matchDoc,
                    matchdoc::MatchDoc& newSubDoc, matchdoc::MatchDoc& lastSubDoc) {
                if (unlikely(getAllSubDoc)) {
                    newSubDoc.setDeleted();
                } else {
                    allocator->deallocateSubDoc(newSubDoc);
                    allocator->truncateSubDoc(matchDoc, lastSubDoc);
                }
            };
            if (needSubMatchDataBeforeFilter) {
                auto ec = _matchDataManager->fillSubMatchData(matchDoc, newSubDoc,
                        curSubDocId, endSubDocId);
                if (unlikely(ec != indexlib::index::ErrorCode::OK)) {
                    releaseSubDoc(_matchDocAllocator, _getAllSubDoc, matchDoc, newSubDoc, lastDoc);
                    return ec;
                }
            }
            bool needReserve = !joinFilter || joinFilter->passSubDoc(matchDoc);
            if (needReserve && (!filter || filter->pass(matchDoc))) {
                if (needSubMatchDataAfterFilter) {
                    auto ec = _matchDataManager->fillSubMatchData(matchDoc, newSubDoc,
                            curSubDocId, endSubDocId);
                    if (unlikely(ec != indexlib::index::ErrorCode::OK)) {
                        releaseSubDoc(_matchDocAllocator, _getAllSubDoc, matchDoc, newSubDoc, lastDoc);
                        return ec;
                    }
                }
                lastDoc = newSubDoc;
            } else {
                releaseSubDoc(_matchDocAllocator, _getAllSubDoc, matchDoc, newSubDoc, lastDoc);
            }
        }
        curSubDocId++;
        preSubDocId = curSubDocId;
    }
    return indexlib::index::ErrorCode::OK;
}

} // namespace search
} // namespace isearch
