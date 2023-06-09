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
#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include <functional>
#include <memory>

#include "autil/CommonMacros.h"
#include "autil/Log.h"
#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/PoolBase.h"
#include "autil/mem_pool/PoolVector.h"
#include "ha3/common/CommonDef.h"
#include "ha3/common/Ha3MatchDocAllocator.h"
#include "ha3/isearch.h"
#include "ha3/rank/ComboComparator.h"
#include "ha3/rank/HitCollectorBase.h"
#include "ha3/rank/ReferenceComparator.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/SubDocAccessor.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/framework/AttributeExpression.h"

using namespace suez::turing;

namespace isearch {
namespace rank {
AUTIL_LOG_SETUP(ha3, HitCollectorBase);

HitCollectorBase::HitCollectorBase(AttributeExpression *expr,
                                   autil::mem_pool::Pool *pool,
                                   const common::Ha3MatchDocAllocatorPtr &allocatorPtr)
    : _expr(expr)
    , _pool(pool)
    , _batchScore(true)
    , _cursor(0)
    , _matchDocBuffer((matchdoc::MatchDoc *)pool->allocate(
                    sizeof(matchdoc::MatchDoc) * BATCH_EVALUATE_SCORE_SIZE))
    , _deletedDocCount(0)
    , _matchDocAllocatorPtr(allocatorPtr)
    , _collectCount(0)
    , _lazyScore(false)
    , _unscoredMatchDocCount(0)
    , _maxUnscoredMatchDocCount(0)
    , _unscoredMatchDocs(NULL)
{
}

HitCollectorBase::~HitCollectorBase() {
    for (size_t i = 0; i < _unscoredMatchDocCount; ++i) {
        _matchDocAllocatorPtr->deallocate(
                _unscoredMatchDocs[i]);
    }
    for (size_t i = 0; i < _cursor; ++i) {
        _matchDocAllocatorPtr->deallocate(
                _matchDocBuffer[i]);
    }
}

void HitCollectorBase::enableLazyScore(size_t bufferSize) {
    assert(_unscoredMatchDocCount == 0);
    _maxUnscoredMatchDocCount = bufferSize;
    _lazyScore = true;
    _unscoredMatchDocs = (matchdoc::MatchDoc *)_pool->allocate(
            sizeof(matchdoc::MatchDoc) * bufferSize);
}

void HitCollectorBase::flush() {
    if (_cursor != 0) {
        doFlush();
    }
    matchdoc::MatchDoc *matchDocs = NULL;
    uint32_t count = flushBuffer(matchDocs);
    for (uint32_t i = 0; i < count; ++i) {
        _matchDocAllocatorPtr->deallocate(matchDocs[i]);
    }
}

void HitCollectorBase::evaluateAllMatchDoc(matchdoc::MatchDoc *matchDocs, uint32_t count) {
    if (_batchScore) {
        doBatchEvaluate(matchDocs, count);
    } else {
        for (uint32_t i = 0; i < count; ++i) {
            evaluateMatchDoc(matchDocs[i]);
        }
    }
    uint32_t leftCount = filterDeletedMatchDocs(matchDocs, count);
    doQuickInit(matchDocs, leftCount);
}

void HitCollectorBase::doQuickInit(matchdoc::MatchDoc *matchDocBuffer, uint32_t count) {
    doCollect(matchDocBuffer, count);
}

void HitCollectorBase::stealAllMatchDocs(autil::mem_pool::PoolVector<matchdoc::MatchDoc> &target) {
    assert(_cursor == 0);
    if (_lazyScore) {
        target.insert(target.end(), _unscoredMatchDocs,
                      _unscoredMatchDocs + _unscoredMatchDocCount);
        _unscoredMatchDocCount = 0;
    } else {
        assert(_unscoredMatchDocCount == 0);
        doStealAllMatchDocs(target);
    }
}

void HitCollectorBase::updateExprEvaluatedStatus() {
    if (_lazyScore) {
        return;
    }
    doUpdateExprEvaluatedStatus();
}

void HitCollectorBase::doUpdateExprEvaluatedStatus() {
    if (_expr) {
        _expr->setEvaluated();
    }
}

void HitCollectorBase::deallocateMatchDocs(matchdoc::MatchDoc *matchDocs, uint32_t count) {
    for (uint32_t i = 0; i < count; ++i) {
        _matchDocAllocatorPtr->deallocate(matchDocs[i]);
    }
}

uint32_t HitCollectorBase::filterDeletedMatchDocs(matchdoc::MatchDoc *matchDocs, uint32_t count) {
    uint32_t leftCount = 0;
    for (uint32_t i = 0; i < count; ++i) {
        auto matchDoc = matchDocs[i];
        if (matchDoc.isDeleted()) {
            _matchDocAllocatorPtr->deallocate(matchDoc);
            incDeletedCount();
            continue;
        }
        matchDocs[leftCount++] = matchDoc;
    }
    return leftCount;
}

void HitCollectorBase::doFlush() {
    matchdoc::MatchDoc *matchDocs = &_matchDocBuffer[0];
    if (_batchScore) {
        doBatchEvaluate(matchDocs, _cursor);
    }
    uint32_t leftCount = filterDeletedMatchDocs(matchDocs, _cursor);
    doCollect(matchDocs, leftCount);
    _cursor = 0;
}

void HitCollectorBase::addExtraDocIdentifierCmp(ComboComparator *comboCmp) {
    auto versionRef = _matchDocAllocatorPtr->findReference<FullIndexVersion>(
            common::FULLVERSION_REF);
    if (versionRef) {
        ReferenceComparator<FullIndexVersion>* cmp = POOL_NEW_CLASS(_pool,
                ReferenceComparator<FullIndexVersion>, versionRef, false);
        comboCmp->setExtrDocIdComparator(cmp);
    }
    auto hashIdRef = _matchDocAllocatorPtr->findReference<hashid_t>(
            common::HASH_ID_REF);
    if (hashIdRef) {
        ReferenceComparator<hashid_t>* cmp = POOL_NEW_CLASS(_pool,
                ReferenceComparator<hashid_t>, hashIdRef, false);
        comboCmp->setExtrHashIdComparator(cmp);
    }
    auto clusterIdRef = _matchDocAllocatorPtr->findReference<clusterid_t>(
            common::CLUSTER_ID_REF);
    if (clusterIdRef) {
        ReferenceComparator<clusterid_t>* cmp = POOL_NEW_CLASS(_pool,
                ReferenceComparator<clusterid_t>, clusterIdRef, false);
        comboCmp->setExtrClusterIdComparator(cmp);
    }
}

uint32_t HitCollectorBase::getItemCount() const {
    if (!_lazyScore) {
        return doGetItemCount();
    }
    return _unscoredMatchDocCount;
}

uint32_t HitCollectorBase::collectAndReplace(matchdoc::MatchDoc *matchDocs,
        uint32_t count, matchdoc::MatchDoc *&retDocs)
{
    uint32_t replacedCount = 0;
    retDocs = matchDocs;
    for (uint32_t i = 0; i < count; ++i) {
        auto matchDoc = collectOneDoc(matchDocs[i]);
        if (matchDoc != matchdoc::INVALID_MATCHDOC) {
            retDocs[replacedCount++] = matchDoc;
        }
    }
    return replacedCount;
}

void HitCollectorBase::collectOneMatchDoc(matchdoc::MatchDoc matchDoc) {
    ++_collectCount;
    if (_lazyScore) {
        if (_unscoredMatchDocCount < _maxUnscoredMatchDocCount) {
            _unscoredMatchDocs[_unscoredMatchDocCount++] = matchDoc;
            return;
        }
        evaluateAllMatchDoc(_unscoredMatchDocs, _unscoredMatchDocCount);
        _unscoredMatchDocCount = 0;
        _lazyScore = false;
    }

    if (!_batchScore) {
        evaluateMatchDoc(matchDoc);
    }
    _matchDocBuffer[_cursor++] = matchDoc;
    if (unlikely(_cursor == BATCH_EVALUATE_SCORE_SIZE)) {
        doFlush();
    }
}

void HitCollectorBase::flattenCollectMatchDoc(matchdoc::MatchDoc matchDoc) {
    auto accessor = _matchDocAllocatorPtr->getSubDocAccessor();
    auto processor = std::bind(&HitCollectorBase::collectDoc,
                                    this, std::placeholders::_1);
    accessor->foreachFlatten(matchDoc, _matchDocAllocatorPtr.get(), processor);
}

void HitCollectorBase::collectDoc(matchdoc::MatchDoc matchDoc) {
    collectOneMatchDoc(matchDoc);
}

} // namespace rank
} // namespace isearch
