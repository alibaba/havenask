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
#include "ha3/rank/DistinctHitCollector.h"

#include <assert.h>
#include <algorithm>
#include <cstddef>

#include "autil/Log.h"
#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/PoolBase.h"
#include "autil/mem_pool/PoolVector.h"
#include "ha3/common/AttributeItem.h"
#include "ha3/isearch.h"
#include "ha3/rank/AttributeExpressionConvertor.h"
#include "ha3/rank/ComboComparator.h"
#include "ha3/rank/Comparator.h"
#include "ha3/rank/ComparatorCreator.h"
#include "ha3/rank/DistinctBoostScorer.h"
#include "ha3/rank/DistinctComparator.h"
#include "ha3/rank/DistinctInfo.h"
#include "ha3/rank/DistinctMap.h"
#include "ha3/rank/DistinctPriorityQueue.h"
#include "ha3/rank/GradeCalculator.h"
#include "ha3/rank/GradeComparator.h"
#include "ha3/rank/MatchDocPriorityQueue.h"
#include "ha3/rank/SimpleDistinctBoostScorer.h"
#include "ha3/search/Filter.h"
#include "ha3/search/SortExpression.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/framework/AttributeExpression.h"

using namespace std;
using namespace suez::turing;
using namespace isearch::search;
using namespace isearch::common;

namespace isearch {
namespace rank {
AUTIL_LOG_SETUP(ha3, DistinctHitCollector);

DistinctHitCollector::DistinctHitCollector(uint32_t size,
        autil::mem_pool::Pool *pool,
        common::Ha3MatchDocAllocatorPtr allocatorPtr,
        AttributeExpression *expr,
        ComboComparator *cmp,
        const DistinctParameter &dp,
        bool sortFlagForGrade)
    : HitCollectorBase(expr, pool, allocatorPtr)
    , _keyExpr(dp.keyExpression->getAttributeExpression())
    , _distinctTimes(dp.distinctTimes)
    , _distinctCount(dp.distinctCount)
    , _reservedFlag(dp.reservedFlag)
    , _updateTotalHitFlag(dp.updateTotalHitFlag)
    , _sortFlagForGrade(sortFlagForGrade)
    , _distinctInfoRef(dp.distinctInfoRef)
    , _filter(dp.distinctFilter)
    , _gradeThresholds(dp.gradeThresholds)
{
    uint32_t actualSize = max(size, dp.maxItemCount);
    _queue = POOL_NEW_CLASS(pool, DistinctPriorityQueue, actualSize,
                            pool, cmp, dp.distinctInfoRef);
    _distinctBoostScorer = NULL;
    _rawComparator = NULL;
    _gradeComparator = NULL;
    _gradeCalculator = NULL;
    assert(dp.keyExpression);
    _keyComparator = ComparatorCreator::createComparator(pool, dp.keyExpression);
    _distinctMap = new DistinctMap(_keyComparator, actualSize);
    setupComparators(cmp, POOL_NEW_CLASS(pool, DistinctComparator, _distinctInfoRef));
    addExtraDocIdentifierCmp(&_comboComparator);
}

DistinctHitCollector::~DistinctHitCollector() {
    delete _distinctBoostScorer;
    delete _distinctMap;
    POOL_DELETE_CLASS(_filter);
    delete _gradeCalculator;
    POOL_DELETE_CLASS(_keyComparator);
}

void DistinctHitCollector::setupComparators(ComboComparator *cmp,
        Comparator *distinctComparator)
{
    if (_gradeThresholds.size() != 0) {
        _gradeComparator = POOL_NEW_CLASS(_pool, GradeComparator,
                _distinctInfoRef, _sortFlagForGrade);
        _comboComparator.addComparator(_gradeComparator);

        _gradeCalculator = AttributeExpressionConvertor::convert<GradeCalculatorBase, GradeCalculator>(_expr);
        if (_gradeCalculator) {
            _gradeCalculator->setGradeThresholds(_gradeThresholds);
        }
    }
    _comboComparator.addComparator(distinctComparator);
    _rawComparator = cmp;//get comparator from base class
    _comboComparator.addComparator(_rawComparator);
    _queue->setComparator(&_comboComparator);
    _distinctBoostScorer
        = new SimpleDistinctBoostScorer(_distinctTimes, _distinctCount);
}

matchdoc::MatchDoc DistinctHitCollector::collectOneDoc(matchdoc::MatchDoc matchDoc) {
    DistinctInfo &distInfo
        = _distinctInfoRef->getReference(matchDoc);
    distInfo.reset(matchDoc);

    if (_gradeCalculator) {
        _gradeCalculator->calculate(&distInfo);
    }

    if (canDrop(distInfo, matchDoc)) {
        return matchDoc;
    }

    // NOTE: not agreed with ha3 documentation, leave it for now for users' compatibility
    if (_filter) {
        if (_filter->pass(matchDoc)) {
            recalculateDistinctBoost(&distInfo, (uint32_t)0);
            return pushQueue(matchDoc);
        }
    }

    _keyExpr->evaluate(matchDoc);

    _distinctMap->insert(&distInfo);

    TreeNode *tn = distInfo.getTreeNode();//item is _sdi of TreeNode now.
    DistinctInfo *distinctInfo = adjustDistinctInfosInList(tn);

    if (!_reservedFlag && distinctInfo) {
        return replaceElement(distinctInfo, matchDoc);
    } else {
        return pushQueue(matchDoc);
    }
}

DistinctInfo* DistinctHitCollector::adjustDistinctInfosInList(TreeNode *tn) {
    DistinctInfo *item = tn->_sdi;
    DistinctInfo *p1 = tn->_sdi;
    DistinctInfo *p2 = p1->_next;
    uint32_t groupLimit = _distinctCount * _distinctTimes;
    DistinctInfo *removedDistInfo = NULL;
    uint32_t keyPosInGrade = 0;

    while (p2) {
        uint32_t pos = p2->getKeyPosition();
        if (p2->getGradeBoost() == item->getGradeBoost()) {
            bool ret = _rawComparator->compare(p2->getMatchDoc(),
                    item->getMatchDoc());
            if (ret) {
                if (pos < groupLimit - 1) {
                    recalculateDistinctBoost(p2, pos + 1);
                    adjustQueue(p2);
                } else {
                    p1->_next = p2->_next;
                    removedDistInfo = p2;
                    p2 = p1->_next;
                    continue;
                }
            } else {
                keyPosInGrade = pos + 1;
                break;
            }
        } else {
            assert(_gradeComparator);
            bool ret = _gradeComparator->compare(item->getMatchDoc(),
                    p2->getMatchDoc());
            if (ret) {
                keyPosInGrade = 0;
                break;
            } else {
                // skip to the next grade
                for (uint32_t i = 0; i < pos; i++) {
                    p1 = p2;
                    p2 = p1->_next;
                }
            }
        }
        p1 = p2;
        p2 = p1->_next;
    }

    if (keyPosInGrade >= groupLimit) {
        removedDistInfo = item;
        tn->_sdi = item->_next;
    } else {
        if (p1 != item) {
            tn->_sdi = item->_next;
            item->_next = p2;
            p1->_next = item;
            assert(tn->_sdi);
        }
        recalculateDistinctBoost(item, keyPosInGrade);
    }

    if (removedDistInfo) {
        removedDistInfo->setTreeNode(NULL);
        removedDistInfo->setDistinctBoost(0);
        if (item != removedDistInfo) {
            adjustQueue(removedDistInfo);
        }
    }

    return removedDistInfo;
}

void DistinctHitCollector::recalculateDistinctBoost(DistinctInfo *item,
        uint32_t newPosition)
{
    item->setKeyPosition(newPosition);
    score_t distinctBoost = _distinctBoostScorer->calculateBoost(
            newPosition, 0.0f);
    item->setDistinctBoost(distinctBoost);
}

matchdoc::MatchDoc DistinctHitCollector::pushQueue(matchdoc::MatchDoc matchDoc) {
    auto retDoc = matchdoc::INVALID_MATCHDOC;
    MatchDocPriorityQueue::PUSH_RETURN_CODE ret;
    ret = _queue->push(matchDoc, &retDoc);
    if (ret != MatchDocPriorityQueue::ITEM_ACCEPTED) {
        DistinctInfo * info = _distinctInfoRef->getPointer(
                retDoc);
        _distinctMap->removeFirstDistinctInfo(info);
    }
    return retDoc;
}

void DistinctHitCollector::adjustQueue(DistinctInfo *item) {
    uint32_t idx = item->getQueuePosition();
    _queue->adjustUp(idx);
}

uint32_t DistinctHitCollector::getQueuePosition(matchdoc::MatchDoc matchDoc) const {
    DistinctInfo *distInfo =
        _distinctInfoRef->getPointer(matchDoc);
    return distInfo->getQueuePosition();
}

matchdoc::MatchDoc DistinctHitCollector::replaceElement(DistinctInfo *distinctInfo,
        matchdoc::MatchDoc matchDoc)
{
    auto toDeleteMatchDoc = matchDoc;
    if (distinctInfo->getMatchDoc() != matchDoc) {
        uint32_t queuePosition = distinctInfo->getQueuePosition();
        assert(queuePosition >= 1 && queuePosition <= _queue->count());
        setQueuePosition(matchDoc, queuePosition);
        toDeleteMatchDoc = _queue->item(queuePosition);
        _queue->item(queuePosition) = matchDoc;
        _queue->adjustDown(queuePosition);
    }
    if (_updateTotalHitFlag) {
        incDeletedCount();
    }
    assert(matchdoc::INVALID_MATCHDOC != toDeleteMatchDoc);
    return toDeleteMatchDoc;
}

matchdoc::MatchDoc DistinctHitCollector::popItem() {
    return _queue->pop();
}

void DistinctHitCollector::doUpdateExprEvaluatedStatus() {
    HitCollectorBase::doUpdateExprEvaluatedStatus();
    _keyExpr->setEvaluated();
}

void DistinctHitCollector::doStealAllMatchDocs(
        autil::mem_pool::PoolVector<matchdoc::MatchDoc> &target)
{
    auto matchDocs = _queue->getAllMatchDocs();
    uint32_t c = _queue->count();
    target.insert(target.end(), matchDocs, matchDocs + c);
    _queue->reset();
}

} // namespace rank
} // namespace isearch
