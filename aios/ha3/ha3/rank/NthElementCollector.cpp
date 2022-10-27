#include <ha3/rank/NthElementCollector.h>
#include <assert.h>
#include <suez/turing/expression/framework/AttributeExpression.h>
#include <suez/turing/expression/framework/ComboAttributeExpression.h>

using namespace std;
using namespace suez::turing;

USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(rank);
HA3_LOG_SETUP(rank, NthElementCollector);


NthElementCollector::NthElementCollector(uint32_t size,
        autil::mem_pool::Pool *pool,
        const common::Ha3MatchDocAllocatorPtr &allocatorPtr,
        AttributeExpression *expr,
        ComboComparator *cmp)
    : HitCollectorBase(expr, pool, allocatorPtr)
    , _size(size)
    , _maxBufferSize(size * 2)
    , _matchDocCount(0)
    , _minMatchDoc(matchdoc::INVALID_MATCHDOC)
    , _cmp(cmp)
{
    addExtraDocIdentifierCmp(_cmp);
    // need BATCH_EVALUATE_SCORE_SIZE more buffer to replace memory
    _matchDocBuffer = (matchdoc::MatchDoc *)pool->allocate(
            (_maxBufferSize + BATCH_EVALUATE_SCORE_SIZE) * sizeof(matchdoc::MatchDoc));
}

NthElementCollector::~NthElementCollector() {
    for (uint32_t i = 0; i < _matchDocCount; ++i) {
        _matchDocAllocatorPtr->deallocate(_matchDocBuffer[i]);
    }
    POOL_DELETE_CLASS(_cmp);
    _pool->deallocate(_matchDocBuffer, (_maxBufferSize + BATCH_EVALUATE_SCORE_SIZE) * sizeof(matchdoc::MatchDoc));
}

void NthElementCollector::doQuickInit(matchdoc::MatchDoc *matchDocs, uint32_t count) {
    assert(count <= _maxBufferSize);
    memcpy(_matchDocBuffer, matchDocs, count * sizeof(matchdoc::MatchDoc));
    _matchDocCount = count;
    // make sure minMatchDoc exist only when at least _size MatchDocs
    if(count >= _size){
        _minMatchDoc = findMinMatchDoc(matchDocs, count);
    }
}

matchdoc::MatchDoc NthElementCollector::findMinMatchDoc(
        matchdoc::MatchDoc *matchDocs, uint32_t count)
{
    if (count == 0) {
        return matchdoc::INVALID_MATCHDOC;
    }
    matchdoc::MatchDoc minMatchDoc = matchDocs[0];
    for (size_t i = 1; i < count; ++i) {
        if (_cmp->compare(matchDocs[i], minMatchDoc)) {
            minMatchDoc = matchDocs[i];
        }
    }
    return minMatchDoc;
}

void NthElementCollector::doStealAllMatchDocs(
        autil::mem_pool::PoolVector<matchdoc::MatchDoc> &target)
{
    assert(_matchDocCount <= _size);
    target.insert(target.end(), _matchDocBuffer, _matchDocBuffer + _matchDocCount);
    _matchDocCount = 0;
}

uint32_t NthElementCollector::collectAndReplace(matchdoc::MatchDoc *matchDocs,
        uint32_t count, matchdoc::MatchDoc *&retDocs)
{
    uint32_t filtedByMinScore = 0;
    for (uint32_t i = 0; i < count; ++i) {
        auto matchDoc = matchDocs[i];
        if ((matchdoc::INVALID_MATCHDOC != _minMatchDoc)
            && _matchDocCount >= _size && _cmp->compare(matchDoc, _minMatchDoc))
        {
            matchDocs[filtedByMinScore++] = matchDoc;
            continue;
        }
        assert(_matchDocCount + filtedByMinScore <= _maxBufferSize + BATCH_EVALUATE_SCORE_SIZE);
        _matchDocBuffer[_matchDocCount++] = matchDoc;
    }
    if (_matchDocCount < _maxBufferSize) {
        retDocs = matchDocs;
        return filtedByMinScore;
    }
    uint32_t replacedMatchDocCount = _matchDocCount - _size;
    doNthElement();
    retDocs = _matchDocBuffer + _size;
    if (filtedByMinScore > 0) {
        // copy filted document to buffer end.
        // now replaced matchDoc is _matchDocBuffer[_size - 1] to
        // _matchDocBuffer[_matchDocCount + filtedByMinScore]
        memcpy(_matchDocBuffer + _matchDocCount, // end
               matchDocs, filtedByMinScore * sizeof(matchdoc::MatchDoc));
        replacedMatchDocCount += filtedByMinScore;
    }
    _matchDocCount = _size;
    return replacedMatchDocCount;
}

void NthElementCollector::doNthElement() {
    MatchDocComp comp(_cmp);
    nth_element(_matchDocBuffer, // start 
                _matchDocBuffer + _size - 1, // mid
                _matchDocBuffer + _matchDocCount, //end
                comp);
    _minMatchDoc = _matchDocBuffer[_size - 1];
}

uint32_t NthElementCollector::flushBuffer(matchdoc::MatchDoc *&retDocs) {
    if (_matchDocCount <= _size) {
        if (matchdoc::INVALID_MATCHDOC == _minMatchDoc) {
            _minMatchDoc = findMinMatchDoc(_matchDocBuffer, _matchDocCount);
        }
        return 0;
    }
    doNthElement();
    retDocs = _matchDocBuffer + _size;
    uint32_t replacedCount = _matchDocCount - _size;
    _matchDocCount = _size;
    return replacedCount;
}

END_HA3_NAMESPACE(rank);

