#include <ha3/rank/HitCollector.h>
#include <assert.h>
#include <suez/turing/expression/framework/AttributeExpression.h>
#include <ha3/rank/ComboComparator.h>
#include <ha3/rank/ReferenceComparator.h>
#include <suez/turing/expression/framework/ComboAttributeExpression.h>

using namespace std;
using namespace suez::turing;

USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(rank);
HA3_LOG_SETUP(rank, HitCollector);


HitCollector::HitCollector(uint32_t size,
                           autil::mem_pool::Pool *pool,
                           const common::Ha3MatchDocAllocatorPtr &allocatorPtr,
                           AttributeExpression *expr,
                           ComboComparator *cmp)
    : HitCollectorBase(expr, pool, allocatorPtr)
{
    _queue = POOL_NEW_CLASS(pool, MatchDocPriorityQueue,
                            size, pool, cmp);
    _cmp = cmp;
    addExtraDocIdentifierCmp(_cmp);
}

HitCollector::~HitCollector() {
    uint32_t count = _queue->count();
    auto leftMatchDocs = _queue->getAllMatchDocs();
    for (uint32_t i = 0; i < count; ++i) {
        _matchDocAllocatorPtr->deallocate(leftMatchDocs[i]);
    }
    POOL_DELETE_CLASS(_queue);
    POOL_DELETE_CLASS(_cmp);
}

matchdoc::MatchDoc HitCollector::collectOneDoc(matchdoc::MatchDoc matchDoc) {
    auto retDoc = matchdoc::INVALID_MATCHDOC;
    _queue->push(matchDoc, &retDoc);
    return retDoc;
}

void HitCollector::doQuickInit(matchdoc::MatchDoc *matchDocs, uint32_t count) {
    _queue->quickInit(matchDocs, count);
}

void HitCollector::doStealAllMatchDocs(autil::mem_pool::PoolVector<matchdoc::MatchDoc> &target) {
    auto matchDocs = _queue->getAllMatchDocs();
    uint32_t c = _queue->count();
    target.insert(target.end(), matchDocs, matchDocs + c);
    _queue->reset();
}

END_HA3_NAMESPACE(rank);

