#include <ha3/rank/MultiHitCollector.h>

using namespace std;
using namespace suez::turing;

USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(rank);
HA3_LOG_SETUP(rank, MultiHitCollector);

MultiHitCollector::MultiHitCollector(autil::mem_pool::Pool *pool,
                                     const common::Ha3MatchDocAllocatorPtr &allocatorPtr,
                                     AttributeExpression *expr)
    : HitCollectorBase(expr, pool, allocatorPtr)
{
}

MultiHitCollector::~MultiHitCollector() {
    for (size_t i = 0; i < _hitCollectors.size(); ++i) {
        POOL_DELETE_CLASS(_hitCollectors[i]);
    }
}

matchdoc::MatchDoc MultiHitCollector::collectOneDoc(matchdoc::MatchDoc matchDoc) {
    auto retDoc = matchDoc;
    for (vector<HitCollectorBase*>::iterator it = _hitCollectors.begin();
         it != _hitCollectors.end(); ++it)
    {
        retDoc = (*it)->collectOneDoc(retDoc);
        if (retDoc == matchdoc::INVALID_MATCHDOC) {
             break;
        }
    }
    return retDoc;
}

void MultiHitCollector::doStealAllMatchDocs(autil::mem_pool::PoolVector<matchdoc::MatchDoc> &target) {
    for (vector<HitCollectorBase*>::iterator it = _hitCollectors.begin();
         it != _hitCollectors.end(); ++it)
    {
        (*it)->stealAllMatchDocs(target);
    }
}

uint32_t MultiHitCollector::doGetItemCount() const {
    uint32_t itemCount = 0;
    for (vector<HitCollectorBase *>::const_iterator it = _hitCollectors.begin();
         it != _hitCollectors.end(); ++it)
    {
        itemCount += (*it)->doGetItemCount();
    }
    return itemCount;
}

matchdoc::MatchDoc MultiHitCollector::top() const {
    assert(false);
    return matchdoc::INVALID_MATCHDOC;
}

END_HA3_NAMESPACE(rank);
