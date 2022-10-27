#ifndef ISEARCH_INNERSEARCHRESULT_H
#define ISEARCH_INNERSEARCHRESULT_H

#include <ha3/common/AggregateResult.h>
#include <ha3/common/Ha3MatchDocAllocator.h>
#include <autil/mem_pool/PoolVector.h>

BEGIN_HA3_NAMESPACE(search);

struct InnerSearchResult{
    InnerSearchResult(autil::mem_pool::Pool *pool) 
        : matchDocVec(pool) 
        , totalMatchDocs(0)
        , actualMatchDocs(0)
        , extraRankCount(0)
    {
    }
    autil::mem_pool::PoolVector<matchdoc::MatchDoc> matchDocVec;
    common::Ha3MatchDocAllocatorPtr matchDocAllocatorPtr;
    uint32_t totalMatchDocs;
    uint32_t actualMatchDocs;
    uint32_t extraRankCount;
    common::AggregateResultsPtr aggResultsPtr;
};

END_HA3_NAMESPACE(search);

#endif //ISEARCH_INNERSEARCHRESULT_H
