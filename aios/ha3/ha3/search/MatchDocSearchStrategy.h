#ifndef ISEARCH_MATCHDOCSEARCHSTRATEGY_H
#define ISEARCH_MATCHDOCSEARCHSTRATEGY_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/search/HitCollectorManager.h>
#include <ha3/search/MatchDocSearcherProcessorResource.h>
#include <ha3/search/InnerSearchResult.h>
#include <ha3/common/MatchDocs.h>
#include <ha3/common/Result.h>
#include <autil/mem_pool/Pool.h>
#include <autil/mem_pool/PoolVector.h>

BEGIN_HA3_NAMESPACE(search);

class MatchDocSearchStrategy
{
public:
    MatchDocSearchStrategy(const DocCountLimits &docCountLimits)
        : _docCountLimits(docCountLimits) { }
    virtual ~MatchDocSearchStrategy() { }
private:
    MatchDocSearchStrategy(const MatchDocSearchStrategy &);
    MatchDocSearchStrategy& operator=(const MatchDocSearchStrategy &);
public:
    virtual void afterSeek(HitCollectorManager * ) { }
    virtual void afterStealFromCollector(InnerSearchResult&, HitCollectorManager * ) { }
    virtual void afterRerank(InnerSearchResult& ) { }
    virtual void afterExtraRank(InnerSearchResult& ) { }
    virtual common::Result* reconstructResult(common::Result *result) {
        return result;
    }
    virtual uint32_t getExtraRankCount() {
        return _docCountLimits.requiredTopK;
    }
    virtual uint32_t getResultCount() {
        return _docCountLimits.requiredTopK;
    }
public:
    template <typename T>
    static void releaseRedundantMatchDocs(T &matchDocVect,
            uint32_t requiredTopK, const common::Ha3MatchDocAllocatorPtr &matchDocAllocator)
    {
        uint32_t size = (uint32_t)matchDocVect.size();
        while(size > requiredTopK)
        {
            auto matchDoc = matchDocVect[--size];
            matchDocAllocator->deallocate(matchDoc);
        }
        matchDocVect.erase(matchDocVect.begin() + size, matchDocVect.end());
    }
protected:
    static void truncateResult(uint32_t requireTopK,
                               common::Result *result) {
        common::MatchDocs *matchDocs = result->getMatchDocs();
        std::vector<matchdoc::MatchDoc> &matchDocVect =
            matchDocs->getMatchDocsVect();
        const auto &matchDocAllocator = matchDocs->getMatchDocAllocator();
        releaseRedundantMatchDocs(matchDocVect,
                requireTopK, matchDocAllocator);
    }
protected:
    const DocCountLimits& _docCountLimits;
private:
    friend class MatchDocSearchStrategyTest;
};

END_HA3_NAMESPACE(search);

#endif //ISEARCH_MATCHDOCSEARCHSTRATEGY_H
