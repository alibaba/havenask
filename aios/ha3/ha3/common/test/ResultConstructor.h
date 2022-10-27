#ifndef ISEARCH_RESULTCONSTRUCTOR_H
#define ISEARCH_RESULTCONSTRUCTOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/Result.h>
#include <autil/mem_pool/Pool.h>

BEGIN_HA3_NAMESPACE(common);

class ResultConstructor
{
public:
    ResultConstructor();
    ~ResultConstructor();
public:
    // use int32_t type for sort reference
    void fillMatchDocs(MatchDocs *matchDocs, uint32_t sortReferCount, 
                       uint32_t distReferCount, uint32_t totalMatchDocs, 
                       uint32_t actualMatchDocs,
                       autil::mem_pool::Pool* pool,
                       const std::string &value, bool sortFlag = false, 
                       const std::string &primaryKeyValues = "", 
                       FullIndexVersion version = 0,
                       bool isDistinctFiltered = false,
                       uint32_t extraDistReferCount = 0, 
                       uint32_t uniqKeyReferCount = 0,
                       uint32_t personalReferCount = 0);

    void prepareMatchDocs(MatchDocs *matchDocs, autil::mem_pool::Pool* pool,
                          uint8_t phaseOneInfoFlag, 
                          bool declareVector = false);

    // use float type for sort reference
    void fillMatchDocs2(MatchDocs *matchDocs, uint32_t sortReferCount, 
                        uint32_t distReferCount, uint32_t totalMatchDocs,
                        autil::mem_pool::Pool* pool,
                        const std::string &value);

    void fillAggregateResult(ResultPtr resultPtr, 
                             const std::string &funNames, 
                             const std::string &slabValue,
                             autil::mem_pool::Pool *pool,
                             const std::string &groupExprStr = "groupExprStr",
                             const std::string &funParameters = "");
    void fillHit(Hits *hits, hashid_t pid, 
                 uint32_t summaryCount, std::string summaryNames, 
                 std::string value);

    void fillPhaseOneSearchInfo(ResultPtr resultPtr, const std::string &infoStr);
    void fillPhaseTwoSearchInfo(ResultPtr resultPtr, const std::string &infoStr);

    void getSortReferences(matchdoc::MatchDocAllocator *allocator,
                           matchdoc::ReferenceVector &refVec);
    
private:
    void declareMatchDocsVariable(common::Ha3MatchDocAllocator *matchDocAllocator,
                                  uint8_t phaseOneInfoFlag, bool declareVector);
    void assignMatchDocsVariable(common::Ha3MatchDocAllocator *matchDocAllocator, 
                                 MatchDocs *matchDocs, bool declareVector,
                                 autil::mem_pool::Pool *pool);
private:
    uint32_t _sortRefCount;
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(common);

#endif //ISEARCH_RESULTCONSTRUCTOR_H
