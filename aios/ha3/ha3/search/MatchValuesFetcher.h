#ifndef ISEARCH_MATCHVALUESFETCHER_H
#define ISEARCH_MATCHVALUESFETCHER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/rank/MatchValues.h>
#include <matchdoc/MatchDoc.h>
#include <ha3/common/Ha3MatchDocAllocator.h>
#include <ha3/search/TermQueryExecutor.h>
#include <ha3/common/CommonDef.h>

BEGIN_HA3_NAMESPACE(search);

class MatchValuesFetcher
{
public:
    MatchValuesFetcher();
    ~MatchValuesFetcher();
private:
    MatchValuesFetcher(const MatchValuesFetcher &);
    MatchValuesFetcher& operator=(const MatchValuesFetcher &);
public:
    void setAccTermCount(uint32_t accTermCount) {
        _accTermCount = accTermCount;
    }
    matchdoc::ReferenceBase *require(common::Ha3MatchDocAllocator *allocator,
            const std::string &refName, uint32_t termCount);
    matchdoc::Reference<rank::MatchValues> *createReference(
            common::Ha3MatchDocAllocator *allocator,
            const std::string &refName, uint32_t termCount);
    IE_NAMESPACE(common)::ErrorCode fillMatchValues(
            const SingleLayerExecutors &singleLayerExecutors,
            matchdoc::MatchDoc);
private:
    matchdoc::Reference<rank::MatchValues> *_ref;
    uint32_t _termCount;
    uint32_t _accTermCount;
private:
    friend class MatchValuesFetcherTest;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(MatchValuesFetcher);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_MATCHVALUESFETCHER_H
