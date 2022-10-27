#ifndef ISEARCH_MATCHDATAFETCHER_H
#define ISEARCH_MATCHDATAFETCHER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <matchdoc/MatchDoc.h>
#include <indexlib/common/error_code.h>
#include <ha3/search/TermQueryExecutor.h>
#include <ha3/common/Ha3MatchDocAllocator.h>
#include <ha3/common/CommonDef.h>

BEGIN_HA3_NAMESPACE(search);

class MatchDataFetcher
{
public:
    MatchDataFetcher();
    virtual ~MatchDataFetcher();
private:
    MatchDataFetcher(const MatchDataFetcher &);
    MatchDataFetcher& operator=(const MatchDataFetcher &);
public:
    virtual matchdoc::ReferenceBase *require(common::Ha3MatchDocAllocator *allocator,
            const std::string &refName, uint32_t termCount) = 0;

    virtual IE_NAMESPACE(common)::ErrorCode fillMatchData(const SingleLayerExecutors &singleLayerExecutors,
                                                          matchdoc::MatchDoc matchDoc,
                                                          matchdoc::MatchDoc subDoc) const = 0;
public:
    void setAccTermCount(uint32_t accTermCount) {
        _accTermCount = accTermCount;
    }
protected:
    template<typename MatchDataType>
    matchdoc::Reference<MatchDataType> *createReference(
            common::Ha3MatchDocAllocator *allocator,
            const std::string &refName, uint32_t termCount)
    {
        return allocator->declare<MatchDataType>(
                refName, common::HA3_MATCHDATA_GROUP, SL_NONE,
                MatchDataType::sizeOf(termCount));
    }
    template<typename MatchDataType>
    matchdoc::Reference<MatchDataType> *createSubReference(
            common::Ha3MatchDocAllocator *allocator,
            const std::string &refName, uint32_t termCount)
    {
        return allocator->declareSub<MatchDataType>(
                refName, common::HA3_SUBMATCHDATA_GROUP, SL_NONE,
                MatchDataType::sizeOf(termCount));
    }
protected:
    uint32_t _termCount;
    uint32_t _accTermCount;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(MatchDataFetcher);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_MATCHDATAFETCHER_H
