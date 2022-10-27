#ifndef ISEARCH_SIMPLEMATCHDATAFETCHER_H
#define ISEARCH_SIMPLEMATCHDATAFETCHER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/MatchDataFetcher.h>
#include <ha3/rank/SimpleMatchData.h>

BEGIN_HA3_NAMESPACE(search);

class SimpleMatchDataFetcher : public MatchDataFetcher
{
public:
    SimpleMatchDataFetcher();
    ~SimpleMatchDataFetcher();
private:
    SimpleMatchDataFetcher(const SimpleMatchDataFetcher &);
    SimpleMatchDataFetcher& operator=(const SimpleMatchDataFetcher &);
public:
    matchdoc::ReferenceBase *require(common::Ha3MatchDocAllocator *allocator,
            const std::string &refName, uint32_t termCount) override;
    IE_NAMESPACE(common)::ErrorCode fillMatchData(const SingleLayerExecutors &singleLayerExecutors,
                                                  matchdoc::MatchDoc matchDoc,
                                                  matchdoc::MatchDoc subDoc) const override;
private:
    matchdoc::Reference<rank::SimpleMatchData> *_ref;
private:
    friend class SimpleMatchDataFetcherTest;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(SimpleMatchDataFetcher);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_SIMPLEMATCHDATAFETCHER_H
