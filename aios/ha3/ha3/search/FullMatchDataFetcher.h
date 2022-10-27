#ifndef ISEARCH_FULLMATCHDATAFETCHER_H
#define ISEARCH_FULLMATCHDATAFETCHER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/MatchDataFetcher.h>
#include <ha3/rank/MatchData.h>

BEGIN_HA3_NAMESPACE(search);

class FullMatchDataFetcher : public MatchDataFetcher
{
public:
    FullMatchDataFetcher();
    ~FullMatchDataFetcher();
private:
    FullMatchDataFetcher(const FullMatchDataFetcher &);
    FullMatchDataFetcher& operator=(const FullMatchDataFetcher &);
public:
    matchdoc::ReferenceBase *require(common::Ha3MatchDocAllocator *allocator,
            const std::string &refName, uint32_t termCount) override;
    IE_NAMESPACE(common)::ErrorCode fillMatchData(const SingleLayerExecutors &singleLayerExecutors,
                       matchdoc::MatchDoc matchDoc,
                       matchdoc::MatchDoc subDoc) const override;
private:
    matchdoc::Reference<rank::MatchData> *_ref;
    friend class FullMatchDataFetcherTest;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(FullMatchDataFetcher);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_FULLMATCHDATAFETCHER_H
