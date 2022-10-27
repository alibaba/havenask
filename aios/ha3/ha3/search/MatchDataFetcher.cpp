#include <ha3/search/MatchDataFetcher.h>
#include <ha3/search/SubTermQueryExecutor.h>

BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, MatchDataFetcher);

MatchDataFetcher::MatchDataFetcher()
    : _termCount (0)
    , _accTermCount(0) {
}

MatchDataFetcher::~MatchDataFetcher() { 
}

END_HA3_NAMESPACE(search);

