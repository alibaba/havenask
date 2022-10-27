#include <ha3/search/SubSimpleMatchDataFetcher.h>

using namespace std;
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(rank);

BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, SubSimpleMatchDataFetcher);
using namespace matchdoc;

SubSimpleMatchDataFetcher::SubSimpleMatchDataFetcher()
    : _ref(NULL)
{
}

SubSimpleMatchDataFetcher::~SubSimpleMatchDataFetcher() {
}

ReferenceBase *SubSimpleMatchDataFetcher::require(
        Ha3MatchDocAllocator *allocator,
        const std::string &refName, uint32_t termCount)
{
    _ref = createSubReference<SimpleMatchData>(allocator, refName, termCount);
    _termCount = termCount;
    return _ref;
}

IE_NAMESPACE(common)::ErrorCode SubSimpleMatchDataFetcher::fillMatchData(
        const SingleLayerExecutors &singleLayerExecutors,
        MatchDoc matchDoc, MatchDoc subDoc) const
{
    docid_t docId = matchDoc.getDocId();
    docid_t subDocId = subDoc.getDocId();
    rank::SimpleMatchData &data = _ref->getReference(matchDoc);
    for (uint32_t i = 0; i < _termCount; ++i) {
        int32_t idx = (int32_t)i - (int32_t)_accTermCount;
        bool isMatch = idx >= 0
                       && ((uint32_t)idx < singleLayerExecutors.size())
                       && singleLayerExecutors[idx]
                       && singleLayerExecutors[idx]->getDocId() == docId
                       && singleLayerExecutors[idx]->getSubDocId() == subDocId;
        data.setMatch(i, isMatch);
    }
    return IE_NAMESPACE(common)::ErrorCode::OK;
}

END_HA3_NAMESPACE(search);
