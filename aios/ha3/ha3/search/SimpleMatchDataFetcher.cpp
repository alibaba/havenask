#include <ha3/search/SimpleMatchDataFetcher.h>

USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, SimpleMatchDataFetcher);
using namespace matchdoc;

SimpleMatchDataFetcher::SimpleMatchDataFetcher()
    : _ref(NULL)
{
}

SimpleMatchDataFetcher::~SimpleMatchDataFetcher() {
}

ReferenceBase *SimpleMatchDataFetcher::require(
        Ha3MatchDocAllocator *allocator,
        const std::string &refName, uint32_t termCount)
{
    _ref = createReference<rank::SimpleMatchData>(
            allocator, refName, termCount);
    _termCount = termCount;
    return _ref;
}

IE_NAMESPACE(common)::ErrorCode SimpleMatchDataFetcher::fillMatchData(
        const SingleLayerExecutors &singleLayerExecutors,
        MatchDoc matchDoc, MatchDoc subDoc) const
{
    docid_t docId = matchDoc.getDocId();
    rank::SimpleMatchData &data = _ref->getReference(matchDoc);
    for (uint32_t i = 0; i < _termCount; ++i) {
        int32_t idx = (int32_t)i - (int32_t)_accTermCount;
        bool isMatch = idx >= 0
                       && (uint32_t)idx < singleLayerExecutors.size()
                       && singleLayerExecutors[idx]
                       && singleLayerExecutors[idx]->getDocId() == docId;
        data.setMatch(i, isMatch);
    }
    return IE_NAMESPACE(common)::ErrorCode::OK;
}

END_HA3_NAMESPACE(search);

