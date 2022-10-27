#include <ha3/search/FullMatchDataFetcher.h>

USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(rank);

BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, FullMatchDataFetcher);
using namespace matchdoc;

FullMatchDataFetcher::FullMatchDataFetcher()
    : _ref(NULL)
{
}

FullMatchDataFetcher::~FullMatchDataFetcher() {
}

ReferenceBase *FullMatchDataFetcher::require(
        Ha3MatchDocAllocator *allocator,
        const std::string &refName, uint32_t termCount)
{
    _ref = createReference<MatchData>(allocator, refName, termCount);
    _termCount = termCount;
    return _ref;
}

IE_NAMESPACE(common)::ErrorCode FullMatchDataFetcher::fillMatchData(
        const SingleLayerExecutors &singleLayerExecutors,
        MatchDoc matchDoc, MatchDoc subDoc) const
{
    docid_t docId = matchDoc.getDocId();
    MatchData &data = _ref->getReference(matchDoc);
    data.setMaxNumTerms(_termCount);
    for (uint32_t i = 0; i < _termCount; ++i) {
        TermMatchData &tmd = data.nextFreeMatchData();
        int32_t idx = (int32_t)i - (int32_t)_accTermCount;
        if (idx >= 0
            && (uint32_t)idx < singleLayerExecutors.size()
            && singleLayerExecutors[idx]
            && singleLayerExecutors[idx]->getDocId() == docId)
        {
            auto ec = singleLayerExecutors[idx]->unpackMatchData(tmd);
            IE_RETURN_CODE_IF_ERROR(ec);
        }
    }
    return IE_NAMESPACE(common)::ErrorCode::OK;
}

END_HA3_NAMESPACE(search);

