#include <ha3/search/MatchValuesFetcher.h>

USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(rank);

BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, MatchValuesFetcher);
using namespace matchdoc;

MatchValuesFetcher::MatchValuesFetcher()
    : _ref(NULL)
    , _termCount(0)
    , _accTermCount(0)
{
}

MatchValuesFetcher::~MatchValuesFetcher() {
}

ReferenceBase *MatchValuesFetcher::require(
        Ha3MatchDocAllocator *allocator,
        const std::string &refName, uint32_t termCount)
{
    _ref = createReference(
            allocator, refName, termCount);
    _termCount = termCount;
    return _ref;
}

matchdoc::Reference<Ha3MatchValues> *MatchValuesFetcher::createReference(
            common::Ha3MatchDocAllocator *allocator,
            const std::string &refName, uint32_t termCount)
{
    return allocator->declare<Ha3MatchValues>(
            refName, HA3_NS(common)::HA3_MATCHVALUE_GROUP, SL_NONE,
            Ha3MatchValues::sizeOf(termCount));
}

IE_NAMESPACE(common)::ErrorCode MatchValuesFetcher::fillMatchValues(
        const SingleLayerExecutors &singleLayerExecutors,
        MatchDoc matchDoc)
{
    docid_t docId = matchDoc.getDocId();
    Ha3MatchValues &mValues = _ref->getReference(matchDoc);
    mValues.setMaxNumTerms(_termCount);
    for (uint32_t i = 0; i < _termCount; ++i) {
        matchvalue_t &mvt = mValues.nextFreeMatchValue();
        mvt = matchvalue_t();
        int32_t idx = (int32_t)i - (int32_t)_accTermCount;
        if (idx >= 0
            && (uint32_t)idx < singleLayerExecutors.size()
            && singleLayerExecutors[idx]
            && singleLayerExecutors[idx]->getDocId() == docId)
        {
            mvt = singleLayerExecutors[idx]->getMatchValue();
        }
    }
    return IE_NAMESPACE(common)::ErrorCode::OK;
}

END_HA3_NAMESPACE(search);

