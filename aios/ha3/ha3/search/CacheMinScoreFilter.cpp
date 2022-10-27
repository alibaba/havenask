#include <ha3/search/CacheMinScoreFilter.h>
#include <tr1/functional>
#include <ha3/rank/MultiHitCollector.h>
#include <ha3/common/Ha3MatchDocAllocator.h>
#include <algorithm>

using namespace std;
using namespace std::tr1;
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(rank);
using namespace suez::turing;

BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, CacheMinScoreFilter);

CacheMinScoreFilter::CacheMinScoreFilter() {
}

CacheMinScoreFilter::CacheMinScoreFilter(const std::vector<score_t> & minScores) {
    _scores = minScores;
}

CacheMinScoreFilter::~CacheMinScoreFilter() {
}

void CacheMinScoreFilter::storeMinScore(HitCollectorBase *rankHitCollector,
                                        const SortExpressionVector &firstExpressions)
{
    HitCollectorBase::HitCollectorType type = rankHitCollector->getType();
    bool isScored = rankHitCollector->isScored();
    if (type == HitCollectorBase::HCT_SINGLE) {
        _scores.push_back(getMinScore(rankHitCollector,
                        isScored, firstExpressions[0]));
    } else {
        MultiHitCollector *multiHitCollector =
            dynamic_cast<MultiHitCollector*>(rankHitCollector);
        const vector<HitCollectorBase*> &hitCollectors =
            multiHitCollector->getHitCollectors();
        for (size_t i = 0; i < hitCollectors.size(); ++i) {
            _scores.push_back(getMinScore(hitCollectors[i],
                            isScored, firstExpressions[i]));
        }
    }
}

void CacheMinScoreFilter::filterByMinScore(
        HitCollectorBase *rankHitCollector,
        const SortExpressionVector &firstExpressions,
        autil::mem_pool::PoolVector<matchdoc::MatchDoc> &matchDocs,
        size_t expectCount) const
{
    // only get basic info from rankHitCollector
    // do not get any matchDocs in rankHitCollector
    bool isScored = rankHitCollector->isScored();
    if (!isScored) {
        return;
    }

    vector<ScoreInfo> infos;
    for (size_t i = 0; i < firstExpressions.size(); ++i) {
        infos.push_back(make_pair(firstExpressions[i]->getReferenceBase(),
                        firstExpressions[i]->getSortFlag()));
    }

    vector<matchdoc::MatchDoc> filteredMatchDocs;
    doFilterByMinScore(infos, matchDocs, filteredMatchDocs);
    size_t actualCount = matchDocs.size();
    HA3_LOG(DEBUG, "actualCount:%zd, expectCount:%zd",
            actualCount, expectCount);
    if (actualCount < expectCount) {
        const ComboComparator *comp = getComparator(rankHitCollector);
        selectExtraMatchDocs(expectCount - actualCount,
                             comp, filteredMatchDocs, matchDocs);
    }

    releaseFilteredMatchDocs(rankHitCollector, filteredMatchDocs);
}

const ComboComparator *CacheMinScoreFilter::getComparator(
        HitCollectorBase *rankHitCollector) const
{
    HitCollectorBase::HitCollectorType type = rankHitCollector->getType();
    if (HitCollectorBase::HCT_SINGLE == type) {
        return rankHitCollector->getComparator();
    } else if (HitCollectorBase::HCT_MULTI == type) {
        auto multiHitCollector =  dynamic_cast<MultiHitCollector *>(
                rankHitCollector);
        if (!multiHitCollector) {
            return NULL;
        } else {
            const vector<HitCollectorBase *> &collectors =
                multiHitCollector->getHitCollectors();
            if (collectors.size() > 0) {
                return collectors[0]->getComparator();
            }
        }
    }
    return NULL;
}

void CacheMinScoreFilter::doFilterByMinScore(const vector<ScoreInfo> &infos,
        autil::mem_pool::PoolVector<matchdoc::MatchDoc> &matchDocs,
        vector<matchdoc::MatchDoc> &filteredMatchDocs) const
{
    size_t newCursor = 0;
    for (size_t i = 0; i < matchDocs.size(); ++i) {
        if (isFilter(infos, matchDocs[i])) {
            matchDocs[newCursor++] = matchDocs[i];
        } else {
            filteredMatchDocs.push_back(matchDocs[i]);
        }
    }
    matchDocs.resize(newCursor);
}

void CacheMinScoreFilter::selectExtraMatchDocs(size_t selectCount,
        const ComboComparator *rankComp,
        std::vector<matchdoc::MatchDoc> &filteredMatchDocs,
        autil::mem_pool::PoolVector<matchdoc::MatchDoc> &matchDocs) const
{
    if (rankComp == NULL) {
        HA3_LOG(ERROR, "rankComp is NULL");
        return;
    }

    if (selectCount < filteredMatchDocs.size()) {
        MatchDocComp comp(rankComp);
        nth_element(filteredMatchDocs.begin(),
                    filteredMatchDocs.begin() + selectCount - 1,
                    filteredMatchDocs.end(),
                    comp);
    }

    for (size_t i = 0; i < filteredMatchDocs.size() && i < selectCount; i++) {
        matchDocs.push_back(filteredMatchDocs[i]);
    }

    if (selectCount > 0 && selectCount < filteredMatchDocs.size()) {
        size_t pos = 0;
        for (size_t i = selectCount; i < filteredMatchDocs.size(); i++, pos++) {
            filteredMatchDocs[pos] = filteredMatchDocs[i];
        }
    }
    HA3_LOG(DEBUG, "filteredMatchDocs.size() = %zd, selectCount = %zd",
            filteredMatchDocs.size(), selectCount);
    filteredMatchDocs.resize(filteredMatchDocs.size() > selectCount ?
                             filteredMatchDocs.size() - selectCount : 0);
}

void CacheMinScoreFilter::releaseFilteredMatchDocs(
        HitCollectorBase *rankHitCollector,
        vector<matchdoc::MatchDoc> &filteredMatchDocs) const
{
    auto matchDocAllocator =
        dynamic_cast<common::Ha3MatchDocAllocator *>(rankHitCollector->getMatchDocAllocator().get());
    assert(matchDocAllocator);
    for (size_t i = 0; i < filteredMatchDocs.size(); i++) {
        auto matchDoc = filteredMatchDocs[i];
        matchDocAllocator->deallocate(matchDoc);
    }
    filteredMatchDocs.clear();
}

void CacheMinScoreFilter::serialize(autil::DataBuffer &dataBuffer) const {
    dataBuffer.write(_scores);
}

void CacheMinScoreFilter::deserialize(autil::DataBuffer &dataBuffer) {
    dataBuffer.read(_scores);
}

score_t CacheMinScoreFilter::getMinScore(HitCollectorBase *hitCollector,
        bool isScored, SortExpression *expr)
{
    bool sortFlag = expr->getSortFlag();
    if (!isScored) {
        return defaultScoreMin(sortFlag);
    }
    matchdoc::MatchDoc minMatchDoc = hitCollector->top();
    //assert(minMatchDoc || hitCollector->getItemCount() == 0);
    if (matchdoc::INVALID_MATCHDOC == minMatchDoc) {
        return defaultScoreMin(sortFlag);
    }
    return getRankScore(expr->getReferenceBase(), minMatchDoc);
}

score_t CacheMinScoreFilter::defaultScoreMin(bool isAsc) {
    if (isAsc) {
        return numeric_limits<score_t>::max();
    }
    return numeric_limits<score_t>::min();
}

score_t CacheMinScoreFilter::getRankScore(matchdoc::ReferenceBase *rankScoreRef,
        matchdoc::MatchDoc matchDoc)
{
    if (matchdoc::INVALID_MATCHDOC == matchDoc || !rankScoreRef) {
        HA3_LOG(WARN, "has no rank scorer, matchDoc:[%d], rankScoreRef:[%p]",
                matchDoc.getDocId(), rankScoreRef);
        return (score_t)0;
    }
    auto vtype = rankScoreRef->getValueType().getBuiltinType();
#define GET_SCORE_CASE(vt) case vt:                                     \
    {                                                                   \
        typedef VariableTypeTraits<vt, false>::AttrExprType AttrExprType; \
        auto scoreRef = dynamic_cast<matchdoc::Reference<AttrExprType> *>(rankScoreRef); \
        if (scoreRef == NULL) {                                         \
            HA3_LOG(WARN, "get min score failed");                      \
        } else {                                                        \
            auto score = scoreRef->get(matchDoc);                       \
            return (score_t)score;                                      \
        }                                                               \
        break;                                                          \
    }

    switch (vtype) {
        GET_SCORE_CASE(vt_int8);
        GET_SCORE_CASE(vt_int16);
        GET_SCORE_CASE(vt_int32);
        GET_SCORE_CASE(vt_int64);
        GET_SCORE_CASE(vt_uint8);
        GET_SCORE_CASE(vt_uint16);
        GET_SCORE_CASE(vt_uint32);
        GET_SCORE_CASE(vt_uint64);
        GET_SCORE_CASE(vt_float);
        GET_SCORE_CASE(vt_double);
    default:
            HA3_LOG(DEBUG, "wrong variable type:[%d]", (int)vtype);
        break;
    }
    return (score_t)0;
}

bool CacheMinScoreFilter::isFilter(const vector<ScoreInfo> &scoreInfos,
                                   matchdoc::MatchDoc matchDoc) const
{
    for (size_t i = 0; i < scoreInfos.size(); ++i) {
        const ScoreInfo &info = scoreInfos[i];
        score_t rankScore = getRankScore(info.first, matchDoc);
        score_t minScore = i >= _scores.size() ? 0 : _scores[i];
        bool ret = info.second ?
                   rankScore <= minScore: rankScore >= minScore;
        if (ret) {
            return true;
        }
    }
    return false;
}

END_HA3_NAMESPACE(search);
