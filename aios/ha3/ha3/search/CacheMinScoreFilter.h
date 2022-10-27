#ifndef ISEARCH_CACHEMINSCOREFILTER_H
#define ISEARCH_CACHEMINSCOREFILTER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/DataBuffer.h>
#include <matchdoc/MatchDoc.h>
#include <ha3/rank/HitCollector.h>
#include <ha3/search/SortExpression.h>

BEGIN_HA3_NAMESPACE(search);

class CacheMinScoreFilter
{
private:
    typedef std::pair<matchdoc::ReferenceBase *, bool> ScoreInfo;
public:
    CacheMinScoreFilter();
    CacheMinScoreFilter(const std::vector<score_t> & minScores);
    ~CacheMinScoreFilter();
public:
    void storeMinScore(rank::HitCollectorBase *rankHitCollector,
                       const SortExpressionVector &firstExpressions);
    void filterByMinScore(rank::HitCollectorBase *rankHitCollector,
                          const SortExpressionVector &firstExpressions,
                          autil::mem_pool::PoolVector<matchdoc::MatchDoc> &matchDocs,
                          size_t expectCount = 0) const;
public:
    void serialize(autil::DataBuffer &dataBuffer) const;
    void deserialize(autil::DataBuffer &dataBuffer);

public:
    // for test
    score_t getMinScore(uint32_t idx) {
        if (idx >= _scores.size()) {
            return 0;
        }
        return _scores[idx];
    }
    void addMinScore(score_t minScore) {
        _scores.push_back(minScore);
    }
    std::vector<score_t> &getMinScoreVec() {
        return _scores;
    }
private:
    bool isFilter(const std::vector<ScoreInfo> &scoreInfos,
                  matchdoc::MatchDoc matchDoc) const;
    void doFilterByMinScore(
            const std::vector<ScoreInfo> &infos,
            autil::mem_pool::PoolVector<matchdoc::MatchDoc> &matchDocs,
            std::vector<matchdoc::MatchDoc> &matchDocsVec) const;
    void selectExtraMatchDocs(
            size_t selectCount, const rank::ComboComparator *rankComp,
            std::vector<matchdoc::MatchDoc> &filteredMatchDocs,
            autil::mem_pool::PoolVector<matchdoc::MatchDoc> &matchDocs) const;
    void releaseFilteredMatchDocs(
            rank::HitCollectorBase *rankHitCollector,
            std::vector<matchdoc::MatchDoc> &filteredMatchDocs) const;
    const rank::ComboComparator* getComparator(
            rank::HitCollectorBase *rankHitCollector) const;
private:
    static score_t getMinScore(rank::HitCollectorBase *hitCollector,
                               bool isScored, SortExpression *expr);
    static score_t defaultScoreMin(bool isAsc);
    static score_t getRankScore(matchdoc::ReferenceBase *rankScoreRef,
                                matchdoc::MatchDoc matchDoc) ;
private:
    std::vector<score_t> _scores;
private:
    friend class CacheMinScoreFilterTest;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(CacheMinScoreFilter);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_CACHEMINSCOREFILTER_H
