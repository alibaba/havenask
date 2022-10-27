#ifndef ISEARCH_MATCHDOCSCORERS_H
#define ISEARCH_MATCHDOCSCORERS_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <suez/turing/expression/framework/AttributeExpression.h>
#include <suez/turing/expression/plugin/Scorer.h>
#include <suez/turing/expression/plugin/ScorerWrapper.h>
#include <ha3/common/Ha3MatchDocAllocator.h>
#include <ha3/search/SearchCommonResource.h>
#include <ha3/search/MatchDocSearcherProcessorResource.h>
#include <ha3/common/Request.h>
#include <autil/mem_pool/PoolVector.h>

BEGIN_HA3_NAMESPACE(rank);
class ScoringProvider;
END_HA3_NAMESPACE(rank);

namespace ha3 {
class ScorerProvider;
}

BEGIN_HA3_NAMESPACE(search);

class MatchDocAllocator;

class MatchDocScorers
{
public:
    MatchDocScorers(uint32_t reRankSize,
                    common::Ha3MatchDocAllocator *allocator,
                    autil::mem_pool::Pool *pool);
    ~MatchDocScorers();
private:
    MatchDocScorers(const MatchDocScorers &);
    MatchDocScorers& operator=(const MatchDocScorers &);
public:
    bool init(const std::vector<suez::turing::Scorer*> &scorerVec,
              const common::Request* request,
              const config::IndexInfoHelper *indexInfoHelper,
              SearchCommonResource &resource,
              SearchPartitionResource &partitionResource,
              SearchProcessorResource &processorResource);
    bool beginRequest();
    void setTotalMatchDocs(uint32_t v);
    void setAggregateResultsPtr(const common::AggregateResultsPtr &aggResultsPtr);
    void endRequest();
    uint32_t scoreMatchDocs(
            autil::mem_pool::PoolVector<matchdoc::MatchDoc> &matchDocVect);
    suez::turing::AttributeExpression* getScoreAttributeExpression();
    void sethasAssginedScoreValue(bool bScored) {
        _hasAssginedScoreValue = bScored;
    }
    size_t getScorerCount() const {
        return _scorerWrappers.size();
    }
    std::vector<std::string> getWarningInfos() {
        std::vector<std::string> warnInfos;
        for (auto wrapper : _scorerWrappers) {
            auto &warnInfo = wrapper->getWarningInfo();
            if (!warnInfo.empty()) {
                warnInfos.emplace_back(warnInfo);
            }
        }
        return warnInfos;
    }
private:
    uint32_t doScore(uint32_t phase,
                     autil::mem_pool::PoolVector<matchdoc::MatchDoc> &matchDocVect);
private:
    std::vector<suez::turing::ScorerWrapper *> _scorerWrappers;
    std::vector<rank::ScoringProvider *> _scoringProviders;
    std::vector<::ha3::ScorerProvider *> _cavaScorerProviders;
    matchdoc::Reference<score_t> *_scoreRef;
    suez::turing::AttributeExpressionTyped<score_t> *_scoreAttrExpr;
    common::Ha3MatchDocAllocator *_allocator;
    uint32_t _reRankSize;
    bool _hasAssginedScoreValue;
    autil::mem_pool::Pool *_pool;
private:
    friend class MatchDocScorersTest;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(MatchDocScorers);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_MATCHDOCSCORERS_H
