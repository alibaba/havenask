#ifndef ISEARCH_SORTEXPRESSIONCREATOR_H
#define ISEARCH_SORTEXPRESSIONCREATOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/SortClause.h>
#include <ha3/common/Ha3MatchDocAllocator.h>
#include <ha3/search/RankResource.h>
#include <suez/turing/expression/framework/AttributeExpressionCreator.h>
#include <ha3/search/SortExpression.h>
#include <ha3/rank/RankProfile.h>
#include <ha3/common/MultiErrorResult.h>
#include <autil/mem_pool/Pool.h>

BEGIN_HA3_NAMESPACE(search);

class SortExpressionCreator
{
public:
    SortExpressionCreator(suez::turing::AttributeExpressionCreator *attributeExpressionCreator,
                          const rank::RankProfile *rankProfile,
                          common::Ha3MatchDocAllocator *matchDocAllocator,
                          const common::MultiErrorResultPtr &errorResultPtr,
                          autil::mem_pool::Pool *pool);
    ~SortExpressionCreator();
private:
    SortExpressionCreator(const SortExpressionCreator &);
    SortExpressionCreator& operator=(const SortExpressionCreator &);
public:
    bool init(const common::Request* request);
    suez::turing::RankAttributeExpression *createRankExpression();
public:
    suez::turing::RankAttributeExpression *getRankExpression() const {
        return _rankExpression;
    }
    const std::vector<SortExpressionVector> &getRankSortExpressions() const {
        return _rankSortExpressions;
    }
    SortExpression* createSortExpression(
            suez::turing::AttributeExpression* attributeExpression, bool sortFlag)
    {
        if (!attributeExpression) {
            return NULL;
        }
        SortExpression *sortExpr = POOL_NEW_CLASS(_pool,
                SortExpression, attributeExpression);
        sortExpr->setSortFlag(sortFlag);
        _needDelExpressions.push_back(sortExpr);
        return sortExpr;
    }
private:
    bool initSortExpressions(const common::SortClause *sortClause);
    bool initRankSortExpressions(const common::RankSortClause *rankSortClause);
    bool doCreateSortExpressions(
            const std::vector<common::SortDescription *> &sortDescriptions,
            SortExpressionVector &sortExpressions);
    bool createSingleSortExpression(
            const common::SortDescription *sortDescription,
            SortExpressionVector &sortExpressions);
public:
    // for test
    void setRankSortExpressions(const std::vector<SortExpressionVector>& v) {
        _rankSortExpressions = v;
    }
private:
    suez::turing::AttributeExpressionCreator *_attributeExprCreator;
    const rank::RankProfile *_rankProfile;
    common::Ha3MatchDocAllocator *_matchDocAllocator;
    common::MultiErrorResultPtr _errorResultPtr;

    suez::turing::RankAttributeExpression *_rankExpression;
    std::vector<SortExpressionVector> _rankSortExpressions;
    SortExpressionVector _needDelExpressions;
    autil::mem_pool::Pool *_pool;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(SortExpressionCreator);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_SORTEXPRESSIONCREATOR_H
