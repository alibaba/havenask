#ifndef ISEARCH_MATCHDOCCREATOR_H
#define ISEARCH_MATCHDOCCREATOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <matchdoc/MatchDoc.h>
#include <ha3/rank/test/FakeRankExpression.h>
#include <ha3/rank/test/FakeExpression.h>
#include <ha3/common/Ha3MatchDocAllocator.h>
#include <suez/turing/expression/framework/RankAttributeExpression.h>

BEGIN_HA3_NAMESPACE(rank);

class MatchDocCreator
{
public:
    MatchDocCreator(common::Ha3MatchDocAllocatorPtr allocatorPtr,
                    suez::turing::AttributeExpressionTyped<score_t> *rankExpr, FakeExpression *uniqKeyExpr,
                    std::vector<FakeExpression*> distKeyExprVect,
                    std::vector<matchdoc::Reference<score_t>*> personalVarRefs);
    ~MatchDocCreator();
public:
    matchdoc::MatchDoc createMatchDoc(docid_t docid, score_t score,
            int32_t uniqKey, int32_t distKey1, int32_t distKey2,
            score_t perVal1 = (score_t)0.0, score_t perVal2 = (score_t)0.0);
private:
    MatchDocCreator(const MatchDocCreator &);
    MatchDocCreator& operator=(const MatchDocCreator &);
public:
private:
    common::Ha3MatchDocAllocatorPtr _allocatorPtr;
    suez::turing::AttributeExpressionTyped<score_t> *_rankExpr;
    FakeExpression *_uniqKeyExpr;
    std::vector<FakeExpression*> _distKeyExprVect;
    std::vector<matchdoc::Reference<score_t>*> _personalVarRefs;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(MatchDocCreator);

END_HA3_NAMESPACE(rank);

#endif //ISEARCH_MATCHDOCCREATOR_H
