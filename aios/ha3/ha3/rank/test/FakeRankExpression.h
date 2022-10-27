#ifndef ISEARCH_FAKERANKEXPRESSION_H
#define ISEARCH_FAKERANKEXPRESSION_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <suez/turing/expression/framework/AttributeExpression.h>
#include <ha3/rank/Comparator.h>
#include <ha3/rank/ReferenceComparator.h>
#include <ha3/common/Ha3MatchDocAllocator.h>
#include <matchdoc/Reference.h>
#include <assert.h>
#include <matchdoc/MatchDoc.h>

BEGIN_HA3_NAMESPACE(rank);

class FakeRankExpression : public suez::turing::AttributeExpressionTyped<score_t>
{
public:
    FakeRankExpression(const std::string &refName)
        : _refName(refName)
    {
        _evaluateCount = 0;
    }
    ~FakeRankExpression() {}
public:
    bool allocate(matchdoc::MatchDocAllocator *allocator) override;
    suez::turing::ExpressionType getExpressionType() const override {
        return suez::turing::ET_RANK;
    }    
    bool evaluate(matchdoc::MatchDoc matchDoc) override;
    score_t evaluateAndReturn(matchdoc::MatchDoc matchDoc) override;
public:
    uint32_t getEvaluateCount() const { return _evaluateCount; }
    void resetEvaluateTimes() { _evaluateCount = 0; }
private:
    uint32_t _evaluateCount;
    std::string _refName;
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(rank);

#endif //ISEARCH_FAKERANKEXPRESSION_H
