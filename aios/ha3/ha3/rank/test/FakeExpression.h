#ifndef ISEARCH_FAKEEXPRESSION_H
#define ISEARCH_FAKEEXPRESSION_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/rank/Comparator.h>
#include <suez/turing/expression/framework/AttributeExpression.h>
#include <ha3/common/Ha3MatchDocAllocator.h>
#include <ha3/rank/ReferenceComparator.h>
#include <matchdoc/MatchDoc.h>

BEGIN_HA3_NAMESPACE(rank);

class FakeExpression : public suez::turing::AttributeExpressionTyped<int32_t>
{
public:
    FakeExpression(const std::string &name = "fakeexpr") {
        _name = name;
        _evaluateTimes = 0;
        setOriginalString(_name);
    }
    ~FakeExpression() {
    }
public:
    uint32_t getEvaluateTimes() const {
        return _evaluateTimes;
    }
    void resetEvaluateTimes() {
        _evaluateTimes = 0;
    }
    bool evaluate(matchdoc::MatchDoc matchDoc) override {
        ++_evaluateTimes;
        return true;
    }
    int32_t evaluateAndReturn(matchdoc::MatchDoc matchDoc) override {
        ++_evaluateTimes;
        return 0;
    }
private:
    std::string _name;
    uint32_t _evaluateTimes;
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(rank);

#endif //ISEARCH_FAKEEXPRESSION_H
