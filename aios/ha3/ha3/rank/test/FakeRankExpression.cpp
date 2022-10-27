#include <ha3/rank/test/FakeRankExpression.h>

USE_HA3_NAMESPACE(common);
BEGIN_HA3_NAMESPACE(rank);
HA3_LOG_SETUP(rank, FakeRankExpression);

bool FakeRankExpression::allocate(matchdoc::MatchDocAllocator *allocator){
    auto ref = allocator->declare<score_t>(_refName);
    this->setReference(ref);
    return true;
}

bool FakeRankExpression::evaluate(matchdoc::MatchDoc matchDoc) {
    ++_evaluateCount;
    return true;
}

score_t FakeRankExpression::evaluateAndReturn(matchdoc::MatchDoc matchDoc) {
    ++_evaluateCount;
    return 0;
}

END_HA3_NAMESPACE(rank);

