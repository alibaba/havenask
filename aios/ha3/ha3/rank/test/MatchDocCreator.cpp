#include <ha3/rank/test/MatchDocCreator.h>

using namespace std;
using namespace suez::turing;

USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(rank);
HA3_LOG_SETUP(rank, MatchDocCreator);

MatchDocCreator::MatchDocCreator(common::Ha3MatchDocAllocatorPtr allocatorPtr,
                                 AttributeExpressionTyped<score_t> *rankExpr,
                                 FakeExpression *uniqKeyExpr,
                                 std::vector<FakeExpression*> distKeyExprVect,
                                 vector<matchdoc::Reference<score_t>*> personalVarRefs)
    : _allocatorPtr(allocatorPtr),
      _rankExpr(rankExpr),
      _uniqKeyExpr(uniqKeyExpr),
      _distKeyExprVect(distKeyExprVect),
      _personalVarRefs(personalVarRefs)
{
}

MatchDocCreator::~MatchDocCreator() {
}

matchdoc::MatchDoc MatchDocCreator::createMatchDoc(
        docid_t docid, score_t score,
        int32_t uniqKey, int32_t distKey1, int32_t distKey2,
        score_t perVal1, score_t perVal2)
{
    auto matchDoc = _allocatorPtr->allocate(docid);
    auto ref = _rankExpr->getReference();
    ref->set(matchDoc, score);

    auto uniqKeyRef = _uniqKeyExpr->getReference();
    uniqKeyRef->set(matchDoc, uniqKey);

    auto distKeyRef1 = _distKeyExprVect[0]->getReference();
    distKeyRef1->set(matchDoc, distKey1);

    auto distKeyRef2 = _distKeyExprVect[1]->getReference();
    distKeyRef2->set(matchDoc, distKey2);

    _personalVarRefs[0]->set(matchDoc, perVal1);
    _personalVarRefs[1]->set(matchDoc, perVal2);
    return matchDoc;
}

END_HA3_NAMESPACE(rank);
