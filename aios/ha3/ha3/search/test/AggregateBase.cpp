#include "ha3/search/test/AggregateBase.h"

using namespace suez::turing;
using namespace std;
USE_HA3_NAMESPACE(index);
BEGIN_HA3_NAMESPACE(search);

AggregateBase::AggregateBase() :
    _keyAttributeExpr(NULL), _funAttributeExpr(NULL),
    _attrExprPool(new suez::turing::AttributeExpressionPool()),
    _pool(new autil::mem_pool::Pool(1024)),
    _mdAllocator(new common::Ha3MatchDocAllocator(_pool, true))
{}

AggregateBase::~AggregateBase() {
    clear();
}

void AggregateBase::baseSetUp()
{
    _fakeAttrExprMaker.reset(new FakeAttributeExpressionMaker(_pool));
}

void AggregateBase::baseTearDown()
{
    _fakeAttrExprMaker.reset();
}

void AggregateBase::resetExpression() {
    // DELETE_AND_SET_NULL(_keyAttributeExpr);
    // DELETE_AND_SET_NULL(_funAttributeExpr);
    DELETE_AND_SET_NULL(_attrExprPool);
}

void AggregateBase::clear() {
    for (vector<matchdoc::MatchDoc>::iterator it = _mdVector.begin();
         it != _mdVector.end(); it++)
    {
        _mdAllocator->deallocate(*it);
    }
    _mdVector.clear();

    DELETE_AND_SET_NULL(_attrExprPool);
    DELETE_AND_SET_NULL(_mdAllocator);
    DELETE_AND_SET_NULL(_pool);
}

void AggregateBase::aggMatchDoc(int32_t docNum, Aggregator *agg) {
    agg->setMatchDocAllocator(_mdAllocator);
    for (int i = 0; i < docNum; i++) {
        matchdoc::MatchDoc doc = _mdAllocator->allocate((docid_t)i);
        _mdVector.push_back(doc);
        agg->aggregate(doc);
    }
    agg->endLayer(1.0f);
    agg->estimateResult(1.0);
}

void AggregateBase::aggSubMatchDoc(
        const std::string &subDocNumStr, Aggregator *agg)
{
    vector<int> subDocNum;
    autil::StringUtil::fromString(subDocNumStr, subDocNum, ",");
    agg->setMatchDocAllocator(_mdAllocator);
    int docNum = (int)subDocNum.size();
    docid_t subDocId = 0;
    for (int i = 0; i < docNum; i++) {
        matchdoc::MatchDoc doc = _mdAllocator->allocate((docid_t)i);
        for (int j = 0; j < subDocNum[i]; j++) {
            _mdAllocator->allocateSub(doc, subDocId++);
        }
        _mdVector.push_back(doc);
        agg->aggregate(doc);
    }
    agg->endLayer(1.0f);
    agg->estimateResult(1.0);
}

END_HA3_NAMESPACE();
