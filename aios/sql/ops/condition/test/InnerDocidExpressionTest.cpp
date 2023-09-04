#include "sql/ops/condition/InnerDocidExpression.h"

#include <cstddef>
#include <vector>

#include "autil/Span.h"
#include "autil/mem_pool/Pool.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/Reference.h"
#include "suez/turing/expression/common.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
using namespace suez::turing;

namespace sql {
class InnerDocidExpressionTest : public TESTBASE {
public:
    InnerDocidExpressionTest();
    ~InnerDocidExpressionTest();

public:
    void setUp();
    void tearDown();

private:
    autil::mem_pool::Pool _pool;
    matchdoc::MatchDocAllocator *_allocator;
    InnerDocIdExpression _expr;
};

InnerDocidExpressionTest::InnerDocidExpressionTest() {}

InnerDocidExpressionTest::~InnerDocidExpressionTest() {}

void InnerDocidExpressionTest::setUp() {
    _allocator = new matchdoc::MatchDocAllocator(&_pool);
    _expr.setOriginalString("__inner_docid");
    _expr.allocate(_allocator);
}

void InnerDocidExpressionTest::tearDown() {
    delete _allocator;
}

TEST_F(InnerDocidExpressionTest, testEvaluate) {
    auto doc1 = _allocator->allocate(1);
    ASSERT_TRUE(_expr.evaluate(doc1));
    auto doc3 = _allocator->allocate(3);
    ASSERT_TRUE(_expr.evaluate(doc3));
    auto ref = _expr.getReference();
    ASSERT_EQ(1, ref->get(doc1));
    ASSERT_EQ(3, ref->get(doc3));
}

TEST_F(InnerDocidExpressionTest, testEvaluateAndReturn) {
    auto doc1 = _allocator->allocate(1);
    ASSERT_EQ(1, _expr.evaluateAndReturn(doc1));
    auto doc3 = _allocator->allocate(3);
    ASSERT_EQ(3, _expr.evaluateAndReturn(doc3));
    auto ref = _expr.getReference();
    ASSERT_EQ(1, ref->get(doc1));
    ASSERT_EQ(3, ref->get(doc3));
}

TEST_F(InnerDocidExpressionTest, testBatchEvaluate) {
    vector<matchdoc::MatchDoc> docs = {_allocator->allocate(1), _allocator->allocate(3)};
    ASSERT_TRUE(_expr.batchEvaluate(docs.data(), docs.size()));
    auto ref = _expr.getReference();
    ASSERT_EQ(1, ref->get(docs[0]));
    ASSERT_EQ(3, ref->get(docs[1]));
}

} // namespace sql
