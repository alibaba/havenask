#include "indexlib/index/attribute/expression/AtomicAttributeExpression.h"

#include "indexlib/index/attribute/expression/test/AttributeExpressionTestHelper.h"
#include "unittest/unittest.h"
namespace indexlibv2::index {

class AtomicAttributeExpressionTest : public TESTBASE
{
public:
    AtomicAttributeExpressionTest() = default;
    ~AtomicAttributeExpressionTest() = default;

public:
    void setUp() override;
    void tearDown() override;
};

void AtomicAttributeExpressionTest::setUp() {}

void AtomicAttributeExpressionTest::tearDown() {}

TEST_F(AtomicAttributeExpressionTest, TestSimple)
{
    AttributeExpressionTestHelper<ft_uint32, false> helper;
    ASSERT_TRUE(helper.Init());

    helper.AddOneDoc(100);
    helper.AddOneDoc(200);

    matchdoc::MatchDocAllocator allocator(&(helper._pool));
    AtomicAttributeExpression<uint32_t> expr(helper._config);
    ASSERT_EQ(expression::ExprValueType::EVT_UINT32, expr.getExprValueType());
    ASSERT_TRUE(expr.InitPrefetcher(helper._resource.get()).IsOK());
    ASSERT_TRUE(expr.allocate(&allocator));

    auto doc = allocator.allocate(0);

    ASSERT_TRUE(expr.Prefetch(doc).IsOK());
    expr.evaluate(doc);
    ASSERT_EQ(100, expr.getValue(doc));

    doc.setDocId(1);
    ASSERT_TRUE(expr.Prefetch(doc).IsOK());
    expr.evaluate(doc);
    ASSERT_EQ(200, expr.getValue(doc));
}

} // namespace indexlibv2::index
