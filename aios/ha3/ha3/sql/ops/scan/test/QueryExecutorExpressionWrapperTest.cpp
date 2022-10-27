#include <unittest/unittest.h>
#include <ha3/sql/ops/scan/QueryExecutorExpressionWrapper.h>
#include <ha3/sql/ops/scan/RangeQueryExecutor.h>
#include <ha3/search/LayerMetas.h>
#include <autil/mem_pool/Pool.h>

using namespace std;
using namespace testing;
USE_HA3_NAMESPACE(search);
using namespace matchdoc;

BEGIN_HA3_NAMESPACE(sql);

class QueryExecutorExpressionWrapperTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
};

void QueryExecutorExpressionWrapperTest::setUp() {
}

void QueryExecutorExpressionWrapperTest::tearDown() {
}


TEST_F(QueryExecutorExpressionWrapperTest, testEvaluateAndReturn) {
    autil::mem_pool::Pool pool;
    LayerMeta layerMeta(&pool);
    layerMeta.push_back(DocIdRangeMeta(5, 7));
    RangeQueryExecutor executor(&layerMeta);
    QueryExecutorExpressionWrapper wrapper(&executor);
    ASSERT_FALSE(wrapper.evaluateAndReturn(MatchDoc(0, 1)));
    ASSERT_FALSE(wrapper.evaluateAndReturn(MatchDoc(0, 4)));
    ASSERT_TRUE(wrapper.evaluateAndReturn(MatchDoc(0, 5)));
    ASSERT_TRUE(wrapper.evaluateAndReturn(MatchDoc(0, 6)));
    ASSERT_TRUE(wrapper.evaluateAndReturn(MatchDoc(0, 7)));
    ASSERT_FALSE(wrapper.evaluateAndReturn(MatchDoc(0, 8)));
}

END_HA3_NAMESPACE();

