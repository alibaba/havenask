#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <matchdoc/MatchDoc.h>
#include <matchdoc/Reference.h>
#include <ha3/common/Ha3MatchDocAllocator.h>
#include <matchdoc/Reference.h>
#include <ha3/search/SumAggregateFunction.h>

using namespace std;
using namespace matchdoc;
using namespace suez::turing;

BEGIN_HA3_NAMESPACE(search);

class SumAggregatorFunctionTest : public TESTBASE {
public:
    void setUp() {};
    void tearDown() {};
private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(search, SumAggregatorFunctionTest);

TEST_F(SumAggregatorFunctionTest, testEndLayer) {
    HA3_LOG(DEBUG, "Begin Test!");
    SumAggregateFunction<int64_t, int32_t> sumAgg("", vt_int64, NULL);
    autil::mem_pool::Pool pool;
    MatchDocAllocator mdAlloc(&pool);
    sumAgg.declareResultReference(&mdAlloc);
    auto doc = mdAlloc.allocate(0);
    sumAgg.initFunction(doc);

    auto &sampleValue = sumAgg._curLayerSampleRefer->getReference(doc);
    auto &layerValue = sumAgg._curLayerResultRefer->getReference(doc);
    auto &totalValue = sumAgg._totalResultRefer->getReference(doc);

    sumAgg.endLayer(1, 1, doc);
    ASSERT_EQ(0, totalValue);
    sumAgg.endLayer(2, 1, doc);
    ASSERT_EQ(0, totalValue);

    sampleValue = 10;
    layerValue = 20;
    sumAgg.endLayer(1, 1, doc);
    ASSERT_EQ(30, totalValue);

    sampleValue = 10;
    layerValue = 20;
    sumAgg.endLayer(1, 1, doc);
    ASSERT_EQ(60, totalValue);

    sampleValue = 10;
    layerValue = 20;
    sumAgg.endLayer(2, 1, doc);
    ASSERT_EQ(100, totalValue);

    sampleValue = 10;
    layerValue = 20;
    sumAgg.endLayer(2, 1, doc);
    ASSERT_EQ(140, totalValue);
}

END_HA3_NAMESPACE(search);
