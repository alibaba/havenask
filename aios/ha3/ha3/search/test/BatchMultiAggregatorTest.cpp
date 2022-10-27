#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/BatchMultiAggregator.h>
#include <ha3/search/test/MultiAggregatorTest.h>

BEGIN_HA3_NAMESPACE(search);

class BatchMultiAggregatorTest : public MultiAggregatorTest {
public:
    BatchMultiAggregatorTest();
    ~BatchMultiAggregatorTest();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(search, BatchMultiAggregatorTest);


BatchMultiAggregatorTest::BatchMultiAggregatorTest() 
    : MultiAggregatorTest(){ 
    _testMode = MULTI_BATCH;
}

BatchMultiAggregatorTest::~BatchMultiAggregatorTest() { 
}

END_HA3_NAMESPACE(search);
