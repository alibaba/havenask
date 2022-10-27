#include <ha3_sdk/example/scorer/ScorerSample.h>
#include <ha3_sdk/testlib/builder/BuilderTestHelper.h>
#include <ha3_sdk/testlib/rank/ScorerTestHelper.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReaderCreator.h>
#include <unittest/unittest.h>
#include <ha3/util/Log.h>

using namespace std;
using namespace build_service::processor;
USE_HA3_NAMESPACE(index);
USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(rank);

class ScorerSampleTest : public TESTBASE {
public:
    void setUp() {}
    void tearDown() {}
private:
    void doTestScore(bool batchScore);
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(rank, ScorerSampleTest);

TEST_F(ScorerSampleTest, testSimpleProcess) {
    HA3_LOG(DEBUG, "Begin Test!");
    doTestScore(false);
}

TEST_F(ScorerSampleTest, testBatchScore) {
    doTestScore(true);
}

void ScorerSampleTest::doTestScore(bool batchScore) {
    KeyValueMap kvMap;
    BuilderTestHelper::prepareKeyValueMap("scorer_conf=aaa", kvMap);
    ScorerSample *scorer = new ScorerSample(kvMap);

    // prepare param
    ScorerTestParam param;
    param.fakeIndex.tablePrefix.push_back("sub_"); // necessary
    param.fakeIndex.tablePrefix.push_back("aux_");
    param.fakeIndex.indexes["aux_pk"] = "0:0,1:1\n";  // necessary
    param.fakeIndex.attributes = "price: int32_t: 1,2,3,4,5,6,7,8,9,10;"
                                 "sales_count: int32_t: 100,200,300,400,500,600,700,800,900,1000;"
                                 "aux_sales_count: int32_t: 100,200,300,400,500,600,700,800,900,1000;";
    param.docIdStr = "0,1,2,3,4,5,6,7,8,9";
    param.expectedScoreStr = "100,800,1800,3200,5000,7200,9800,6400,8100,10000";
    param.numTerms = 2;
    param.matchDataStr = "1:1:1:1,1:1:1:0;"   // doc 0
                         "1:1:1:1,1:1:1:1;"   // doc 1
                         "1:1:1:1,1:1:1:1;"   // doc 2
                         "1:1:1:1,1:1:1:1;"   // doc 3
                         "1:1:1:1,1:1:1:1;"   // doc 4
                         "1:1:1:1,1:1:1:1;"   // doc 5
                         "1:1:1:1,1:1:1:1;"   // doc 6
                         "1:1:1:1,1:1:1:0;"   // doc 7
                         "1:1:1:1,1:1:1:0;"   // doc 8
                         "1:1:1:1,1:1:1:0;";  // doc 9

    ScorerTestHelper helper("ScorerSampleTest");
    ASSERT_TRUE(helper.init(&param));

    // beginRequest
    ASSERT_TRUE(helper.beginRequest(scorer));

    // begin score
    ASSERT_TRUE(helper.beginScore(scorer));

    // score
    if (!batchScore) {
        ASSERT_TRUE(helper.score(scorer));
    } else {
        ASSERT_TRUE(helper.batchScore(scorer));
    }

    ASSERT_TRUE(batchScore == scorer->isBatchScore());

    // endScore
    ASSERT_TRUE(helper.endScore(scorer));

    // endRequest
    ASSERT_TRUE(helper.endRequest(scorer));
}

END_HA3_NAMESPACE(rank);
