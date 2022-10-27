#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/ResultFormatter.h>

using namespace std;

BEGIN_HA3_NAMESPACE(common);

class ResultFormatterTest : public TESTBASE {
public:
    ResultFormatterTest();
    ~ResultFormatterTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(common, ResultFormatterTest);


ResultFormatterTest::ResultFormatterTest() { 
}

ResultFormatterTest::~ResultFormatterTest() { 
}

void ResultFormatterTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void ResultFormatterTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(ResultFormatterTest, testGetCoveredPercent) { 
    HA3_LOG(DEBUG, "Begin Test!");

    Result::ClusterPartitionRanges ranges;

    double coveredPercent = ResultFormatter::getCoveredPercent(ranges);
    ASSERT_EQ(0.00, coveredPercent);
    
    ranges["cluster1"].push_back(make_pair(0, 32767));
    coveredPercent = ResultFormatter::getCoveredPercent(ranges);
    ASSERT_EQ(50.00, coveredPercent);

    ranges["cluster1"].push_back(make_pair(32768, 65535));
    coveredPercent = ResultFormatter::getCoveredPercent(ranges);
    ASSERT_EQ(100.00, coveredPercent);
    
    ranges["cluster2"] = Result::PartitionRanges();
    coveredPercent = ResultFormatter::getCoveredPercent(ranges);
    ASSERT_EQ(50.00, coveredPercent);

    ranges["cluster2"].push_back(make_pair(32768, 65535));
    coveredPercent = ResultFormatter::getCoveredPercent(ranges);
    ASSERT_EQ(75.00, coveredPercent);
}

END_HA3_NAMESPACE(common);

