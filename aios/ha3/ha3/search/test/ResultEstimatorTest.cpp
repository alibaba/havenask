#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/ResultEstimator.h>
#include <ha3/search/test/LayerMetasConstructor.h>
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;
BEGIN_HA3_NAMESPACE(search);

class ResultEstimatorTest : public TESTBASE {
public:
    ResultEstimatorTest();
    ~ResultEstimatorTest();
public:
    void setUp();
    void tearDown();
protected:
    void checkTotalRange(const std::string &layerStr, 
                         const std::string &resultStr);

    void internalTestEstimator(const std::string &layerMetasStr,
                               const std::string &endLayerOperators,
                               uint32_t totalMatchCount,
                               const std::string &needAggregatesStr = "");

    void internalTestInitCoveredInfo(const std::string &layerMetasStr,
            const std::string &results);
protected:
    autil::mem_pool::Pool *_pool;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(search, ResultEstimatorTest);


ResultEstimatorTest::ResultEstimatorTest() {
    _pool = new autil::mem_pool::Pool(1024);
}

ResultEstimatorTest::~ResultEstimatorTest() {
    delete _pool;
}

void ResultEstimatorTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void ResultEstimatorTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(ResultEstimatorTest, testEndLayer) { 
    internalTestEstimator("1,4,0|5,7,0", "0,3,3;1,3,1", 5);
    internalTestEstimator("1,4,0|1,4,0", "0,2,2;1,2,1", 4);
    internalTestEstimator("1,4,0;7,10,0", "0,6,5", 6);
    internalTestEstimator("1,4,0;7,10,0|15,20,0", "0,6,5;1,4,1", 7);
    internalTestEstimator("1,400,0;700,1000,0|1,400,0;700,1000,0", "0,600,17;1,400,11", 19);
    internalTestEstimator("1,400,0;700,1000,0|1204,1455,0;1600,7000,0|10000,55555,0;555555,666666,0", "0,600,17;1,2345,20;2,54321,23", 133);
    internalTestEstimator("1,400,0;700,1000,0|1204,1455,0;1600,7000,0|10000,55555,0;555555,666666,0", "0,600,17;2,54321,23", 88);
    internalTestEstimator("1,400,0;700,1000,0|1,400,0;700,1000,0|10000,55555,0;555555,666666,0", "0,600,17;2,54321,23", 85);
    internalTestEstimator("1,400,0;700,1000,0|1,400,0;700,1000,0|10000,55555,0;555555,666666,0", "0,600,17;1,2345,20;2,54321,23", 85);
    internalTestEstimator("1,400,0;700,1000,0|1204,1455,0;1600,7000,0|10000,55555,0;555555,666666,0", "0,701,17;1,5653,20;2,54321,23", 103);
    internalTestEstimator("1,400,0;700,1000,0|1204,1455,0;1600,7000,0|10000,55555,0;555555,666666,0", "0,701,17;1,5653,20;2,156668,23", 60);
    internalTestEstimator("1,4,0|1,4,0", "0,2,1;1,2,0", 2);
    internalTestEstimator("1,4,0|1,4,0", "1,2,1", 2);
}

TEST_F(ResultEstimatorTest, testJumpLayer) { 
    internalTestEstimator("1,400,0;700,1000,0|1204,1455,0;1600,7000,0|10000,55555,0;555555,666666,0", "1,2345,20;2,54321,23", 114);
    internalTestEstimator("1,400,0;700,1000,0|1204,1455,0;1600,7000,0|10000,55555,0;555555,666666,0", "0,600,17;1,2345,20", 1718);
    internalTestEstimator("1,400,0;700,1000,0|1204,1455,0;1600,7000,0|10000,55555,0;555555,666666,0", "0,701,17;1,2345,20;2,54321,23", 131);
}

void ResultEstimatorTest::internalTestEstimator(const string &layerMetasStr,
        const string &endLayerOperators, uint32_t totalMatchCount,
        const string &needAggregatesStr)
{
    LayerMetas layerMetas = LayerMetasConstructor::createLayerMetas(
            _pool, layerMetasStr);
    ResultEstimator estimator;
    estimator.init(layerMetas, NULL);

    StringTokenizer st(endLayerOperators, ";",
                       StringTokenizer::TOKEN_IGNORE_EMPTY 
                       | StringTokenizer::TOKEN_TRIM);
    map<uint32_t, pair<uint32_t, uint32_t> > operators;
    for (size_t i = 0; i < st.getNumTokens(); ++i) {
        StringTokenizer st1(st[i], ",", StringTokenizer::TOKEN_IGNORE_EMPTY 
                            | StringTokenizer::TOKEN_TRIM);
        assert(st1.getNumTokens() >= 3);
        uint32_t layer = StringUtil::fromString<uint32_t>(st1[0].c_str());
        uint32_t seekedCount = StringUtil::fromString<uint32_t>(st1[1].c_str());
        uint32_t singleLayerMatchCount = StringUtil::fromString<uint32_t>(st1[2].c_str());
        
        operators[layer] = make_pair(seekedCount, singleLayerMatchCount);
    }
    vector<bool> needAggregates;
    if (!needAggregatesStr.empty()) {
        StringTokenizer st2(needAggregatesStr, ";",
                            StringTokenizer::TOKEN_IGNORE_EMPTY 
                            | StringTokenizer::TOKEN_TRIM);
        for (size_t i = 0; i < st2.getNumTokens(); ++i) {
            if (st2[i] == "true") {
                needAggregates.push_back(true);
            } else {
                needAggregates.push_back(false);
            }
        }
    }

    for (uint32_t i = 0; i < layerMetas.size(); ++i) {
        if (!needAggregates.empty()) {
            ASSERT_TRUE(estimator.needAggregate(i) == needAggregates[i]);
        }
        map<uint32_t, pair<uint32_t, uint32_t> >::iterator it = operators.find(i);
        if (it != operators.end()) {
            uint32_t layer = it->first;
            uint32_t seekedCount = it->second.first;
            uint32_t singleLayerMatchCount = it->second.second;            
            estimator.endLayer(layer, seekedCount, singleLayerMatchCount, 1.0);
        }
    }

    estimator.endSeek();
    ASSERT_EQ(totalMatchCount, estimator.getTotalMatchCount());
}

TEST_F(ResultEstimatorTest, testEndSeek) { 
    {
        LayerMetas layerMetas = LayerMetasConstructor::createLayerMetas(
                _pool, "1,4,5|5,7,1");
        ResultEstimator estimator;
        estimator.init(layerMetas, NULL);
        estimator.endLayer(0, 3, 3, 1.0);
        ASSERT_EQ((uint32_t)4, estimator.getTotalMatchCount());
        estimator.endSeek();
        ASSERT_EQ((uint32_t)7, estimator.getTotalMatchCount());
    }
    {
        LayerMetas layerMetas = LayerMetasConstructor::createLayerMetas(
                _pool, "1,4,5|5,7,1|8,8,1");
        ResultEstimator estimator;
        estimator.init(layerMetas, NULL);
        estimator.endLayer(0, 4, 2, 1.0);
        ASSERT_EQ((uint32_t)2, estimator.getTotalMatchCount());
        estimator.endSeek();
        ASSERT_EQ((uint32_t)4, estimator.getTotalMatchCount());
    }
}

void ResultEstimatorTest::checkTotalRange(const string &layerStr, 
        const string &resultStr)
{
    LayerMetas layerMetas = LayerMetasConstructor::createLayerMetas(
            _pool, layerStr);
    vector<uint32_t> layerRangeSizes;
    vector<vector<uint32_t> > coveredLayerInfo;
    // ResultEstimator::totalRangeSize(layerMetas, layerRangeSizes, coveredLayerInfo);
    StringTokenizer st(resultStr, "|", StringTokenizer::TOKEN_IGNORE_EMPTY 
                       | StringTokenizer::TOKEN_TRIM);

    ASSERT_EQ(size_t(st.getNumTokens()), layerRangeSizes.size());
    for (size_t i = 0; i < st.getNumTokens(); ++i) {
        ASSERT_EQ(autil::StringUtil::fromString<uint32_t>(st[i]),
                             layerRangeSizes[i]);
    }
}

TEST_F(ResultEstimatorTest, testInitCoveredInfo) {
    internalTestInitCoveredInfo("1,4,5|5,7,1|8,8,1", ";;");
    internalTestInitCoveredInfo("1,4,5;10,15,6|1,4,0;10,15,0|8,8,1", "1;0;");
    internalTestInitCoveredInfo("1,4,5;10,15,6|1,4,0;10,15,0|1,4,0;10,15,0", "1,2;0,2;0,1");
    internalTestInitCoveredInfo("1,4,5;10,15,6|1,16,0|1,16,0", ";2;1");
    internalTestInitCoveredInfo("1,4,5;10,15,6|1,4,0;10,16,0|1,16,0", ";;");
}

void ResultEstimatorTest::internalTestInitCoveredInfo(
        const string &layerMetasStr, const string &results)
{
    LayerMetas layerMetas = LayerMetasConstructor::createLayerMetas(
            _pool, layerMetasStr);
    std::vector<std::vector<uint32_t> > coveredLayerInfo;
    ResultEstimator::initCoveredInfo(layerMetas, coveredLayerInfo);
    std::vector<std::vector<uint32_t> > resultCoveredInfo;
    StringTokenizer st(results, ";");
    ASSERT_EQ((size_t)st.getNumTokens(), layerMetas.size());
    resultCoveredInfo.resize(layerMetas.size());
    for (size_t i = 0; i < st.getNumTokens(); ++i) {
        StringTokenizer st1(st[i], ",", StringTokenizer::TOKEN_IGNORE_EMPTY 
                            | StringTokenizer::TOKEN_TRIM);
        for (size_t j = 0; j < st1.getNumTokens(); ++j) {
            resultCoveredInfo[i].push_back(StringUtil::fromString<uint32_t>(st1[j].c_str()));
        }
    }
    ASSERT_EQ(resultCoveredInfo.size(), coveredLayerInfo.size());
    for (size_t i = 0 ; i < resultCoveredInfo.size(); ++i) {
        ASSERT_EQ(resultCoveredInfo[i].size(), coveredLayerInfo[i].size());
        for (size_t j = 0; j <resultCoveredInfo[i].size(); ++j) {
            ASSERT_EQ(resultCoveredInfo[i][j], coveredLayerInfo[i][j]);
        }
    }    
}

END_HA3_NAMESPACE(search);

