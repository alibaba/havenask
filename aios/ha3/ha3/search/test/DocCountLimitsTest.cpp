#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/DocCountLimits.h>
#include <ha3/common/Request.h>
#include <ha3/config/RankProfileInfo.h>
#include <ha3/rank/RankProfile.h>

using namespace std;
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(rank);

BEGIN_HA3_NAMESPACE(search);

class DocCountLimitsTest : public TESTBASE {
public:
    DocCountLimitsTest();
    ~DocCountLimitsTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(search, DocCountLimitsTest);


DocCountLimitsTest::DocCountLimitsTest() {
}

DocCountLimitsTest::~DocCountLimitsTest() {
}

void DocCountLimitsTest::setUp() {
    HA3_LOG(DEBUG, "setUp!");
}

void DocCountLimitsTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(DocCountLimitsTest, testGetRuntimeTopK) {
    Request request;

    request.setDistinctClause(NULL);
    ASSERT_EQ((uint32_t)0,
                         DocCountLimits::getRuntimeTopK(&request, 0, 0));
    ASSERT_EQ((uint32_t)200,
                         DocCountLimits::getRuntimeTopK(&request, 200, 100));
    ASSERT_EQ((uint32_t)200,
                         DocCountLimits::getRuntimeTopK(&request, 100, 200));
    ASSERT_EQ((uint32_t)MAX_RERANK_SIZE,
                         DocCountLimits::getRuntimeTopK(&request,
                                 MAX_RERANK_SIZE + 1, 200));

    DistinctDescription *distDesc = new DistinctDescription(
            DEFAULT_DISTINCT_MODULE_NAME, "type", 1, 1, 100);
    DistinctClause *distClause = new DistinctClause;
    distClause->addDistinctDescription(distDesc);
    request.setDistinctClause(distClause);
    ASSERT_EQ((uint32_t)100,
                         DocCountLimits::getRuntimeTopK(&request, 50, 50));
    ASSERT_EQ((uint32_t)200,
                         DocCountLimits::getRuntimeTopK(&request, 50, 200));

    // dist item count exceeds MAX_RERANK_SIZE
    distDesc->setMaxItemCount(MAX_RERANK_SIZE + 100);
    ASSERT_EQ((uint32_t)MAX_RERANK_SIZE,
                         DocCountLimits::getRuntimeTopK(&request, 50, 200));
}

TEST_F(DocCountLimitsTest, testGetNewSize) {
    uint32_t partCount = 1;
    uint32_t singleFromQuery = 0;
    uint32_t totalFromQuery = 0;
    uint32_t scorerId = 0;
    uint32_t defaultSize = 0;

    totalFromQuery = 100;
    EXPECT_EQ(100u, DocCountLimits::getNewSize(partCount, singleFromQuery,
                    totalFromQuery, NULL, scorerId, defaultSize, true));

    partCount = 2;
    EXPECT_EQ(50u, DocCountLimits::getNewSize(partCount, singleFromQuery,
                    totalFromQuery, NULL, scorerId, defaultSize, true));

    totalFromQuery = std::numeric_limits<uint32_t>::max();
    EXPECT_EQ(std::numeric_limits<uint32_t>::max() / 2 + 1, DocCountLimits::getNewSize(partCount, singleFromQuery,
                    totalFromQuery, NULL, scorerId, defaultSize, true));

    totalFromQuery = 100;
    defaultSize = 60;
    EXPECT_EQ(60u, DocCountLimits::getNewSize(partCount, singleFromQuery,
                    totalFromQuery, NULL, scorerId, defaultSize, false));

    partCount = 3;
    EXPECT_EQ(34u, DocCountLimits::getNewSize(partCount, singleFromQuery,
                    totalFromQuery, NULL, scorerId, defaultSize, true));

    totalFromQuery = 0;
    singleFromQuery = 200;
    EXPECT_EQ(200u, DocCountLimits::getNewSize(partCount, singleFromQuery,
                    totalFromQuery, NULL, scorerId, defaultSize, true));

    defaultSize = 300;
    singleFromQuery = 0;
    EXPECT_EQ(300u, DocCountLimits::getNewSize(partCount, singleFromQuery,
                    totalFromQuery, NULL, scorerId, defaultSize, true));

    ScorerInfo scorerInfo;
    scorerInfo.rankSize = 480;
    scorerInfo.totalRankSize = 401;
    RankProfile rankProfile("");
    rankProfile._scorerInfos.push_back(scorerInfo);
    rankProfile._scorerInfos.push_back(scorerInfo);
    defaultSize = 300;
    partCount = 8;
    EXPECT_EQ(51u, DocCountLimits::getNewSize(partCount, singleFromQuery,
                    totalFromQuery, &rankProfile, scorerId, defaultSize, true));
    EXPECT_EQ(480u, DocCountLimits::getNewSize(partCount, singleFromQuery,
                    totalFromQuery, &rankProfile, scorerId, defaultSize, false));

    rankProfile._scorerInfos.clear();
    scorerInfo.rankSize = 480;
    scorerInfo.totalRankSize = 0;
    rankProfile._scorerInfos.push_back(scorerInfo);
    defaultSize = 300;
    partCount = 8;
    EXPECT_EQ(480u, DocCountLimits::getNewSize(partCount, singleFromQuery,
                    totalFromQuery, &rankProfile, scorerId, defaultSize, true));

    rankProfile._scorerInfos.clear();
    scorerInfo.rankSize = 480;
    scorerInfo.totalRankSize = std::numeric_limits<uint32_t>::max();
    rankProfile._scorerInfos.push_back(scorerInfo);
    defaultSize = 300;
    partCount = 8;
    EXPECT_EQ(scorerInfo.totalRankSize / 8 + 1, DocCountLimits::getNewSize(partCount, singleFromQuery,
                    totalFromQuery, &rankProfile, scorerId, defaultSize, true));

    rankProfile._scorerInfos.clear();
    scorerInfo.rankSize = 480;
    scorerInfo.totalRankSize = 0;
    rankProfile._scorerInfos.push_back(scorerInfo);
    defaultSize = 300;
    partCount = 8;
    EXPECT_EQ(480u, DocCountLimits::getNewSize(partCount, singleFromQuery,
                    totalFromQuery, &rankProfile, scorerId, defaultSize, true));
}

TEST_F(DocCountLimitsTest, testGetRequiredTopK) {
    ClusterConfigInfo clusterConfigInfo;
    ConfigClause *configClause = new ConfigClause;
    configClause->setStartOffset(5);
    configClause->setHitCount(10);
    Request request;
    request.setConfigClause(configClause);
    uint32_t partCount = 1;
    bool useTotal = true;
    for (; partCount < 10; partCount++) {
        configClause->setSearcherReturnHits(0);
        ASSERT_EQ((uint32_t)15, DocCountLimits::getRequiredTopK(&request, clusterConfigInfo,
                        partCount, useTotal, NULL));

        configClause->setSearcherReturnHits(100);
        ASSERT_EQ((uint32_t)15, DocCountLimits::getRequiredTopK(&request, clusterConfigInfo,
                        partCount, useTotal, NULL));

        configClause->setSearcherReturnHits(10);
        ASSERT_EQ((uint32_t)10, DocCountLimits::getRequiredTopK(&request, clusterConfigInfo,
                        partCount, useTotal, NULL));
    }

    clusterConfigInfo._returnHitThreshold = 300;
    clusterConfigInfo._returnHitRatio = 1.2;
    configClause->setStartOffset(0);
    configClause->setHitCount(500);
    configClause->setSearcherReturnHits(0);
    partCount = 2;
    ASSERT_EQ((uint32_t)500 / 2 * 1.2, DocCountLimits::getRequiredTopK(&request, clusterConfigInfo,
                    partCount, useTotal, NULL));

    ASSERT_EQ((uint32_t)500, DocCountLimits::getRequiredTopK(&request, clusterConfigInfo,
                    partCount, false, NULL));

    configClause->setSearcherReturnHits(30);
    ASSERT_EQ((uint32_t)30, DocCountLimits::getRequiredTopK(&request, clusterConfigInfo,
                    partCount, false, NULL));

    clusterConfigInfo._returnHitRatio = 0.0;
    configClause->setSearcherReturnHits(0);
    ASSERT_EQ((uint32_t)500, DocCountLimits::getRequiredTopK(&request, clusterConfigInfo,
                    partCount, useTotal, NULL));

    // invalid ratio
    clusterConfigInfo._returnHitRatio = RETURN_HIT_REWRITE_RATIO;
    ASSERT_EQ((uint32_t)500, DocCountLimits::getRequiredTopK(&request, clusterConfigInfo,
                    partCount, useTotal, NULL));

    // ratio > partCount
    clusterConfigInfo._returnHitRatio = 2.3;
    ASSERT_EQ((uint32_t)500, DocCountLimits::getRequiredTopK(&request, clusterConfigInfo,
                    partCount, useTotal, NULL));

    // return hit specified
    configClause->setSearcherReturnHits(271);
    ASSERT_EQ((uint32_t)271, DocCountLimits::getRequiredTopK(&request, clusterConfigInfo,
                    partCount, useTotal, NULL));
    configClause->setSearcherReturnHits(600);
    ASSERT_EQ((uint32_t)500, DocCountLimits::getRequiredTopK(&request, clusterConfigInfo,
                    partCount, useTotal, NULL));

    // not cross threshold
    clusterConfigInfo._returnHitThreshold = 600;
    ASSERT_EQ((uint32_t)500, DocCountLimits::getRequiredTopK(&request, clusterConfigInfo,
                    partCount, useTotal, NULL));
}

TEST_F(DocCountLimitsTest, testGetDegradeSize) {
    EXPECT_EQ(200, DocCountLimits::getDegradeSize(0.0f, 100, 200));
    EXPECT_EQ(190, DocCountLimits::getDegradeSize(0.1f, 100, 200));
    EXPECT_EQ(150, DocCountLimits::getDegradeSize(0.5f, 100, 200));
    EXPECT_EQ(100, DocCountLimits::getDegradeSize(1.0f, 100, 200));
    EXPECT_EQ(100, DocCountLimits::getDegradeSize(2.0f, 100, 200));

    EXPECT_EQ(100, DocCountLimits::getDegradeSize(0.5f, 200, 100));
    EXPECT_EQ(200, DocCountLimits::getDegradeSize(0.5f, 0, 200));
}

TEST_F(DocCountLimitsTest, testInit) {
    ClusterConfigInfo clusterConfigInfo;
    ConfigClause *configClause = new ConfigClause;
    configClause->setRankSize(200);
    configClause->setRerankSize(400);
    Request request;
    request.setConfigClause(configClause);
    {
        DocCountLimits limits;
        limits.init(&request, NULL, clusterConfigInfo, 10, NULL);
        EXPECT_EQ(200, limits.rankSize);
        EXPECT_EQ(400, limits.rerankSize);
    }
    {
        request.setDegradeLevel(0.6f, 100, 200);
        DocCountLimits limits;
        limits.init(&request, NULL, clusterConfigInfo, 10, NULL);
        EXPECT_EQ(140, limits.rankSize);
        EXPECT_EQ(280, limits.rerankSize);
    }
}

TEST_F(DocCountLimitsTest, testInitWithPartCnt) {
    {
        ClusterConfigInfo clusterConfigInfo;
        ConfigClause *configClause = new ConfigClause();
        configClause->setTotalRankSize(2000);
        configClause->setRerankSize(400);
        ClusterClause *clusterClause = new ClusterClause();
        clusterClause->setTotalSearchPartCnt(2);
        Request request;
        request.setConfigClause(configClause);
        DocCountLimits limits;
        limits.init(&request, NULL, clusterConfigInfo, 10, NULL);
        EXPECT_EQ(200, limits.rankSize);
        EXPECT_EQ(400, limits.rerankSize);
        delete clusterClause;
    }
    {
        ClusterConfigInfo clusterConfigInfo;
        ConfigClause *configClause = new ConfigClause();
        configClause->setTotalRankSize(2000);
        configClause->setRerankSize(400);
        ClusterClause *clusterClause = new ClusterClause();
        clusterClause->setTotalSearchPartCnt(2);
        Request request;
        request.setConfigClause(configClause);

        vector<pair<hashid_t, hashid_t> > partids;
        partids.push_back({0, 0});
        clusterClause->addClusterPartIds(string("cluster"), partids);

        request.setClusterClause(clusterClause);
        DocCountLimits limits;
        limits.init(&request, NULL, clusterConfigInfo, 10, NULL);
        EXPECT_EQ(1000, limits.rankSize);
        EXPECT_EQ(400, limits.rerankSize);
    }
}


END_HA3_NAMESPACE(search);
