#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/config/RankProfileConfig.h>
#include <suez/turing/common/FileUtil.h>
#include <ha3/test/test.h>
#include <ha3/util/NumericLimits.h>

using namespace std;
using namespace suez::turing;
BEGIN_HA3_NAMESPACE(config);

class RankProfileConfigTest : public TESTBASE {
public:
    RankProfileConfigTest();
    ~RankProfileConfigTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(config, RankProfileConfigTest);


RankProfileConfigTest::RankProfileConfigTest() { 
}

RankProfileConfigTest::~RankProfileConfigTest() { 
}

void RankProfileConfigTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void RankProfileConfigTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(RankProfileConfigTest, testJsonizeScorerInfo) {
    string scorerInfoStr = "{\n\
\"scorer_name\" : \"fakeScorer\",\n\
\"module_name\" : \"fakeModule\",\n\
\"rank_size\" : \"1000\",\n\
\"parameters\" : {\n\
    \"key1\" : \"value1\"\n\
}\n\
}";
    ScorerInfo scorerInfo;
    FromJsonString(scorerInfo, scorerInfoStr);
    ASSERT_EQ(string("fakeScorer"), scorerInfo.scorerName);
    ASSERT_EQ(string("fakeModule"), scorerInfo.moduleName);
    ASSERT_EQ((uint32_t)1000, scorerInfo.rankSize);
    ASSERT_EQ(size_t(1), scorerInfo.parameters.size());
    ASSERT_EQ(string("value1"), scorerInfo.parameters["key1"]);

    string scorerInfoStr2 = ToJsonString(scorerInfo);
    ScorerInfo scorerInfo2;
    FromJsonString(scorerInfo2, scorerInfoStr2);
    ASSERT_TRUE(scorerInfo == scorerInfo2);
}

TEST_F(RankProfileConfigTest, testJsonizeRankProfileInfo) {
    string rankProfileInfoStr = "{\n\
\"field_boost\" : {\n\
      \"phrase.title\" : 12345, \n\
      \"phrase.body\" : 54321 \n\
},\n\
\"rank_profile_name\" : \"RankProfile1\",\n\
\"scorers\" : [\n\
{\n\
      \"scorer_name\" : \"fakeScorer\",\n\
      \"module_name\" : \"fakeModule\",\n\
      \"rank_size\" : \"1000\", \n\
      \"total_rank_size\" : \"2000\"\n\
}\n\
]\n\
}";

    ScorerInfo expectedScorerInfo;
    auto defaultRankSize = util::NumericLimits<uint32_t>::max();
    ASSERT_EQ(defaultRankSize, expectedScorerInfo.rankSize);
    ASSERT_EQ(0u, expectedScorerInfo.totalRankSize);

    expectedScorerInfo.scorerName = "fakeScorer";
    expectedScorerInfo.moduleName = "fakeModule";
    expectedScorerInfo.rankSize = 1000;
    expectedScorerInfo.totalRankSize = 2000;

    map<string, uint32_t> expectedFieldBoostInfo;
    expectedFieldBoostInfo["phrase.title"] = 12345;
    expectedFieldBoostInfo["phrase.body"] = 54321;

    RankProfileInfo rankProfileInfo;

    FromJsonString(rankProfileInfo, rankProfileInfoStr);

    ASSERT_TRUE(rankProfileInfo.scorerInfos.size() == 1);
    ASSERT_EQ(1000, rankProfileInfo.scorerInfos[0].rankSize);
    ASSERT_EQ(2000, rankProfileInfo.scorerInfos[0].totalRankSize);

    ASSERT_EQ(string("RankProfile1"), rankProfileInfo.rankProfileName);
    ASSERT_TRUE(expectedFieldBoostInfo == rankProfileInfo.fieldBoostInfo);
    ASSERT_TRUE(expectedScorerInfo == rankProfileInfo.scorerInfos[0]);
}

TEST_F(RankProfileConfigTest, testJsonizeRankProfileConfig) { 
    HA3_LOG(DEBUG, "Begin Test!");

    string profilePath = TOP_BUILDDIR;
    profilePath += "/testdata/conf/rankprofile.conf";
    string configStr = FileUtil::readFile(profilePath);

    RankProfileConfig rankProfileConfig;
    FromJsonString(rankProfileConfig, configStr);
    ASSERT_EQ((size_t)1, rankProfileConfig.getModuleInfos().size());
    ASSERT_EQ((size_t)6, rankProfileConfig.getRankProfileInfos().size());

    string configStr2 = ToJsonString(rankProfileConfig);
    RankProfileConfig rankProfileConfig2;
    FromJsonString(rankProfileConfig2, configStr2);
    ASSERT_TRUE(rankProfileConfig == rankProfileConfig2);
}

END_HA3_NAMESPACE(config);

