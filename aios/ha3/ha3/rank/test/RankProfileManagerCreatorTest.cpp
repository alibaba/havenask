#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <build_service/plugin/PlugInManager.h>
#include <ha3/rank/RankProfileManagerCreator.h>
#include <ha3/config/ResourceReader.h>
#include <ha3/test/test.h>
#include <suez/turing/common/FileUtil.h>
using namespace std;
USE_HA3_NAMESPACE(util);
using namespace build_service::plugin;
using namespace suez::turing;
USE_HA3_NAMESPACE(config);

BEGIN_HA3_NAMESPACE(rank);

class RankProfileManagerCreatorTest : public TESTBASE {
public:
    RankProfileManagerCreatorTest();
    ~RankProfileManagerCreatorTest();
public:
    void setUp();
    void tearDown();
protected:
    void testRankProfileManagerFromJson();
protected:
    RankProfileManagerPtr _rankProfileManagerPtr;
    build_service::plugin::PlugInManagerPtr _plugInManagerPtr;
    config::ResourceReaderPtr _resourceReaderPtr;
    CavaPluginManagerPtr _cavaPluginManagerPtr;
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(rank, RankProfileManagerCreatorTest);

RankProfileManagerCreatorTest::RankProfileManagerCreatorTest() { 
}

RankProfileManagerCreatorTest::~RankProfileManagerCreatorTest() { 
}

void RankProfileManagerCreatorTest::setUp() 
{ 
    HA3_LOG(DEBUG, "setUp!");
    _resourceReaderPtr.reset(new ResourceReader(TEST_DATA_CONF_PATH_HA3));
}

void RankProfileManagerCreatorTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(RankProfileManagerCreatorTest, testCreateFromString)
{
    HA3_LOG(DEBUG, "Begin Test!");
    string profilePath = TOP_BUILDDIR;
    profilePath += "/testdata/conf/rankprofile.conf";
    string configStr = FileUtil::readFile(profilePath);
    RankProfileManagerCreator creator(_resourceReaderPtr, _cavaPluginManagerPtr, NULL);
    _rankProfileManagerPtr = creator.createFromString(configStr);
    testRankProfileManagerFromJson();
}


TEST_F(RankProfileManagerCreatorTest, testCreateFromStringWithEmptyString)
{
    HA3_LOG(DEBUG, "Begin Test!");
    string configStr = "";
    RankProfileManagerCreator creator(_resourceReaderPtr, _cavaPluginManagerPtr, NULL);
    _rankProfileManagerPtr = creator.createFromString(configStr);

    ASSERT_TRUE(_rankProfileManagerPtr.get());
    ASSERT_TRUE(_rankProfileManagerPtr->init(_resourceReaderPtr, _cavaPluginManagerPtr, NULL));

    RankProfile *profile = _rankProfileManagerPtr->getRankProfile("profile1");
    ASSERT_FALSE(profile);

    {    //default profile
        profile = _rankProfileManagerPtr->getRankProfile("DefaultProfile");
        ASSERT_TRUE(profile);

        ScorerInfo scorerinfo;
        ASSERT_TRUE(profile->getScorerInfo(0u, scorerinfo));
        ASSERT_EQ(string("DefaultScorer"), scorerinfo.scorerName);

        KeyValueMap parameters = scorerinfo.parameters;
        ASSERT_EQ((size_t)0, parameters.size());

        ASSERT_EQ((uint32_t)util::NumericLimits<uint32_t>::max(),
                             profile->getRankSize(0));
    }

    {    //debug query profile
        string debugProfileName = DEFAULT_DEBUG_RANK_PROFILE;
        string scorerName = DEFAULT_DEBUG_SCORER;
        profile = _rankProfileManagerPtr->getRankProfile(debugProfileName);
        ASSERT_TRUE(profile);

        ScorerInfo scorerinfo;
        ASSERT_TRUE(profile->getScorerInfo(0u, scorerinfo));
        ASSERT_EQ(string(scorerName), scorerinfo.scorerName);

        KeyValueMap parameters = scorerinfo.parameters;
        ASSERT_EQ((size_t)0, parameters.size());
        ASSERT_EQ((uint32_t)10,
                             profile->getRankSize(0));
    }
}

void RankProfileManagerCreatorTest::testRankProfileManagerFromJson()
{
    HA3_LOG(DEBUG, "Begin Test!");
    ASSERT_TRUE(_rankProfileManagerPtr.get());

    ASSERT_TRUE(_rankProfileManagerPtr->init(_resourceReaderPtr, _cavaPluginManagerPtr, NULL));

    RankProfile *profile = NULL;
    {
        profile = _rankProfileManagerPtr->getRankProfile("profile1");
        ASSERT_TRUE(profile);

        ScorerInfo scorerinfo;
        ASSERT_TRUE(profile->getScorerInfo(0u, scorerinfo));
        ASSERT_EQ(string("DefaultScorer"), scorerinfo.scorerName);

        KeyValueMap parameters = scorerinfo.parameters;
        ASSERT_EQ((size_t)2, parameters.size());
        ASSERT_EQ(string("221"), parameters["111"]);    
        ASSERT_EQ(string("441"), parameters["331"]);


        ASSERT_TRUE(profile->getScorerInfo(1u, scorerinfo));
        ASSERT_EQ(string("DefaultScorer"), scorerinfo.scorerName);

        KeyValueMap parameters1 = scorerinfo.parameters;
        ASSERT_EQ((size_t)2, parameters1.size());
        ASSERT_EQ(string("222"), parameters1["112"]);    
        ASSERT_EQ(string("442"), parameters1["332"]);

        ASSERT_TRUE(profile->getScorerInfo(2u, scorerinfo));
        ASSERT_EQ(string("FakeDefaultScorer"), scorerinfo.scorerName);

        KeyValueMap parameters2 = scorerinfo.parameters;
        ASSERT_EQ((size_t)2, parameters2.size());
        ASSERT_EQ(string("223"), parameters2["113"]);    
        ASSERT_EQ(string("443"), parameters2["333"]);

        ASSERT_EQ((uint32_t)util::NumericLimits<uint32_t>::max(),
                             profile->getRankSize(0));
        ASSERT_EQ((uint32_t)900, profile->getRankSize(1));
        ASSERT_EQ((uint32_t)100, profile->getRankSize(2));
    }
    {
        profile = _rankProfileManagerPtr->getRankProfile(DEFAULT_DEBUG_RANK_PROFILE);
        ASSERT_TRUE(profile);

        ScorerInfo scorerinfo;
        ASSERT_TRUE(profile->getScorerInfo(0u, scorerinfo));
        ASSERT_EQ(string(DEFAULT_DEBUG_SCORER), scorerinfo.scorerName);
        KeyValueMap parameters = scorerinfo.parameters;
        ASSERT_EQ((size_t)0, parameters.size());
        ASSERT_EQ((uint32_t)10, profile->getRankSize(0));
    }
}

END_HA3_NAMESPACE(rank);
