#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/config/JoinConfig.h>
#include <suez/turing/common/JoinConfigInfo.h>
using namespace std;
using namespace suez::turing;
BEGIN_HA3_NAMESPACE(config);

class JoinConfigTest : public TESTBASE {
public:
    JoinConfigTest();
    ~JoinConfigTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(config, JoinConfigTest);


JoinConfigTest::JoinConfigTest() { 
}

JoinConfigTest::~JoinConfigTest() { 
}

void JoinConfigTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void JoinConfigTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(JoinConfigTest, testJsonizeForNewFormat) { 
    HA3_LOG(DEBUG, "Begin Test!");

    string configStr = "{\n"
        "\"join_infos\":[\n"
        "{\n"
        "\"join_field\" : \"user_id\",\n"
        "\"join_cluster\" : \"user\",\n"
        "\"strong_join\" : true,\n"
        "\"use_join_cache\" : true\n"
	"},\n"
        "{\n"
        "\"join_field\" : \"item_id\",\n"
        "\"join_cluster\" : \"item\",\n"
        "\"strong_join\" : false,\n"
        "\"use_join_cache\" : false\n"
	"},\n"
	"{\n"
        "\"join_field\" : \"customer_id\",\n"
        "\"join_cluster\" : \"customer\"\n"
	"}\n"
        "]}";
    JoinConfig config;
    FromJsonString(config, configStr);
    const vector<JoinConfigInfo> &joinInfos = config.getJoinInfos();
    ASSERT_EQ(size_t(3), joinInfos.size());
    ASSERT_EQ(string("user_id"), joinInfos[0].getJoinField());
    ASSERT_EQ(string("user"), joinInfos[0].getJoinCluster());
    ASSERT_EQ(true, joinInfos[0].isStrongJoin());
    ASSERT_EQ(true, joinInfos[0].useJoinCache);
    ASSERT_EQ(string("item_id"), joinInfos[1].getJoinField());
    ASSERT_EQ(string("item"), joinInfos[1].getJoinCluster());
    ASSERT_EQ(false, joinInfos[1].isStrongJoin());
    ASSERT_EQ(false, joinInfos[1].useJoinCache);
    ASSERT_EQ(string("customer_id"), joinInfos[2].getJoinField());
    ASSERT_EQ(string("customer"), joinInfos[2].getJoinCluster());
    ASSERT_EQ(false, joinInfos[2].isStrongJoin());
    ASSERT_EQ(false, joinInfos[2].useJoinCache);

    string configStr2 = ToJsonString(config);
    JoinConfig config2;
    FromJsonString(config2, configStr2);
    ASSERT_TRUE(config == config2);
}

TEST_F(JoinConfigTest, testJsonizeForOldFormat) { 
    HA3_LOG(DEBUG, "Begin Test!");

    string configStr = "{\n"
        "\"use_join_cache\" : true,\n"
        "\"join_infos\":[\n"
        "{\n"
        "\"join_field\" : \"user_id\",\n"
        "\"join_cluster\" : \"user\",\n"
        "\"strong_join\" : true\n"
	"},\n"
	"{\n"
        "\"join_field\" : \"customer_id\",\n"
        "\"join_cluster\" : \"customer\"\n"
	"}\n"
        "]}";
    JoinConfig config;
    FromJsonString(config, configStr);
    const vector<JoinConfigInfo> &joinInfos = config.getJoinInfos();
    ASSERT_EQ(size_t(2), joinInfos.size());
    ASSERT_EQ(string("user_id"), joinInfos[0].getJoinField());
    ASSERT_EQ(string("user"), joinInfos[0].getJoinCluster());
    ASSERT_EQ(true, joinInfos[0].isStrongJoin());
    ASSERT_EQ(true, joinInfos[0].useJoinCache);
    ASSERT_EQ(string("customer_id"), joinInfos[1].getJoinField());
    ASSERT_EQ(string("customer"), joinInfos[1].getJoinCluster());
    ASSERT_EQ(false, joinInfos[1].isStrongJoin());
    ASSERT_EQ(true, joinInfos[1].useJoinCache);

    string configStr2 = ToJsonString(config);
    JoinConfig config2;
    FromJsonString(config2, configStr2);
    ASSERT_TRUE(config == config2);
}

TEST_F(JoinConfigTest, testJsonizeForOldFormatAndNewFormatCombination) { 
    HA3_LOG(DEBUG, "Begin Test!");

    string configStr = "{\n"
        "\"use_join_cache\" : true,\n"
        "\"join_infos\":[\n"
        "{\n"
        "\"join_field\" : \"user_id\",\n"
        "\"join_cluster\" : \"user\",\n"
        "\"strong_join\" : true,\n"
        "\"use_join_cache\" : true\n"
	"},\n"
        "{\n"
        "\"join_field\" : \"item_id\",\n"
        "\"join_cluster\" : \"item\",\n"
        "\"strong_join\" : false,\n"
        "\"use_join_cache\" : false\n"
	"},\n"
	"{\n"
        "\"join_field\" : \"customer_id\",\n"
        "\"join_cluster\" : \"customer\"\n"
	"}\n"
        "]}";
    JoinConfig config;
    FromJsonString(config, configStr);
    const vector<JoinConfigInfo> &joinInfos = config.getJoinInfos();
    ASSERT_EQ(size_t(3), joinInfos.size());
    ASSERT_EQ(string("user_id"), joinInfos[0].getJoinField());
    ASSERT_EQ(string("user"), joinInfos[0].getJoinCluster());
    ASSERT_EQ(true, joinInfos[0].isStrongJoin());
    ASSERT_EQ(true, joinInfos[0].useJoinCache);
    ASSERT_EQ(string("item_id"), joinInfos[1].getJoinField());
    ASSERT_EQ(string("item"), joinInfos[1].getJoinCluster());
    ASSERT_EQ(false, joinInfos[1].isStrongJoin());
    ASSERT_EQ(true, joinInfos[1].useJoinCache);
    ASSERT_EQ(string("customer_id"), joinInfos[2].getJoinField());
    ASSERT_EQ(string("customer"), joinInfos[2].getJoinCluster());
    ASSERT_EQ(false, joinInfos[2].isStrongJoin());
    ASSERT_EQ(true, joinInfos[2].useJoinCache);

    string configStr2 = ToJsonString(config);
    JoinConfig config2;
    FromJsonString(config2, configStr2);
    ASSERT_TRUE(config == config2);
}

END_HA3_NAMESPACE(config);

