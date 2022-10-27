#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <suez/turing/common/FileUtil.h>
#include <ha3/test/test.h>
#include <ha3/config/ClusterConfigInfo.h>
#include <indexlib/indexlib.h>

using namespace std;
using namespace suez::turing;
BEGIN_HA3_NAMESPACE(config);

class ClusterConfigInfoTest : public TESTBASE {
public:
    ClusterConfigInfoTest();
    ~ClusterConfigInfoTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(config, ClusterConfigInfoTest);


ClusterConfigInfoTest::ClusterConfigInfoTest() { 
}

ClusterConfigInfoTest::~ClusterConfigInfoTest() { 
}

void ClusterConfigInfoTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void ClusterConfigInfoTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}


TEST_F(ClusterConfigInfoTest, testHashMode) {
    { // empty hash field
        string hashModeStr = R"({
        "hash_field":"",
        "hash_function":"fun"
})";
        HashMode hashMode;
        EXPECT_NO_THROW(FromJsonString(hashMode, hashModeStr));
        EXPECT_FALSE(hashMode.validate());
    }
    { // empty hash field
        string hashModeStr = R"({
})";
        HashMode hashMode;
        EXPECT_NO_THROW(FromJsonString(hashMode, hashModeStr));
        EXPECT_FALSE(hashMode.validate());
    }
    { // empty hash name
        string hashModeStr = R"({
        "hash_field":"aa",
        "hash_function":""
})";
        HashMode hashMode;
        EXPECT_NO_THROW(FromJsonString(hashMode, hashModeStr));
        EXPECT_FALSE(hashMode.validate());
    }
    { // normal
        string hashModeStr = R"({
        "hash_field":"abc"
})";
        HashMode hashMode;
        EXPECT_NO_THROW(FromJsonString(hashMode, hashModeStr));
        EXPECT_TRUE(hashMode.validate());
        EXPECT_EQ("HASH", hashMode._hashFunction);
        EXPECT_EQ("abc", hashMode._hashField);
        EXPECT_EQ(1, hashMode._hashFields.size());
        EXPECT_EQ("abc", hashMode._hashFields[0]);
    }

    { // normal
        string hashModeStr = R"({
        "hash_field":"abc",
        "hash_function":"fun"
})";
        HashMode hashMode;
        EXPECT_NO_THROW(FromJsonString(hashMode, hashModeStr));
        EXPECT_TRUE(hashMode.validate());
        EXPECT_EQ("fun", hashMode._hashFunction);
        EXPECT_EQ("abc", hashMode._hashField);
        EXPECT_EQ(1, hashMode._hashFields.size());
        EXPECT_EQ("abc", hashMode._hashFields[0]);
    }
    { // normal with multi hash fields
        string hashModeStr = R"({
        "hash_fields":["abc", "ef"],
        "hash_function":"fun",
        "hash_params": {"a":"b"}
})";
        HashMode hashMode;
        EXPECT_NO_THROW(FromJsonString(hashMode, hashModeStr));
        EXPECT_TRUE(hashMode.validate());
        EXPECT_EQ("fun", hashMode._hashFunction);
        EXPECT_EQ("", hashMode._hashField);
        EXPECT_EQ(2, hashMode._hashFields.size());
        EXPECT_EQ("abc", hashMode._hashFields[0]);
        EXPECT_EQ("ef", hashMode._hashFields[1]);
        EXPECT_EQ("b", hashMode._hashParams["a"]);
    }

}

TEST_F(ClusterConfigInfoTest, testSerilizeAndDeserilize) { 
    HA3_LOG(DEBUG, "Begin Test!");
    string fileName = string(TEST_DATA_PATH) + "/config_test/complete_cluster.json";
    string str = FileUtil::readFile(fileName);

    ClusterConfigInfo info;
    ASSERT_EQ(RETURN_HIT_REWRITE_THRESHOLD, info._returnHitThreshold);
    ASSERT_EQ(RETURN_HIT_REWRITE_RATIO, info._returnHitRatio);

    FromJsonString(info, str);
    ASSERT_EQ(string("auctionTable"), info.getTableName());
    ASSERT_EQ(2000, info._returnHitThreshold);
    ASSERT_EQ(1.5, info._returnHitRatio);

    const HashMode &hashMode = info.getHashMode();
    ASSERT_EQ(string("md5"), hashMode._hashFunction);
    ASSERT_EQ(string("uri"), hashMode._hashField);

    string jsonString = ToJsonString(info);
    ClusterConfigInfo info2;
    FromJsonString(info2, jsonString);

    ASSERT_EQ(POOL_TRUNK_SIZE, info2._poolTrunkSize);
    ASSERT_EQ(POOL_RECYCLE_SIZE_LIMIT, info2._poolRecycleSizeLimit);
    ASSERT_EQ(POOL_MAX_COUNT, info2._poolMaxCount);
}

TEST_F(ClusterConfigInfoTest, testSerilizeAndDeserilizeWithPoolConfig) { 
    HA3_LOG(DEBUG, "Begin Test!");
    string fileName = string(TEST_DATA_PATH) + "/config_test/complete_cluster_with_pool_config.json";
    string str = FileUtil::readFile(fileName);

    ClusterConfigInfo info;
    ASSERT_EQ(RETURN_HIT_REWRITE_THRESHOLD, info._returnHitThreshold);
    ASSERT_EQ(RETURN_HIT_REWRITE_RATIO, info._returnHitRatio);

    FromJsonString(info, str);
    ASSERT_EQ(string("auctionTable"), info.getTableName());
    ASSERT_EQ(2000, info._returnHitThreshold);
    ASSERT_EQ(1.5, info._returnHitRatio);

    const HashMode &hashMode = info.getHashMode();
    ASSERT_EQ(string("md5"), hashMode._hashFunction);
    ASSERT_EQ(string("uri"), hashMode._hashField);

    string jsonString = ToJsonString(info);
    ClusterConfigInfo info2;
    FromJsonString(info2, jsonString);

    ASSERT_EQ(400, info2._poolTrunkSize);
    ASSERT_EQ(800, info2._poolRecycleSizeLimit);
    ASSERT_EQ(300, info2._poolMaxCount);
}

TEST_F(ClusterConfigInfoTest, testSerilizeAndDeserilizeWithoutSomeSection) { 
    HA3_LOG(DEBUG, "Begin Test!");
    string fileName = string(TEST_DATA_PATH) + "/config_test/simple_cluster.json";
    string str = FileUtil::readFile(fileName);

    ClusterConfigInfo info;
    FromJsonString(info, str);
    ASSERT_EQ(string("auctionTable"), info.getTableName());
    
    const HashMode &hashMode = info.getHashMode();
    ASSERT_EQ(string("md5"), hashMode._hashFunction);
    ASSERT_EQ(string("uri"), hashMode._hashField);

    string jsonString = ToJsonString(info);
    ClusterConfigInfo info2;
    FromJsonString(info2, jsonString);
}

TEST_F(ClusterConfigInfoTest, testIncWithRealtimeMode) { 
    HA3_LOG(DEBUG, "Begin Test!");
    string fileName = string(TEST_DATA_PATH) + "/config_test/inc_with_realtime_cluster.json";
    string str = FileUtil::readFile(fileName);

    ClusterConfigInfo info;
    FromJsonString(info, str);
    ASSERT_EQ(string("auctionTable"), info.getTableName());
    
    const HashMode &hashMode = info.getHashMode();
    ASSERT_EQ(string("md5"), hashMode._hashFunction);
    ASSERT_EQ(string("uri"), hashMode._hashField);

    string jsonString = ToJsonString(info);
    ClusterConfigInfo info2;
    FromJsonString(info2, jsonString);
}

TEST_F(ClusterConfigInfoTest, testNewIncWithRealtimeMode) {
    HA3_LOG(DEBUG, "Begin Test!");
    string fileName = string(TEST_DATA_PATH) + "/config_test/new_inc_with_realtime_cluster.json";
    string str = FileUtil::readFile(fileName);

    ClusterConfigInfo info;
    FromJsonString(info, str);
    ASSERT_EQ(string("auctionTable"), info.getTableName());

    const HashMode &hashMode = info.getHashMode();
    ASSERT_EQ(string("md5"), hashMode._hashFunction);
    ASSERT_EQ(string("uri"), hashMode._hashField);
}

TEST_F(ClusterConfigInfoTest, testCheck) {
    {
        ClusterConfigInfo info;
        ASSERT_TRUE(!info.check());
    }
    {
        ClusterConfigInfo info;
        info._hashMode._hashFields.push_back("abc");
        ASSERT_TRUE(info.check());
    }
}

TEST_F(ClusterConfigInfoTest, testCheckJoinConfig) {
        ClusterConfigInfo info;
        info._hashMode._hashFields.push_back("abc");
        ASSERT_TRUE(info.check());

        info._joinConfig.addJoinInfo(JoinConfigInfo(
                        false, "cluster1", "", true));
        ASSERT_TRUE(!info.check());
}

END_HA3_NAMESPACE(config);

