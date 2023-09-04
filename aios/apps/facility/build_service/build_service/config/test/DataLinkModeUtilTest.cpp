#include "build_service/config/DataLinkModeUtil.h"

#include "build_service/config/CLIOptionNames.h"
#include "build_service/test/unittest.h"

using namespace std;
using namespace testing;

namespace build_service { namespace config {

class DataLinkModeUtilTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp();
    void tearDown();
};

void DataLinkModeUtilTest::setUp() {}

void DataLinkModeUtilTest::tearDown() {}

TEST_F(DataLinkModeUtilTest, testAdaptsLegacRealtimeInfoToDataLinkMode)
{
    {
        KeyValueMap legacyRtInfo;
        legacyRtInfo[REALTIME_MODE] = REALTIME_SERVICE_RAWDOC_RT_BUILD_MODE;
        ASSERT_FALSE(DataLinkModeUtil::adaptsRealtimeInfoToDataLinkMode(string("invalid_str"), legacyRtInfo));
        ASSERT_TRUE(DataLinkModeUtil::adaptsRealtimeInfoToDataLinkMode(string("fp_inp"), legacyRtInfo));
        ASSERT_TRUE(DataLinkModeUtil::adaptsRealtimeInfoToDataLinkMode(string(""), legacyRtInfo));
        ASSERT_EQ(REALTIME_SERVICE_RAWDOC_RT_BUILD_MODE, legacyRtInfo[REALTIME_MODE]);
        ASSERT_TRUE(DataLinkModeUtil::adaptsRealtimeInfoToDataLinkMode(string("npc"), legacyRtInfo));
        ASSERT_EQ(REALTIME_SERVICE_NPC_MODE, legacyRtInfo[REALTIME_MODE]);
    }
    {
        KeyValueMap legacyRtInfo;
        legacyRtInfo[REALTIME_MODE] = REALTIME_SERVICE_MODE;
        ASSERT_FALSE(DataLinkModeUtil::adaptsRealtimeInfoToDataLinkMode(string("fp_inp"), legacyRtInfo));
        ASSERT_FALSE(DataLinkModeUtil::adaptsRealtimeInfoToDataLinkMode(string("npc"), legacyRtInfo));
        ASSERT_TRUE(DataLinkModeUtil::adaptsRealtimeInfoToDataLinkMode(string("normal"), legacyRtInfo));
        ASSERT_EQ(REALTIME_SERVICE_MODE, legacyRtInfo[REALTIME_MODE]);
    }
    {
        KeyValueMap legacyRtInfo;
        legacyRtInfo[REALTIME_MODE] = REALTIME_SERVICE_NPC_MODE;
        ASSERT_FALSE(DataLinkModeUtil::adaptsRealtimeInfoToDataLinkMode(string("fp_inp"), legacyRtInfo));
        ASSERT_FALSE(DataLinkModeUtil::adaptsRealtimeInfoToDataLinkMode(string("normal"), legacyRtInfo));
        ASSERT_TRUE(DataLinkModeUtil::adaptsRealtimeInfoToDataLinkMode(string("npc"), legacyRtInfo));
        ASSERT_EQ(REALTIME_SERVICE_NPC_MODE, legacyRtInfo[REALTIME_MODE]);
    }
}

TEST_F(DataLinkModeUtilTest, testNPCMode)
{
    {
        KeyValueMap kvMap;
        kvMap[REALTIME_MODE] = REALTIME_SERVICE_RAWDOC_RT_BUILD_MODE;
        ASSERT_FALSE(DataLinkModeUtil::isDataLinkNPCMode(kvMap));
        kvMap[REALTIME_MODE] = REALTIME_SERVICE_MODE;
        ASSERT_FALSE(DataLinkModeUtil::isDataLinkNPCMode(kvMap));
        kvMap[REALTIME_MODE] = REALTIME_SERVICE_NPC_MODE;
        ASSERT_TRUE(DataLinkModeUtil::isDataLinkNPCMode(kvMap));
    }
    {
        KeyValueMap kvMap;
        ASSERT_FALSE(DataLinkModeUtil::isDataLinkNPCMode(kvMap));
    }
    {
        KeyValueMap kvMap;
        kvMap[DATA_LINK_MODE] = "normal";
        ASSERT_FALSE(DataLinkModeUtil::isDataLinkNPCMode(kvMap));
        kvMap[DATA_LINK_MODE] = "fp_inp";
        ASSERT_FALSE(DataLinkModeUtil::isDataLinkNPCMode(kvMap));
        kvMap[DATA_LINK_MODE] = "npc";
        ASSERT_TRUE(DataLinkModeUtil::isDataLinkNPCMode(kvMap));
    }
}

}} // namespace build_service::config
