#include "suez/worker/KMonitorMetaParser.h"

#include "suez/sdk/KMonitorMetaInfo.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;

namespace suez {

static const string ROLE_TYPE_QRS = "qrs";
static const string ROLE_TYPE_SEARCHER = "searcher";

class KMonitorMetaParserTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
};

void KMonitorMetaParserTest::setUp() {}

void KMonitorMetaParserTest::tearDown() {}

TEST_F(KMonitorMetaParserTest, testParse) {
    {
        KMonitorMetaParam param;
        param.serviceName = "mainse_search_graph_pre";
        param.amonitorPath = "na130/qrs";
        param.partId = "0";
        param.roleType = ROLE_TYPE_QRS;
        KMonitorMetaInfo result;
        ASSERT_TRUE(KMonitorMetaParser::parse(param, result));
        EXPECT_EQ("tisplus.amon.mainse_search_graph_pre.ha3suez.qrs", result.metricsPrefix);
        map<string, string> tagsMap = {{"zone", "qrs"}, {"role", "0"}, {"instance", "na130"}};
        ASSERT_TRUE(result.metricsPath.empty());
        EXPECT_EQ(tagsMap, result.tagsMap);
    }
    {
        KMonitorMetaParam param;
        param.serviceName = "mainse_search_graph_pre";
        param.amonitorPath = "na130/good_searcher";
        param.partId = "1";
        param.roleType = ROLE_TYPE_SEARCHER;
        KMonitorMetaInfo result;
        ASSERT_TRUE(KMonitorMetaParser::parse(param, result));
        EXPECT_EQ("tisplus.amon.mainse_search_graph_pre.ha3suez.searcher", result.metricsPrefix);
        map<string, string> tagsMap = {{"zone", "good_searcher"}, {"role", "1"}, {"instance", "na130"}};
        ASSERT_TRUE(result.metricsPath.empty());
        EXPECT_EQ(tagsMap, result.tagsMap);
    }
    {
        KMonitorMetaParam param;
        param.serviceName = "sophon.search4eleme_mainsch_v1.domain^na63cloud_gray@sophon_env^online";
        param.amonitorPath = "search4eleme_mainsch_v1_na63cloud_gray/search4eleme_mainsch_v1_qrs";
        param.partId = "1";
        param.roleType = ROLE_TYPE_QRS;
        KMonitorMetaInfo result;
        ASSERT_TRUE(KMonitorMetaParser::parse(param, result));
        EXPECT_EQ("sophon.ha3suez.qrs", result.metricsPrefix);
        map<string, string> tagsMap = {{"service_type", "online"},
                                       {"domain", "na63cloud_gray"},
                                       {"sophon_env", "online"},
                                       {"app", "search4eleme_mainsch_v1"},
                                       {"zone", "search4eleme_mainsch_v1_qrs"},
                                       {"role", "1"},
                                       {"instance", "search4eleme_mainsch_v1_na63cloud_gray"}};
        ASSERT_TRUE(result.metricsPath.empty());
        EXPECT_EQ(tagsMap, result.tagsMap);
    }
    {
        KMonitorMetaParam param;
        param.serviceName = "sophon.search4eleme_mainsch_v1.domain^na63cloud_gray@sophon_env^online";
        param.amonitorPath = "search4eleme_mainsch_v1_na63cloud_gray/mainsch";
        param.partId = "1";
        param.roleType = ROLE_TYPE_SEARCHER;
        KMonitorMetaInfo result;
        ASSERT_TRUE(KMonitorMetaParser::parse(param, result));
        EXPECT_EQ("sophon.ha3suez.searcher", result.metricsPrefix);
        map<string, string> tagsMap = {{"service_type", "online"},
                                       {"domain", "na63cloud_gray"},
                                       {"sophon_env", "online"},
                                       {"app", "search4eleme_mainsch_v1"},
                                       {"zone", "mainsch"},
                                       {"role", "1"},
                                       {"instance", "search4eleme_mainsch_v1_na63cloud_gray"}};
        ASSERT_TRUE(result.metricsPath.empty());
        EXPECT_EQ(tagsMap, result.tagsMap);
    }
}

TEST_F(KMonitorMetaParserTest, testHa3Default) {
    {
        KMonitorMetaInfo result;
        ASSERT_TRUE(KMonitorMetaParser::parseDefault("app", ROLE_TYPE_QRS, result));
        EXPECT_EQ("tisplus.amon.app.ha3suez.qrs", result.metricsPrefix);
        EXPECT_EQ("app", result.serviceName);
        EXPECT_TRUE(result.metricsPath.empty());
        EXPECT_TRUE(result.tagsMap.empty());
    }
    {
        KMonitorMetaInfo result;
        ASSERT_TRUE(KMonitorMetaParser::parseDefault("app", ROLE_TYPE_SEARCHER, result));
        EXPECT_EQ("tisplus.amon.app.ha3suez.searcher", result.metricsPrefix);
        EXPECT_EQ("app", result.serviceName);
        EXPECT_TRUE(result.metricsPath.empty());
        EXPECT_TRUE(result.tagsMap.empty());
    }
}

TEST_F(KMonitorMetaParserTest, testTisplus) {
    string a = "sophon.search4eleme_mainsch_v1.domain^na63cloud_gray@sophon_env^online";
    {
        KMonitorMetaInfo result;
        vector<string> patternVec = {"sophon", "search4eleme_mainsch_v1", "domain^na63cloud_gray@sophon_env^online"};
        ASSERT_TRUE(KMonitorMetaParser::parseTisplus(patternVec, ROLE_TYPE_QRS, result));
        EXPECT_EQ("sophon.ha3suez.qrs", result.metricsPrefix);
        EXPECT_EQ("sophon", result.serviceName);
        map<string, string> tagsMap = {{"service_type", "online"},
                                       {"domain", "na63cloud_gray"},
                                       {"sophon_env", "online"},
                                       {"app", "search4eleme_mainsch_v1"}};
        ASSERT_TRUE(result.metricsPath.empty());
        EXPECT_EQ(tagsMap, result.tagsMap);
    }
    {
        KMonitorMetaInfo result;
        vector<string> patternVec = {"sophon", "search4eleme_mainsch_v1", "domain^na63cloud_gray@sophon_env^online"};
        ASSERT_TRUE(KMonitorMetaParser::parseTisplus(patternVec, ROLE_TYPE_SEARCHER, result));
        EXPECT_EQ("sophon.ha3suez.searcher", result.metricsPrefix);
        EXPECT_EQ("sophon", result.serviceName);
        map<string, string> tagsMap = {{"service_type", "online"},
                                       {"domain", "na63cloud_gray"},
                                       {"sophon_env", "online"},
                                       {"app", "search4eleme_mainsch_v1"}};
        EXPECT_TRUE(result.metricsPath.empty());
        EXPECT_EQ(tagsMap, result.tagsMap);
    }
    {
        KMonitorMetaInfo result;
        vector<string> patternVec = {"sophon", "search4eleme_mainsch_v1", "domain^na63cloud_gray@sophon_env"};
        ASSERT_FALSE(KMonitorMetaParser::parseTisplus(patternVec, ROLE_TYPE_SEARCHER, result));
    }
    {
        KMonitorMetaInfo result;
        vector<string> patternVec = {"sophon", "", ""};
        ASSERT_TRUE(KMonitorMetaParser::parseTisplus(patternVec, ROLE_TYPE_SEARCHER, result));
        EXPECT_EQ("sophon.ha3suez.searcher", result.metricsPrefix);
        EXPECT_EQ("sophon", result.serviceName);
        map<string, string> tagsMap = {{"service_type", "online"}, {"app", ""}};
        EXPECT_TRUE(result.metricsPath.empty());
        EXPECT_EQ(tagsMap, result.tagsMap);
    }
}

} // namespace suez
