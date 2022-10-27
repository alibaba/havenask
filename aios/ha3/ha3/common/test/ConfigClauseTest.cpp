#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/test/test.h>
#include <ha3/common/ConfigClause.h>
#include <string>
#include <ha3/common/ConfigClause.h>
#include <ha3/common/PBResultFormatter.h>
#include <ha3/common/CompressTypeConvertor.h>

using namespace std;

USE_HA3_NAMESPACE(util);
BEGIN_HA3_NAMESPACE(common);

class ConfigClauseTest : public TESTBASE {
public:
    ConfigClauseTest();
    ~ConfigClauseTest();
public:
    void setUp();
    void tearDown();
protected:
    void serializeRequest(const ConfigClause& request,
                          ConfigClause& request2);
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(common, ConfigClauseTest);


static int sTestCount = 0;

ConfigClauseTest::ConfigClauseTest() { 
}

ConfigClauseTest::~ConfigClauseTest() { 
}

void ConfigClauseTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void ConfigClauseTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
    sTestCount++;
}

TEST_F(ConfigClauseTest, testAddClusterName) {
    HA3_LOG(DEBUG, "Begin Test!");
    ConfigClause cc;
    cc.addClusterName("aaa");
    cc.addClusterName("bbb");
    cc.addClusterName("aaa");

    ASSERT_EQ(size_t(2), cc.getClusterNameCount());
    ASSERT_EQ(string("aaa.default"), cc.getClusterName(0));
    ASSERT_EQ(string("bbb.default"), cc.getClusterName(1));
}

TEST_F(ConfigClauseTest, testSerializeAndDeserialize) { 
    HA3_LOG(DEBUG, "Begin Test!");
    string originalString = "table:phrase, start:10, hit:100, rerank:20"
                            "searcher_return_hits:5,format:protobuf, analyzer:simple,"
                            "cacheinfo:y, no_summary:yes, dedup:no, no_tokenize_indexes:index1;index2,"
                            "sourceid:source1, batch_score:false, optimize_rank:false, optimize_comparator:false,"
                            "debug_query_key:123456,"
                            "default_index:test_index, default_operator:AND,"
                            "research_threshold:1000,"
                            "rerank_hint: true";

    ConfigClause requestConfig;
    requestConfig.setOriginalString(originalString);
    requestConfig.addClusterName("phrase");
    requestConfig.setStartOffset(10);
    requestConfig.setHitCount(100);
    requestConfig.setRankSize(200);
    requestConfig.setRerankSize(300);
    requestConfig.setResultFormatSetting("protobuf");
    requestConfig.setPhaseNumber(SEARCH_PHASE_TWO);
    requestConfig.setTrace("WARN");
    requestConfig.setRankTrace("DEBUG");
    requestConfig.addKVPair("key1", "value1");
    requestConfig.addKVPair("key2", "value2");
    requestConfig.setQrsChainName("qrsChain");
    requestConfig.setSummaryProfileName("summaryProfile");
    requestConfig.setDefaultIndexName("test_index");
    requestConfig.setDefaultOP("AND");
    requestConfig.setAnalyzerName("simple");
    requestConfig.setRpcTimeOut(123456);
    requestConfig.setDisplayCacheInfo("yes");
    requestConfig.setSearcherReturnHits(5);
    requestConfig.setNoSummary("yes");
    requestConfig.setDoDedup("no");
    requestConfig.setBatchScore("no");
    requestConfig.setOptimizeRank("no");
    requestConfig.setOptimizeComparator("no");
    requestConfig.setDebugQueryKey("123456");
    requestConfig.setSourceId("source1");
    requestConfig.setActualHitsLimit(50);
    requestConfig.addNoTokenizeIndex("index1");
    requestConfig.addNoTokenizeIndex("index2");
    requestConfig.setTraceType("kvtracer");
    requestConfig.setResearchThreshold(1000);
    requestConfig.setNeedRerank(true);
    requestConfig.setUseTruncateOptimizer(false);
    requestConfig.setSubDocDisplayType(SUB_DOC_DISPLAY_GROUP);
    ASSERT_TRUE(!requestConfig.hasPBMatchDocFormat());
    requestConfig.addProtoFormatOption(PBResultFormatter::PB_MATCHDOC_FORMAT);
    ASSERT_TRUE(requestConfig.hasPBMatchDocFormat());
    requestConfig.setProtoFormatOption(PBResultFormatter::SUMMARY_IN_BYTES);
    ASSERT_TRUE(!requestConfig.hasPBMatchDocFormat());
    ASSERT_TRUE(PBResultFormatter::SUMMARY_IN_BYTES == requestConfig.getProtoFormatOption());
    requestConfig.setProtoFormatOption(PBResultFormatter::PB_MATCHDOC_FORMAT);
    ASSERT_TRUE(requestConfig.hasPBMatchDocFormat());
    ASSERT_TRUE(PBResultFormatter::PB_MATCHDOC_FORMAT == requestConfig.getProtoFormatOption());
    requestConfig.addIndexAnalyzerName("index3", "aliws");

    ConfigClause requestConfig2;
    serializeRequest(requestConfig, requestConfig2);

    ASSERT_EQ(originalString, requestConfig2.getOriginalString());
    ASSERT_EQ(string("phrase.default"), requestConfig2.getClusterName());
    ASSERT_EQ((uint32_t)10, requestConfig2.getStartOffset());
    ASSERT_EQ((uint32_t)100, requestConfig2.getHitCount());
    ASSERT_EQ((uint32_t)200, requestConfig2.getRankSize());
    ASSERT_EQ((uint32_t)300, requestConfig2.getRerankSize());
    ASSERT_EQ(string("test_index"), requestConfig2.getDefaultIndexName());
    ASSERT_EQ(OP_AND, requestConfig2.getDefaultOP());
    ASSERT_EQ(string("xml"), requestConfig2.getResultFormatSetting());// used for qrs

    ASSERT_EQ(SEARCH_PHASE_TWO, requestConfig2.getPhaseNumber());
    ASSERT_EQ(string("WARN"), requestConfig2.getTrace());
    ASSERT_EQ(string("DEBUG"), requestConfig2.getRankTrace());
    ASSERT_EQ(string("value1"), requestConfig2.getKVPairValue("key1"));
    ASSERT_EQ(string("value2"), requestConfig2.getKVPairValue("key2"));
    ASSERT_EQ(string("DEFAULT"), requestConfig2.getQrsChainName()); // used for qrs

    ASSERT_EQ(string("summaryProfile"), requestConfig2.getSummaryProfileName());
    ASSERT_EQ(string("simple"), requestConfig2.getAnalyzerName());
    ASSERT_TRUE(!requestConfig2.getDisplayCacheInfo());

    ASSERT_EQ(uint32_t(123456), requestConfig2.getRpcTimeOut());
    ASSERT_EQ((uint32_t)5, requestConfig2.getSearcherReturnHits());
    ASSERT_TRUE(requestConfig2.isNoSummary());
    ASSERT_TRUE(requestConfig2.isDoDedup());
    ASSERT_TRUE(!requestConfig2.isBatchScore());
    ASSERT_TRUE(!requestConfig2.isOptimizeRank());
    ASSERT_TRUE(!requestConfig2.isOptimizeComparator());
    ASSERT_EQ(string("123456"), requestConfig2.getDebugQueryKey());
    ASSERT_EQ(string("source1"), requestConfig2.getSourceId());
    ASSERT_EQ((uint32_t)1000, requestConfig2.getResearchThreshold());
    ASSERT_TRUE(requestConfig2.needRerank());
    ASSERT_TRUE(!requestConfig2.useTruncateOptimizer());
    ASSERT_EQ(SUB_DOC_DISPLAY_GROUP, requestConfig2.getSubDocDisplayType());

    ASSERT_EQ((uint32_t)0, requestConfig2.getActualHitsLimit()); // used for qrs
    ASSERT_TRUE(requestConfig.getNoTokenizeIndexes() == 
                   requestConfig2.getNoTokenizeIndexes());
    ASSERT_EQ(size_t(1),
                         requestConfig.getIndexAnalyzerMap().size());
    ASSERT_EQ(size_t(1),
                         requestConfig2.getIndexAnalyzerMap().size());
    ASSERT_EQ(string("aliws"),
                         requestConfig.getIndexAnalyzerName("index3"));
    ASSERT_EQ(string("aliws"),
                         requestConfig2.getIndexAnalyzerName("index3"));
    ASSERT_EQ(string("kvtracer"), requestConfig.getTraceType());
}

void ConfigClauseTest::serializeRequest(const ConfigClause& request,
                                        ConfigClause& request2)
{
    autil::DataBuffer buffer;
    buffer.write(request);
    buffer.read(request2);
}

TEST_F(ConfigClauseTest, testAddAndRemoveKVPairs) {
    ConfigClause requestConfig;
    requestConfig.removeKVPair("not_exist_key");
    requestConfig.addKVPair("key1", "value1");
    requestConfig.addKVPair("key2", "value2");
    requestConfig.addKVPair("key3", "value3");

    const map<string, string> resultKVPairs1 = requestConfig.getKVPairs();
    ASSERT_EQ((size_t)3, resultKVPairs1.size());

    ASSERT_EQ(string("value2"), requestConfig.getKVPairValue("key2"));
    requestConfig.removeKVPair("key2");
    ASSERT_EQ(string(""), requestConfig.getKVPairValue("key2"));
    const map<string, string> resultKVPairs2 = requestConfig.getKVPairs();
    ASSERT_EQ((size_t)2, resultKVPairs2.size());
    
    ASSERT_EQ(string("value1"), requestConfig.getKVPairValue("key1"));
    requestConfig.removeKVPair("key1");
    ASSERT_EQ(string(""), requestConfig.getKVPairValue("key1"));
    const map<string, string> resultKVPairs3 = requestConfig.getKVPairs();
    ASSERT_EQ((size_t)1, resultKVPairs3.size());

    ASSERT_EQ(string("value3"), requestConfig.getKVPairValue("key3"));
    requestConfig.removeKVPair("key3");
    ASSERT_EQ(string(""), requestConfig.getKVPairValue("key3"));
    const map<string, string> resultKVPairs4 = requestConfig.getKVPairs();
    ASSERT_EQ((size_t)0, resultKVPairs4.size());

    requestConfig.removeKVPair("key3");
    ASSERT_EQ(string(""), requestConfig.getKVPairValue("key3"));
    const map<string, string> resultKVPairs5 = requestConfig.getKVPairs();
    ASSERT_EQ((size_t)0, resultKVPairs5.size());
}

TEST_F(ConfigClauseTest, testAllowLackOfSummarySwitch) {
    ConfigClause configClause;
    ASSERT_TRUE(configClause.allowLackOfSummary());
    configClause.setAllowLackSummaryConfig("yes");
    ASSERT_TRUE(configClause.allowLackOfSummary());
    configClause.setAllowLackSummaryConfig("y");
    ASSERT_TRUE(configClause.allowLackOfSummary());
    configClause.setAllowLackSummaryConfig("true");
    ASSERT_TRUE(configClause.allowLackOfSummary());
    
    configClause.setAllowLackSummaryConfig("no");
    ASSERT_TRUE(!(configClause.allowLackOfSummary()));
    configClause.setAllowLackSummaryConfig("n");
    ASSERT_TRUE(!(configClause.allowLackOfSummary()));
    configClause.setAllowLackSummaryConfig("false");
    ASSERT_TRUE(!(configClause.allowLackOfSummary()));
    
    configClause.setAllowLackSummaryConfig("invalid_value");
    ASSERT_TRUE(!(configClause.allowLackOfSummary()));
}

TEST_F(ConfigClauseTest, testConvertCompressType) {
    ASSERT_EQ(NO_COMPRESS, 
                         CompressTypeConvertor::convertCompressType("no_compress"));
    ASSERT_EQ(Z_SPEED_COMPRESS, 
                         CompressTypeConvertor::convertCompressType("z_speed_compress"));
    ASSERT_EQ(Z_DEFAULT_COMPRESS, 
                         CompressTypeConvertor::convertCompressType("z_default_compress"));
    ASSERT_EQ(SNAPPY, 
                         CompressTypeConvertor::convertCompressType("snappy"));
    ASSERT_EQ(LZ4, 
                         CompressTypeConvertor::convertCompressType("lz4"));
    ASSERT_EQ(INVALID_COMPRESS_TYPE, 
                         CompressTypeConvertor::convertCompressType("aaa"));
    ASSERT_EQ(INVALID_COMPRESS_TYPE, 
                         CompressTypeConvertor::convertCompressType("compress"));
}

TEST_F(ConfigClauseTest, testGetSearcherRequireCount) {
    ConfigClause configClause;
    ASSERT_EQ((uint32_t)10, configClause.getSearcherRequireCount());

    configClause.setSearcherReturnHits(5);
    ASSERT_EQ((uint32_t)5, configClause.getSearcherRequireCount());

    configClause.setSearcherReturnHits(15);
    ASSERT_EQ((uint32_t)10, configClause.getSearcherRequireCount());
}

TEST_F(ConfigClauseTest, testGetIndexAnalyzerName) {
    ConfigClause configClause;
    ASSERT_EQ(string(""), configClause.getIndexAnalyzerName("index1"));
    configClause.setAnalyzerName("aliws");
    ASSERT_EQ(string("aliws"), configClause.getIndexAnalyzerName("index1"));
    ASSERT_EQ(string("aliws"), configClause.getIndexAnalyzerName("index2"));
    configClause.addNoTokenizeIndex("index2");
    ASSERT_TRUE(configClause.isIndexNoTokenize("index2"));
    ASSERT_EQ((size_t)1, configClause.getNoTokenizeIndexes().size());
    ASSERT_EQ(string("aliws"), configClause.getIndexAnalyzerName("index2"));
    
    configClause.addIndexAnalyzerName("index1", "singlews");
    ASSERT_EQ(string("singlews"), 
                         configClause.getIndexAnalyzerName("index1"));
    ASSERT_EQ(string("aliws"), configClause.getIndexAnalyzerName("index2"));
}

TEST_F(ConfigClauseTest, testSetIgnoreDelete) {
    ConfigClause configClause;
    ASSERT_EQ(ConfigClause::CB_DEFAULT, configClause.ignoreDelete());
    configClause.setIgnoreDelete(true);
    ASSERT_EQ(ConfigClause::CB_TRUE, configClause.ignoreDelete());

    configClause.setIgnoreDelete("true");
    ASSERT_EQ(ConfigClause::CB_TRUE, configClause.ignoreDelete());
    configClause.setIgnoreDelete("yes");
    ASSERT_EQ(ConfigClause::CB_TRUE, configClause.ignoreDelete());
    configClause.setIgnoreDelete("y");
    ASSERT_EQ(ConfigClause::CB_TRUE, configClause.ignoreDelete());

    configClause.setIgnoreDelete(false);
    ASSERT_EQ(ConfigClause::CB_FALSE, configClause.ignoreDelete());
    autil::DataBuffer dataBuffer;
    dataBuffer.write(configClause);
    ConfigClause newConfigClause;
    dataBuffer.read(newConfigClause);
    ASSERT_EQ(ConfigClause::CB_FALSE, newConfigClause.ignoreDelete());
}

TEST_F(ConfigClauseTest, testVersion) {
    ConfigClause configClause;
    ASSERT_EQ(0, configClause.getTotalRankSize());
    ASSERT_EQ(0, configClause.getTotalRerankSize());
    ASSERT_EQ(-1, configClause.getVersion());
    configClause.setVersion(12345);
    ASSERT_EQ(12345, configClause.getVersion());
}

END_HA3_NAMESPACE(common);

