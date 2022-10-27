#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/queryparser/RequestParser.h>
#include <ha3/common/Request.h>
#include <ha3/config/ClusterConfigInfo.h>
#include <string>
#include <ha3/common/ErrorResult.h>
#include <ha3/common/ErrorDefine.h>
#include <autil/HashFuncFactory.h>
#include <suez/turing/expression/syntax/AtomicSyntaxExpr.h>
#include <suez/turing/expression/syntax/BinarySyntaxExpr.h>
#include <ha3/queryparser/test/AnalyzerFactoryInit.h>
#include <suez/turing/expression/syntax/RankSyntaxExpr.h>
#include <ha3/test/test.h>
#include <string>
#include <ha3/queryparser/RequestSymbolDefine.h>
#include <ha3/common/VirtualAttributeClause.h>
#include <ha3/common/PBResultFormatter.h>
#include <ha3/common/SearcherCacheClause.h>

using namespace std;
using namespace autil;
using namespace suez::turing;
USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(util);
USE_HA3_NAMESPACE(common);
using namespace build_service::analyzer;
USE_HA3_NAMESPACE(search);

BEGIN_HA3_NAMESPACE(queryparser);

class RequestParserTest : public TESTBASE {

public:
    RequestParserTest();
    ~RequestParserTest();
public:
    void setUp();
    void tearDown();
protected:
    void checkErrorResult(std::string queryStr, ErrorCode errorCode);
    void checkParseGidResult(const std::string& gidStr, bool expectParseRet,
                             const std::string& expectClusterName = "",
                             uint32_t expectFullInexVerison = 0,
                             int32_t expectIndexVersion = 0,
                             hashid_t expectHashId = 0,
                             docid_t expectDocId = 0,
                             primarykey_t expectPk = (primarykey_t)0,
                             uint32_t expectIp = 0);

protected:
    config::ClusterConfigInfo *_clusterConfigInfo;
    config::ClusterConfigMapPtr _clusterConfigMapPtr;
    RequestParser *_requestParser;
    common::Request *_request;
    autil::HashFunctionBasePtr _hashFuncPtr;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(queryparser, RequestParserTest);


RequestParserTest::RequestParserTest() {
    _requestParser = NULL;
    _request = NULL;
}

RequestParserTest::~RequestParserTest() {
}

void RequestParserTest::setUp() {
    HA3_LOG(DEBUG, "setUp!");
    _requestParser = new RequestParser();

    _clusterConfigInfo = new ClusterConfigInfo();
    _clusterConfigInfo->_tableName = "auction";
    _clusterConfigInfo->setQueryInfo(QueryInfo("phrase", OP_AND));

    HashMode hashMode;
    hashMode._hashFields.push_back("user");
    hashMode._hashFunction = "HASH";
    _clusterConfigInfo->_hashMode = hashMode;
    ASSERT_TRUE(_clusterConfigInfo->initHashFunc());
    _clusterConfigMapPtr.reset(new ClusterConfigMap);
    _clusterConfigMapPtr->insert(make_pair("cluster1.default", *_clusterConfigInfo));

    ClusterConfigInfo clusterConfigInfo2;
    clusterConfigInfo2._tableName = "auction";
    clusterConfigInfo2._hashMode = hashMode;
    ASSERT_TRUE(clusterConfigInfo2.initHashFunc());
    _clusterConfigMapPtr->insert(make_pair("cluster2.default", clusterConfigInfo2));

    ClusterConfigInfo clusterConfigInfo3;
    clusterConfigInfo3._tableName = "auction";
    clusterConfigInfo3._hashMode = hashMode;
    ASSERT_TRUE(clusterConfigInfo3.initHashFunc());
    _clusterConfigMapPtr->insert(make_pair("cluster3.default", clusterConfigInfo3));

    _hashFuncPtr = HashFuncFactory::createHashFunc(hashMode._hashFunction,
            MAX_PARTITION_COUNT);
    ASSERT_TRUE(_hashFuncPtr);
}

void RequestParserTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");
    delete _requestParser;
    if (_request) {
        delete _request;
        _request = NULL;
    }
    DELETE_AND_SET_NULL(_clusterConfigInfo);
}

TEST_F(RequestParserTest, testParseConfigClause) {
    HA3_LOG(DEBUG, "Begin Test");
    RequestPtr requestPtr(new Request);

    string oriStr = "cluster:cluster1,start:50,hit:10,format:xml,rank_size:123,total_rank_size:1230,request_trace_type:kvtracer,"
                    "rerank_size:321,total_rerank_size:3210,timeout:3000,batch_score:false,phase_one_task_queue:search_queue,"
                    "searcher_return_hits:5,no_summary:yes,no_tokenize_indexes:index1;index2,default_index:test_index,default_operator:OR,"
                    "proto_format_option:pb_matchdoc_format,seek_timeout:20,phase_two_task_queue:summary_queue,"
                    "dedup:no,inner_result_compress:z_speed_compress,actual_hits_limit:10, sub_doc:flat,ignore_delete:false";
    requestPtr->setOriginalString("config=" + oriStr);
    ConfigClause *configClause = requestPtr->getConfigClause();
    ASSERT_TRUE(configClause);
    ASSERT_TRUE(_requestParser->parseConfigClause(requestPtr));
    ASSERT_EQ(oriStr, configClause->getOriginalString());
    ASSERT_EQ(string("cluster1.default"), configClause->getClusterName());
    ASSERT_EQ(string("xml"), configClause->getResultFormatSetting());
    ASSERT_EQ(uint32_t(50), configClause->getStartOffset());
    ASSERT_EQ(uint32_t(10), configClause->getHitCount());
    ASSERT_EQ(uint32_t(20), configClause->getSeekTimeOut());
    ASSERT_EQ(OP_OR, configClause->getDefaultOP());
    ASSERT_EQ(string("test_index"), configClause->getDefaultIndexName());
    ASSERT_EQ(uint32_t(123), configClause->getRankSize());
    ASSERT_EQ(uint32_t(1230), configClause->getTotalRankSize());
    ASSERT_EQ(uint32_t(321), configClause->getRerankSize());
    ASSERT_EQ(uint32_t(3210), configClause->getTotalRerankSize());
    ASSERT_EQ(size_t(0), configClause->getKVPairs().size());
    ASSERT_EQ(string("DEFAULT"), configClause->getQrsChainName());
    ASSERT_EQ(string("DefaultProfile"), configClause->getSummaryProfileName());
    ASSERT_EQ(string("search_queue"), configClause->getPhaseOneTaskQueueName());
    ASSERT_EQ(string("summary_queue"), configClause->getPhaseTwoTaskQueueName());
    ASSERT_EQ(uint32_t(3000), configClause->getRpcTimeOut());
    ASSERT_EQ((uint32_t)5, configClause->getSearcherReturnHits());
    ASSERT_TRUE(configClause->isNoSummary());
    ASSERT_TRUE(configClause->hasPBMatchDocFormat());
    ASSERT_TRUE(!configClause->isDoDedup());
    ASSERT_TRUE(!configClause->isBatchScore());
    ASSERT_EQ(ConfigClause::CB_FALSE, configClause->ignoreDelete());
    ASSERT_EQ(Z_SPEED_COMPRESS,
                         (HaCompressType)configClause->getInnerResultCompressType());
    ASSERT_EQ(DEFAULT_JOIN, (JoinType)configClause->getJoinType());
    ASSERT_EQ(uint32_t(10), configClause->getActualHitsLimit());
    const set<string>& noTokenizeIndexes = configClause->getNoTokenizeIndexes();
    ASSERT_EQ(size_t(2), noTokenizeIndexes.size());
    ASSERT_TRUE(noTokenizeIndexes.find("index1") != noTokenizeIndexes.end());
    ASSERT_TRUE(noTokenizeIndexes.find("index2") != noTokenizeIndexes.end());
    ASSERT_EQ(string("kvtracer"), configClause->getTraceType());
    ASSERT_EQ(SUB_DOC_DISPLAY_FLAT, configClause->getSubDocDisplayType());
}

TEST_F(RequestParserTest, testParseConfigClauseMultiClusterName) {
    HA3_LOG(DEBUG, "Begin Test");
    RequestPtr requestPtr(new Request);
    string configStr = "cluster:cluster1&cluster2&cluster3,start:50,hit:10,"
                       "format:xml,no_tokenize_indexes:index1";
    string oriStr = "config=" + configStr;
    requestPtr->setOriginalString(oriStr);
    ConfigClause *configClause = requestPtr->getConfigClause();
    ASSERT_TRUE(configClause);

    ASSERT_TRUE(_requestParser->parseConfigClause(requestPtr));

    ASSERT_EQ(configStr, configClause->getOriginalString());
    ASSERT_EQ((size_t)3, configClause->getClusterNameCount());
    ASSERT_EQ(string("cluster1.default"), configClause->getClusterName());
    ASSERT_EQ(string("cluster2.default"), configClause->getClusterName(1));
    ASSERT_EQ(string("cluster3.default"), configClause->getClusterName(2));
    ASSERT_EQ(string("xml"), configClause->getResultFormatSetting());
    ASSERT_EQ(uint32_t(50), configClause->getStartOffset());
    ASSERT_EQ(uint32_t(10), configClause->getHitCount());
    ASSERT_EQ(size_t(0), configClause->getKVPairs().size());
    ASSERT_EQ(string("DEFAULT"), configClause->getQrsChainName());
    ASSERT_EQ(string("DefaultProfile"), configClause->getSummaryProfileName());
    ASSERT_EQ(uint32_t(0), configClause->getRpcTimeOut());
    const set<string>& noTokenizeIndexes = configClause->getNoTokenizeIndexes();
    ASSERT_EQ(size_t(1), noTokenizeIndexes.size());
    ASSERT_TRUE(noTokenizeIndexes.find("index1") != noTokenizeIndexes.end());
}

TEST_F(RequestParserTest, testParseConfigClauseErrorKVPair) {
    RequestPtr requestPtr(new Request);
    string oriStr = "config=cluster:cluster1,start:,hit:10";
    requestPtr->setOriginalString(oriStr);
    ASSERT_TRUE(requestPtr->getConfigClause());
    ASSERT_TRUE(!_requestParser->parseConfigClause(requestPtr));
}

TEST_F(RequestParserTest, testParseConfigClauseErrorSubDocDisplayType) {
    RequestPtr requestPtr(new Request);
    string oriStr = "config=cluster:cluster1,sub_doc:yes";
    requestPtr->setOriginalString(oriStr);
    ASSERT_TRUE(requestPtr->getConfigClause());
    ASSERT_TRUE(!_requestParser->parseConfigClause(requestPtr));
    ErrorResult err = _requestParser->getErrorResult();
    ASSERT_TRUE(err.hasError());
    ASSERT_EQ(ERROR_CONFIG_CLAUSE_SUB_DOC_DISPLAY_TYPE, err.getErrorCode());
}

TEST_F(RequestParserTest, testParseConfigClauseWithYesSummary) {
    RequestPtr requestPtr(new Request);
    string oriStr = "config=cluster:cluster1,start:50,hit:10";
    requestPtr->setOriginalString(oriStr);
    ConfigClause *configClause = requestPtr->getConfigClause();
    ASSERT_TRUE(configClause);
    ASSERT_TRUE(_requestParser->parseConfigClause(requestPtr));
    ASSERT_TRUE(!configClause->isNoSummary());

    oriStr = "config=cluster:cluster1,start:50,hit:10,no_summary:aa";
    requestPtr->setOriginalString(oriStr);
    configClause = requestPtr->getConfigClause();
    ASSERT_TRUE(configClause);
    ASSERT_TRUE(_requestParser->parseConfigClause(requestPtr));
    ASSERT_TRUE(!configClause->isNoSummary());
}

TEST_F(RequestParserTest, testParseConfigClauseWithSourceId) {
    RequestPtr requestPtr(new Request);
    string oriStr = "config=cluster:cluster1";
    requestPtr->setOriginalString(oriStr);
    ConfigClause *configClause = requestPtr->getConfigClause();
    ASSERT_TRUE(configClause);
    ASSERT_TRUE(_requestParser->parseConfigClause(requestPtr));
    ASSERT_EQ(string(""), configClause->getSourceId());

    oriStr = "config=cluster:cluster1,sourceid:source1";
    requestPtr->setOriginalString(oriStr);
    configClause = requestPtr->getConfigClause();
    ASSERT_TRUE(configClause);
    ASSERT_TRUE(_requestParser->parseConfigClause(requestPtr));
    ASSERT_EQ(string("source1"), configClause->getSourceId());
}

TEST_F(RequestParserTest, testParseConfigClauseWithVersion) {
    {
        RequestPtr requestPtr(new Request);
        string oriStr = "config=cluster:cluster1";
        requestPtr->setOriginalString(oriStr);
        ConfigClause *configClause = requestPtr->getConfigClause();
        ASSERT_TRUE(configClause);
        ASSERT_TRUE(_requestParser->parseConfigClause(requestPtr));
        ASSERT_EQ(-1, configClause->getVersion());
    }
    {
        RequestPtr requestPtr(new Request);
        string oriStr = "config=cluster:cluster1,version:1111";
        requestPtr->setOriginalString(oriStr);
        ConfigClause *configClause = requestPtr->getConfigClause();
        ASSERT_TRUE(configClause);
        ASSERT_TRUE(_requestParser->parseConfigClause(requestPtr));
        ASSERT_EQ(1111, configClause->getVersion());
    }
    {
        RequestPtr requestPtr(new Request);
        string oriStr = "config=cluster:cluster1,version:abc";
        requestPtr->setOriginalString(oriStr);
        ConfigClause *configClause = requestPtr->getConfigClause();
        ASSERT_TRUE(configClause);
        ASSERT_FALSE(_requestParser->parseConfigClause(requestPtr));
        ASSERT_EQ(-1, configClause->getVersion());
    }
}

TEST_F(RequestParserTest, testParseConfigClauseMissClustername) {
    HA3_LOG(DEBUG, "Begin Test");
    RequestPtr requestPtr(new Request);
    string oriStr = "config=start:50,hit:10,format:xml";
    requestPtr->setOriginalString(oriStr);
    ASSERT_TRUE(_requestParser->parseConfigClause(requestPtr));
}

TEST_F(RequestParserTest, testParseConfigClauseUseDefaultValue) {
    HA3_LOG(DEBUG, "Begin Test");
    RequestPtr requestPtr(new Request);
    string oriStr = "config=cluster:cluster1";
    requestPtr->setOriginalString(oriStr);

    ConfigClause *configClause = requestPtr->getConfigClause();
    ASSERT_TRUE(_requestParser->parseConfigClause(requestPtr));
    ASSERT_TRUE(configClause);

    string expectedString = "cluster:cluster1";
    ASSERT_EQ(expectedString, configClause->getOriginalString());
    ASSERT_EQ(string("cluster1.default"), configClause->getClusterName());
    ASSERT_EQ(string("xml"), configClause->getResultFormatSetting());
    ASSERT_EQ(string(""), configClause->getPhaseOneTaskQueueName());
    ASSERT_EQ(string(""), configClause->getPhaseTwoTaskQueueName());
    ASSERT_EQ(uint32_t(0), configClause->getStartOffset());
    ASSERT_EQ(uint32_t(10), configClause->getHitCount());
    ASSERT_EQ(size_t(0), configClause->getKVPairs().size());
    ASSERT_EQ((uint32_t)0, configClause->getSearcherReturnHits());
}

TEST_F(RequestParserTest, testParseConfigClauseMissValue) {
    HA3_LOG(DEBUG, "Begin Test");
    RequestPtr requestPtr(new Request);
    string oriStr = "config=cluster:,start:50,hit:10,format:xml";
    requestPtr->setOriginalString(oriStr);

    ConfigClause *configClause = requestPtr->getConfigClause();
    ASSERT_TRUE(configClause);
    ASSERT_TRUE(!_requestParser->parseConfigClause(requestPtr));
}

TEST_F(RequestParserTest, testParseConfigClauseWithKVPairs) {
    HA3_LOG(DEBUG, "Begin Test");
    RequestPtr requestPtr(new Request);
    string oriStr = "config=cluster:cluster1,start:50,hit:10,kvpairs:key1#value1;key2#value2";
    requestPtr->setOriginalString(oriStr);
    ConfigClause *configClause = requestPtr->getConfigClause();
    ASSERT_TRUE(configClause);

    ASSERT_TRUE(_requestParser->parseConfigClause(requestPtr));

    string expectedString = "cluster:cluster1,start:50,hit:10,kvpairs:key1#value1;key2#value2";
    ASSERT_EQ(expectedString, configClause->getOriginalString());
    ASSERT_EQ(string("cluster1.default"), configClause->getClusterName());
    ASSERT_EQ(string("xml"), configClause->getResultFormatSetting());
    ASSERT_EQ(uint32_t(50), configClause->getStartOffset());
    ASSERT_EQ(uint32_t(10), configClause->getHitCount());
    ASSERT_EQ(size_t(2), configClause->getKVPairs().size());
    ASSERT_EQ(string("value1"), configClause->getKVPairValue(string("key1")));
    ASSERT_EQ(string("value2"), configClause->getKVPairValue(string("key2")));
}

TEST_F(RequestParserTest, testParseConfigClauseWithNegativeStartAndHitCount) {
    HA3_LOG(DEBUG, "Begin Test");
    RequestPtr requestPtr(new Request);
    string oriStr = "config=cluster:cluster1,start:-1";
    requestPtr->setOriginalString(oriStr);
    ConfigClause *configClause = requestPtr->getConfigClause();
    ASSERT_TRUE(configClause);
    ASSERT_TRUE(!_requestParser->parseConfigClause(requestPtr));
    ASSERT_EQ(ERROR_CONFIG_CLAUSE_START_VALUE, _requestParser->getErrorResult().getErrorCode());

    oriStr = "config=cluster:cluster1,hit:-1";
    requestPtr->setOriginalString(oriStr);
    ASSERT_TRUE(!_requestParser->parseConfigClause(requestPtr));
    ASSERT_EQ(ERROR_CONFIG_CLAUSE_HIT_VALUE, _requestParser->getErrorResult().getErrorCode());

    oriStr = "config=cluster:cluster1,searcher_return_hits:-6";
    requestPtr->setOriginalString(oriStr);
    ASSERT_TRUE(!_requestParser->parseConfigClause(requestPtr));
    ASSERT_EQ(ERROR_CONFIG_CLAUSE_HIT_VALUE, _requestParser->getErrorResult().getErrorCode());
}

TEST_F(RequestParserTest, testParseConfigClauseWithInvalidSearcherReturnHits) {
    HA3_LOG(DEBUG, "Begin Test");
    RequestPtr requestPtr(new Request);
    string oriStr = "config=cluster:cluster1,searcher_return_hits:ccc";
    requestPtr->setOriginalString(oriStr);

    ConfigClause *configClause = requestPtr->getConfigClause();
    ASSERT_TRUE(configClause);

    ASSERT_TRUE(!_requestParser->parseConfigClause(requestPtr));
}

TEST_F(RequestParserTest, testParseConfigClauseWithInvalidNumber) {
    HA3_LOG(DEBUG, "Begin Test");
    RequestPtr requestPtr(new Request);
    string oriStr = "config=cluster:cluster1,rank_size:-1";
    requestPtr->setOriginalString(oriStr);
    ASSERT_TRUE(!_requestParser->parseConfigClause(requestPtr));
    ASSERT_EQ(ERROR_CONFIG_CLAUSE_RANKSIZE,
                         _requestParser->getErrorResult().getErrorCode());

    oriStr = "config=cluster:cluster1,rerank_size:xxds";
    requestPtr->setOriginalString(oriStr);
    ASSERT_TRUE(!_requestParser->parseConfigClause(requestPtr));
    ASSERT_EQ(ERROR_CONFIG_CLAUSE_RERANKSIZE,
                         _requestParser->getErrorResult().getErrorCode());

    oriStr = "config=cluster:cluster1,rank_size:xxds";
    requestPtr->setOriginalString(oriStr);
    ASSERT_TRUE(!_requestParser->parseConfigClause(requestPtr));
    ASSERT_EQ(ERROR_CONFIG_CLAUSE_RANKSIZE,
                         _requestParser->getErrorResult().getErrorCode());

    oriStr = "config=cluster:cluster1,rerank_size:-1";
    requestPtr->setOriginalString(oriStr);
    ASSERT_TRUE(!_requestParser->parseConfigClause(requestPtr));
    ASSERT_EQ(ERROR_CONFIG_CLAUSE_RERANKSIZE,
                         _requestParser->getErrorResult().getErrorCode());

    oriStr = "config=cluster:cluster1,timeout:cx";
    requestPtr->setOriginalString(oriStr);
    ASSERT_TRUE(!_requestParser->parseConfigClause(requestPtr));
    ASSERT_EQ(ERROR_CONFIG_CLAUSE_TIMEOUT,
                         _requestParser->getErrorResult().getErrorCode());

    oriStr = "config=cluster:cluster1,actual_hits_limit:-1";
    requestPtr->setOriginalString(oriStr);
    ASSERT_TRUE(!_requestParser->parseConfigClause(requestPtr));
    ASSERT_EQ(ERROR_CONFIG_CLAUSE_HIT_VALUE,
                         _requestParser->getErrorResult().getErrorCode());

    oriStr = "config=cluster:cluster1,actual_hits_limit:xxds";
    requestPtr->setOriginalString(oriStr);
    ASSERT_TRUE(!_requestParser->parseConfigClause(requestPtr));
    ASSERT_EQ(ERROR_CONFIG_CLAUSE_HIT_VALUE,
                         _requestParser->getErrorResult().getErrorCode());
}

TEST_F(RequestParserTest, testParseConfigClauseWithAnalyzer) {
    HA3_LOG(DEBUG, "Begin Test");
    RequestPtr requestPtr(new Request);

    {//with analyzer
        string oriStr = "config=cluster:cluster1,analyzer:simple";
        requestPtr->setOriginalString(oriStr);

        ConfigClause *configClause = requestPtr->getConfigClause();
        ASSERT_TRUE(configClause);
        ASSERT_TRUE(_requestParser->parseConfigClause(requestPtr));

        string expectedString = "cluster:cluster1,analyzer:simple";
        ASSERT_EQ(expectedString, configClause->getOriginalString());
        ASSERT_EQ(string("cluster1.default"), configClause->getClusterName());
        ASSERT_EQ(string("simple"), configClause->getAnalyzerName());
    }

    {//test with wrong analyzer config
        string oriStr = "config=cluster:cluster1,analyzer:";
        requestPtr->setOriginalString(oriStr);

        ConfigClause *configClause = requestPtr->getConfigClause();
        ASSERT_TRUE(configClause);
        ASSERT_TRUE(!_requestParser->parseConfigClause(requestPtr));
    }

    {//test without analyzer
        string oriStr = "config=cluster:cluster1";
        requestPtr->setOriginalString(oriStr);
        ConfigClause *configClause = requestPtr->getConfigClause();
        ASSERT_TRUE(configClause);

        ASSERT_TRUE(_requestParser->parseConfigClause(requestPtr));

        string expectedString = "cluster:cluster1";
        ASSERT_EQ(expectedString, configClause->getOriginalString());
        ASSERT_EQ(string("cluster1.default"), configClause->getClusterName());
        ASSERT_EQ(string(""), configClause->getAnalyzerName());
    }
}


TEST_F(RequestParserTest, testParseConfigClauseWithDefaultQrsChainConfigAndSummaryExtractor) {
    HA3_LOG(DEBUG, "Begin Test");
    RequestPtr requestPtr(new Request);
    string oriStr = "config=cluster:cluster1,start:50,hit:10,format:xml, qrs_chain:chain1,summary_profile:summary1";
    requestPtr->setOriginalString(oriStr);

    ConfigClause *configClause = requestPtr->getConfigClause();
    ASSERT_TRUE(configClause);

    ASSERT_TRUE(_requestParser->parseConfigClause(requestPtr));

    string expectedString = "cluster:cluster1,start:50,hit:10,format:xml, qrs_chain:chain1,summary_profile:summary1";
    ASSERT_EQ(expectedString, configClause->getOriginalString());
    ASSERT_EQ(string("cluster1.default"), configClause->getClusterName());
    ASSERT_EQ(string("xml"), configClause->getResultFormatSetting());
    ASSERT_EQ(uint32_t(50), configClause->getStartOffset());
    ASSERT_EQ(uint32_t(10), configClause->getHitCount());
    ASSERT_EQ(size_t(0), configClause->getKVPairs().size());
    ASSERT_EQ(string("chain1"), configClause->getQrsChainName());
    ASSERT_EQ(string("summary1"), configClause->getSummaryProfileName());
}

TEST_F(RequestParserTest, testParseConfigClauseWithFetchSummaryCluster) {
    // with fetch summary cluster name
    {
        RequestPtr requestPtr(new Request);
        string oriStr = "config=cluster:cluster1&cluster2,fetch_summary_cluster:cluster3";
        string expectedString = "cluster:cluster1&cluster2,fetch_summary_cluster:cluster3";;
        requestPtr->setOriginalString(oriStr);

        ConfigClause *configClause = requestPtr->getConfigClause();
        ASSERT_TRUE(configClause);
        ASSERT_TRUE(_requestParser->parseConfigClause(requestPtr));

        ASSERT_EQ(expectedString, configClause->getOriginalString());
        ASSERT_EQ(string("cluster1.default"), configClause->getClusterName(0));
        ASSERT_EQ(string("cluster2.default"), configClause->getClusterName(1));
        ASSERT_EQ(string("cluster3.default"),
                             configClause->getFetchSummaryClusterName("cluster1"));
        ASSERT_EQ(string("cluster3.default"),
                             configClause->getFetchSummaryClusterName("cluster2"));

    }
    // empty fetch summary cluster name (default)
    {
        RequestPtr requestPtr(new Request);
        string oriStr = "config=cluster:cluster1&cluster2,fetch_summary_cluster:";
        string expectedString = "cluster:cluster1&cluster2,fetch_summary_cluster:";
        requestPtr->setOriginalString(oriStr);

        ConfigClause *configClause = requestPtr->getConfigClause();
        ASSERT_TRUE(configClause);
        ASSERT_TRUE(!_requestParser->parseConfigClause(requestPtr));
        ASSERT_EQ(ERROR_KEY_VALUE_PAIR,
                             _requestParser->getErrorResult().getErrorCode());
    }
}

TEST_F(RequestParserTest, testParseConfigJoinType) {
    {
        RequestPtr requestPtr(new Request);
        string oriStr = "config=cluster:cluster1";
        requestPtr->setOriginalString(oriStr);
        ConfigClause *configClause = requestPtr->getConfigClause();
        ASSERT_TRUE(configClause);
        ASSERT_TRUE(_requestParser->parseConfigClause(requestPtr));
        ASSERT_EQ(JoinType(DEFAULT_JOIN), configClause->getJoinType());
    }
    {
        RequestPtr requestPtr(new Request);
        string oriStr = "config=cluster:cluster1,join_type:weak";
        requestPtr->setOriginalString(oriStr);
        ConfigClause *configClause = requestPtr->getConfigClause();
        ASSERT_TRUE(configClause);
        ASSERT_TRUE(_requestParser->parseConfigClause(requestPtr));
        ASSERT_EQ(JoinType(WEAK_JOIN), configClause->getJoinType());
    }
    {
        RequestPtr requestPtr(new Request);
        string oriStr = "config=cluster:cluster1,join_type:strong";
        requestPtr->setOriginalString(oriStr);
        ConfigClause *configClause = requestPtr->getConfigClause();
        ASSERT_TRUE(configClause);
        ASSERT_TRUE(_requestParser->parseConfigClause(requestPtr));
        ASSERT_EQ(JoinType(STRONG_JOIN), configClause->getJoinType());
    }
    {
        RequestPtr requestPtr(new Request);
        string oriStr = "config=cluster:cluster1,join_type:error_type";
        requestPtr->setOriginalString(oriStr);
        ConfigClause *configClause = requestPtr->getConfigClause();
        ASSERT_TRUE(configClause);
        ASSERT_TRUE(!_requestParser->parseConfigClause(requestPtr));
        ASSERT_EQ(ERROR_CONFIG_CLAUSE_JOIN_TYPE,
                             _requestParser->getErrorResult().getErrorCode());
    }

}

TEST_F(RequestParserTest, testParseQueryClause) {
    HA3_LOG(DEBUG, "Begin Test");
    RequestPtr requestPtr(new Request);

    string oriStr ="config=a&&query=default:'a:bc'";
    requestPtr->setOriginalString(oriStr);

    QueryClause *queryClause = requestPtr->getQueryClause();
    ASSERT_TRUE(queryClause);

    ASSERT_TRUE(_requestParser->parseQueryClause(requestPtr,
                    _clusterConfigInfo->getQueryInfo()));

    string expectedString ="default:'a:bc'";
    ASSERT_EQ(expectedString, queryClause->getOriginalString());
    const Query *query = queryClause->getRootQuery();
    ASSERT_TRUE(query);
}

TEST_F(RequestParserTest, testParseQueryClauseWithEmptyString) {
    HA3_LOG(DEBUG, "Begin Test");
    RequestPtr requestPtr(new Request);
    string oriStr = "config=a&&query=";
    requestPtr->setOriginalString(oriStr);
    QueryClause *queryClause = requestPtr->getQueryClause();
    ASSERT_TRUE(queryClause);
    ASSERT_TRUE(!_requestParser->parseQueryClause(requestPtr, _clusterConfigInfo->getQueryInfo()));
}

TEST_F(RequestParserTest, testParseRequestWithoutConfigClauseAndClusterClause) {
    HA3_LOG(DEBUG, "Begin Test");
    RequestPtr requestPtr(new Request);
    string requestStr = "query=phrase:aaa";
    requestPtr->setOriginalString(requestStr);
    ASSERT_TRUE(requestPtr->getQueryClause());

    ASSERT_TRUE(!_requestParser->parseRequest(requestPtr, _clusterConfigMapPtr));
}

TEST_F(RequestParserTest, testParseSimpleRankClause) {
    HA3_LOG(DEBUG, "Begin Test");

    string rankStr = "rank_profile:profile_name1,fieldboost:phrase.title(5000);phrase.body(2000)";
    string requestStr = "config=cluster:cluster1,start:0,hit:10&&rank=" + rankStr;

    RequestPtr requestPtr(new Request);
    requestPtr->setOriginalString(requestStr);
    ASSERT_TRUE(_requestParser->parseRequest(requestPtr, _clusterConfigMapPtr));

    RankClause *rankClause = requestPtr->getRankClause();
    ASSERT_TRUE(rankClause);
    ASSERT_EQ(rankStr, rankClause->getOriginalString());
    ASSERT_EQ(string("profile_name1"), rankClause->getRankProfileName());

    FieldBoostDescription fieldBoostDes = rankClause->getFieldBoostDescription();
    ASSERT_EQ((size_t)1, fieldBoostDes.size());
    ASSERT_TRUE(fieldBoostDes.end() != fieldBoostDes.find("phrase"));
    ASSERT_EQ((size_t)2, fieldBoostDes.find("phrase")->second.size());
    ASSERT_EQ((int32_t)5000, fieldBoostDes["phrase"]["title"]);
    ASSERT_EQ((int32_t)2000, fieldBoostDes["phrase"]["body"]);
}

TEST_F(RequestParserTest, testParseSimpleSortClause) {
    HA3_LOG(DEBUG, "Begin Test");

    string requestStr = "config=cluster:cluster1,start:0,hit:10&&sort=RANK;price1";
    RequestPtr requestPtr(new Request);
    requestPtr->setOriginalString(requestStr);
    ASSERT_TRUE(_requestParser->parseRequest(requestPtr, _clusterConfigMapPtr));

    SortClause *sortClause = requestPtr->getSortClause();
    ASSERT_TRUE(sortClause);
    ASSERT_EQ(string("RANK;price1"), sortClause->getOriginalString());
    const vector<SortDescription *> &sortDescriptions
        = sortClause->getSortDescriptions();
    ASSERT_EQ((size_t)2, sortDescriptions.size());

    ASSERT_EQ(string("RANK"), sortDescriptions[0]->getOriginalString());
    RankSyntaxExpr checkRankExpr;
    ASSERT_TRUE(checkRankExpr == (sortDescriptions[0]->getRootSyntaxExpr()));
    ASSERT_TRUE(!sortDescriptions[0]->getSortAscendFlag());

    AtomicSyntaxExpr checkExpr("price1", vt_unknown, ATTRIBUTE_NAME);
    ASSERT_TRUE(checkExpr == (sortDescriptions[1]->getRootSyntaxExpr()));
    ASSERT_TRUE(!sortDescriptions[1]->getSortAscendFlag());
}

TEST_F(RequestParserTest, testParseRankClauseWithRankHint) {
    HA3_LOG(DEBUG, "Begin Test");

    string rankStr = "rank_profile:profile_name1,rank_hint:+price";
    string requestStr = "config=cluster:cluster1,start:0,hit:10&&rank=" + rankStr;

    RequestPtr requestPtr(new Request);
    requestPtr->setOriginalString(requestStr);
    ASSERT_TRUE(_requestParser->parseRequest(requestPtr, _clusterConfigMapPtr));

    RankClause *rankClause = requestPtr->getRankClause();
    ASSERT_TRUE(rankClause);
    ASSERT_EQ(rankStr, rankClause->getOriginalString());
    ASSERT_EQ(string("profile_name1"), rankClause->getRankProfileName());

    const RankHint *rankHint = rankClause->getRankHint();
    ASSERT_TRUE(rankHint);
    ASSERT_EQ(string("price"), rankHint->sortField);
    ASSERT_EQ(sp_asc, rankHint->sortPattern);
}

TEST_F(RequestParserTest, testParseRankClauseWithRankHintFailed) {
    HA3_LOG(DEBUG, "Begin Test");

    string rankStr = "rank_profile:profile_name1,rank_hint:aaaprice";
    string requestStr = "config=cluster:cluster1,start:0,hit:10&&rank=" + rankStr;

    RequestPtr requestPtr(new Request);
    requestPtr->setOriginalString(requestStr);
    ASSERT_TRUE(!_requestParser->parseRequest(requestPtr, _clusterConfigMapPtr));
    ErrorCode ec = _requestParser->getErrorResult().getErrorCode();
    ASSERT_EQ((ErrorCode)ERROR_PARSE_RANK_HINT, ec);
}

TEST_F(RequestParserTest, testParseSortClauseWithSortFlag) {
    HA3_LOG(DEBUG, "Begin Test");

    string requestStr = "config=cluster:cluster1,start:0,hit:10&&sort=+price1;-type";

    RequestPtr requestPtr(new Request);
    requestPtr->setOriginalString(requestStr);
    _requestParser->parseRequest(requestPtr, _clusterConfigMapPtr);

    SortClause *sortClause = requestPtr->getSortClause();
    ASSERT_TRUE(sortClause);
    const vector<SortDescription *> &sortDescriptions
        = sortClause->getSortDescriptions();
    ASSERT_EQ((size_t)2, sortDescriptions.size());

    AtomicSyntaxExpr checkExpr1("price1", vt_unknown, ATTRIBUTE_NAME);
    ASSERT_TRUE(checkExpr1 ==  (sortDescriptions[0]->getRootSyntaxExpr()));
    ASSERT_TRUE(sortDescriptions[0]->getSortAscendFlag());

    AtomicSyntaxExpr checkExpr2("type", vt_unknown, ATTRIBUTE_NAME);
    ASSERT_TRUE(checkExpr2 ==  (sortDescriptions[1]->getRootSyntaxExpr()));
    ASSERT_TRUE(!sortDescriptions[1]->getSortAscendFlag());
}

TEST_F(RequestParserTest, testParseSortClauseWithAttrExpr) {
    HA3_LOG(DEBUG, "Begin Test");

    string requestStr = "config=cluster:cluster1,start:0,hit:10&&sort=+price1+price2;-price1*price2";

    RequestPtr requestPtr(new Request);
    requestPtr->setOriginalString(requestStr);
    _requestParser->parseRequest(requestPtr, _clusterConfigMapPtr);

    SortClause *sortClause = requestPtr->getSortClause();
    ASSERT_TRUE(sortClause);
    const vector<SortDescription *> &sortDescriptions
        = sortClause->getSortDescriptions();
    ASSERT_EQ((size_t)2, sortDescriptions.size());

    AtomicSyntaxExpr *checkExpr1 = new AtomicSyntaxExpr("price1", vt_unknown, ATTRIBUTE_NAME);
    AtomicSyntaxExpr *checkExpr2 = new AtomicSyntaxExpr("price2", vt_unknown, ATTRIBUTE_NAME);
    AddSyntaxExpr checkAddExpr(checkExpr1, checkExpr2);

    ASSERT_TRUE(checkAddExpr ==  (sortDescriptions[0]->getRootSyntaxExpr()));
    ASSERT_TRUE(sortDescriptions[0]->getSortAscendFlag());

    AtomicSyntaxExpr *checkExpr3 = new AtomicSyntaxExpr("price1", vt_unknown, ATTRIBUTE_NAME);
    AtomicSyntaxExpr *checkExpr4 = new AtomicSyntaxExpr("price2", vt_unknown, ATTRIBUTE_NAME);
    MulSyntaxExpr checkMulExpr(checkExpr3, checkExpr4);

    ASSERT_TRUE(checkMulExpr ==  (sortDescriptions[1]->getRootSyntaxExpr()));
    ASSERT_TRUE(!sortDescriptions[1]->getSortAscendFlag());
}

TEST_F(RequestParserTest, testParseSimpleDistinctClause) {
    HA3_LOG(DEBUG, "Begin Test");

    string requestStr = "config=cluster:cluster1,start:0,hit:10&&distinct="
                        "dist_key:price1,dist_filter:price2<3";

    RequestPtr requestPtr(new Request);
    requestPtr->setOriginalString(requestStr);
    bool ret = _requestParser->parseRequest(requestPtr, _clusterConfigMapPtr);
    ASSERT_TRUE(ret);

    DistinctClause *distinctClause = requestPtr->getDistinctClause();
    ASSERT_TRUE(distinctClause);
    ASSERT_EQ((uint32_t)1,
                         distinctClause->getDistinctDescriptionsCount());
    const DistinctDescription *distDesc =
        distinctClause->getDistinctDescription(0);
    ASSERT_TRUE(distDesc);

    ASSERT_EQ(string("dist_key:price1,dist_filter:price2<3"),
                         distDesc->getOriginalString());
    ASSERT_EQ(string(DEFAULT_DISTINCT_MODULE_NAME),
                         distDesc->getModuleName());
    ASSERT_EQ(1, distDesc->getDistinctCount());
    ASSERT_EQ(1, distDesc->getDistinctTimes());
    ASSERT_EQ(0, distDesc->getMaxItemCount());
    ASSERT_TRUE(distDesc->getReservedFlag());
    ASSERT_TRUE(!distDesc->getUpdateTotalHitFlag());

    AtomicSyntaxExpr checkExpr("price1", vt_unknown, ATTRIBUTE_NAME);
    ASSERT_TRUE(checkExpr ==  (distDesc->getRootSyntaxExpr()));

    FilterClause* filterClause = distDesc->getFilterClause();
    ASSERT_TRUE(filterClause);
    ASSERT_EQ(string("(price2<3)"),
                         filterClause->getOriginalString());

    AtomicSyntaxExpr *leftExpr = new AtomicSyntaxExpr("price2",
            vt_unknown, ATTRIBUTE_NAME);
    AtomicSyntaxExpr *rightExpr = new AtomicSyntaxExpr("3",
            vt_unknown, INTEGER_VALUE);
    LessSyntaxExpr checkFilterSyntaxExpr(leftExpr, rightExpr);
    ASSERT_TRUE(checkFilterSyntaxExpr == filterClause->getRootSyntaxExpr());
}

TEST_F(RequestParserTest, testParseDistinctClauseWithNoKey) {
    HA3_LOG(DEBUG, "Begin Test");

    string requestStr = "config=cluster:cluster1,start:0,hit:10&&"
                        "distinct=dist_times:2,dist_count:2";

    RequestPtr requestPtr(new Request);
    requestPtr->setOriginalString(requestStr);
    ASSERT_TRUE(!_requestParser->parseRequest(requestPtr,
                    _clusterConfigMapPtr));
    ErrorCode ec = _requestParser->getErrorResult().getErrorCode();
    ASSERT_EQ((ErrorCode)ERROR_DISTINCT_MISS_KEY, ec);

    DistinctClause *distinctClause = requestPtr->getDistinctClause();
    ASSERT_TRUE(distinctClause);

    ASSERT_EQ((uint32_t)0,
                         distinctClause->getDistinctDescriptionsCount());
}

TEST_F(RequestParserTest, testParseFullDistinctClause) {
    HA3_LOG(DEBUG, "Begin Test");

    string prefixRequestStr = "config=cluster:cluster1,start:0,hit:10&&"
                              "distinct=";
    string distClauseStr = "dist_key:price1/price2,dbmodule:testModule,"
                           "dist_count:5, dist_times: 7, max_item_count : 4000,"
                           "reserved : false,"
                           " update_total_hit : true,"
                           " grade :0.1|0.3 | 0.2";
    RequestPtr requestPtr(new Request);
    requestPtr->setOriginalString(prefixRequestStr + distClauseStr);
    bool ret = _requestParser->parseRequest(requestPtr, _clusterConfigMapPtr);
    ASSERT_TRUE(ret);

    DistinctClause *distinctClause = requestPtr->getDistinctClause();
    ASSERT_TRUE(distinctClause);
    ASSERT_EQ(string(distClauseStr),
                         distinctClause->getOriginalString());

    ASSERT_EQ((uint32_t)1,
                         distinctClause->getDistinctDescriptionsCount());

    const DistinctDescription *distDesc =
        distinctClause->getDistinctDescription(0);
    ASSERT_TRUE(distDesc);
    ASSERT_EQ(string("testModule"), distDesc->getModuleName());
    ASSERT_EQ(5, distDesc->getDistinctCount());
    ASSERT_EQ(7, distDesc->getDistinctTimes());
    ASSERT_EQ(4000, distDesc->getMaxItemCount());
    ASSERT_TRUE(!distDesc->getReservedFlag());
    ASSERT_TRUE(distDesc->getUpdateTotalHitFlag());
    vector<double> gradeThresholds = distDesc->getGradeThresholds();
    ASSERT_EQ((size_t)3, gradeThresholds.size());
    ASSERT_EQ((double)0.1, gradeThresholds[0]);
    ASSERT_EQ((double)0.2, gradeThresholds[1]);
    ASSERT_EQ((double)0.3, gradeThresholds[2]);
    AtomicSyntaxExpr *checkExpr1 = new AtomicSyntaxExpr("price1",
            vt_unknown, ATTRIBUTE_NAME);
    AtomicSyntaxExpr *checkExpr2 = new AtomicSyntaxExpr("price2",
            vt_unknown, ATTRIBUTE_NAME);
    DivSyntaxExpr checkDivExpr(checkExpr1, checkExpr2);
    ASSERT_TRUE(checkDivExpr ==  (distDesc->getRootSyntaxExpr()));
}

TEST_F(RequestParserTest, testParseMultiDistinctClause1) {
    HA3_LOG(DEBUG, "Begin Test");

    string prefixRequestStr = "config=cluster:cluster1,start:0,hit:10&&"
                              "distinct=";
    string distDescStr = "dist_key:price1,dist_count:5,dist_times:7,"
                           "reserved:false,update_total_hit : false";
    string distClauseStr = ";" + distDescStr + " ; none_dist ; ;";
    RequestPtr requestPtr(new Request);
    requestPtr->setOriginalString(prefixRequestStr + distClauseStr);
    bool ret = _requestParser->parseRequest(requestPtr,
            _clusterConfigMapPtr);
    ASSERT_TRUE(ret);

    DistinctClause *distinctClause = requestPtr->getDistinctClause();
    ASSERT_TRUE(distinctClause);
    ASSERT_EQ(distClauseStr,
                         distinctClause->getOriginalString());

    ASSERT_EQ((uint32_t)2,
                         distinctClause->getDistinctDescriptionsCount());

    const DistinctDescription *distDesc =
        distinctClause->getDistinctDescription(0);
    ASSERT_EQ(distDescStr, distDesc->getOriginalString());
    ASSERT_EQ(5, distDesc->getDistinctCount());
    ASSERT_EQ(7, distDesc->getDistinctTimes());
    ASSERT_EQ(0, distDesc->getMaxItemCount());
    ASSERT_TRUE(!distDesc->getReservedFlag());
    ASSERT_TRUE(!distDesc->getUpdateTotalHitFlag());
    vector<double> gradeThresholds = distDesc->getGradeThresholds();
    ASSERT_EQ((size_t)0, gradeThresholds.size());
    AtomicSyntaxExpr checkExpr("price1", vt_unknown, ATTRIBUTE_NAME);
    ASSERT_TRUE(checkExpr ==  (distDesc->getRootSyntaxExpr()));

    ASSERT_TRUE(!distinctClause->getDistinctDescription(1));
}

TEST_F(RequestParserTest, testParseMultiDistinctClause2) {
    HA3_LOG(DEBUG, "Begin Test");

    string prefixRequestStr = "config=cluster:cluster1,start:0,hit:10&&"
                              "distinct=";
    string distDescStr = "dist_key:price1,dist_count:5,dist_times:7,"
                         "reserved:false";
    string distClauseStr = " ; none_dist ; " + distDescStr + "; ;";
    RequestPtr requestPtr(new Request);
    requestPtr->setOriginalString(prefixRequestStr + distClauseStr);
    bool ret = _requestParser->parseRequest(requestPtr, _clusterConfigMapPtr);
    ASSERT_TRUE(ret);

    DistinctClause *distinctClause = requestPtr->getDistinctClause();
    ASSERT_TRUE(distinctClause);

    ASSERT_EQ((uint32_t)2,
                         distinctClause->getDistinctDescriptionsCount());

    const DistinctDescription *distDesc =
        distinctClause->getDistinctDescription(0);
    ASSERT_TRUE(!distDesc);

    distDesc = distinctClause->getDistinctDescription(1);
    ASSERT_TRUE(distDesc);
    ASSERT_EQ(distDescStr,
                         distDesc->getOriginalString());
    ASSERT_EQ(5, distDesc->getDistinctCount());
    ASSERT_EQ(7, distDesc->getDistinctTimes());
    ASSERT_TRUE(!distDesc->getReservedFlag());
    vector<double> gradeThresholds = distDesc->getGradeThresholds();
    ASSERT_EQ((size_t)0, gradeThresholds.size());
    AtomicSyntaxExpr checkExpr1("price1", vt_unknown, ATTRIBUTE_NAME);
    ASSERT_TRUE(checkExpr1 ==  (distDesc->getRootSyntaxExpr()));
}

TEST_F(RequestParserTest, testParseMultiDistinctClause3) {
    HA3_LOG(DEBUG, "Begin Test");

    string prefixRequestStr = "config=cluster:cluster1,start:0,hit:10&&"
                              "distinct=";
    string distDescStr1 = "dist_key:price1,dist_count:5,dist_times:7,"
                          "reserved:false,max_item_count:40,update_total_hit:true";
    string distDescStr2 = "dist_key:price2,dist_count:8,dist_times:9,"
                          "reserved:false,max_item_count:400,update_total_hit:false";
    string distClauseStr = " ; ; ;none_dist;" +
                           distDescStr1 + " ; " +
                           "none_dist;" +
                           distDescStr2 + ";none_dist; ; ";

    RequestPtr requestPtr(new Request);
    requestPtr->setOriginalString(prefixRequestStr + distClauseStr);
    bool ret = _requestParser->parseRequest(requestPtr, _clusterConfigMapPtr);
    ASSERT_TRUE(ret);

    DistinctClause *distinctClause = requestPtr->getDistinctClause();
    ASSERT_TRUE(distinctClause);

    ASSERT_EQ((uint32_t)5,
                         distinctClause->getDistinctDescriptionsCount());
    const DistinctDescription *distDesc =
        distinctClause->getDistinctDescription(0);
    ASSERT_TRUE(!distDesc);

    distDesc = distinctClause->getDistinctDescription(1);
    ASSERT_TRUE(distDesc);
    ASSERT_EQ(distDescStr1, distDesc->getOriginalString());
    ASSERT_EQ(5, distDesc->getDistinctCount());
    ASSERT_EQ(7, distDesc->getDistinctTimes());
    ASSERT_EQ(40, distDesc->getMaxItemCount());
    ASSERT_TRUE(!distDesc->getReservedFlag());
    ASSERT_TRUE(distDesc->getUpdateTotalHitFlag());
    vector<double> gradeThresholds = distDesc->getGradeThresholds();
    ASSERT_EQ((size_t)0, gradeThresholds.size());
    AtomicSyntaxExpr checkExpr1("price1", vt_unknown, ATTRIBUTE_NAME);
    ASSERT_TRUE(checkExpr1 ==  (distDesc->getRootSyntaxExpr()));

    distDesc = distinctClause->getDistinctDescription(2);
    ASSERT_TRUE(!distDesc);

    distDesc = distinctClause->getDistinctDescription(3);
    ASSERT_TRUE(distDesc);
    ASSERT_EQ(distDescStr2, distDesc->getOriginalString());
    ASSERT_EQ(8, distDesc->getDistinctCount());
    ASSERT_EQ(9, distDesc->getDistinctTimes());
    ASSERT_EQ(400, distDesc->getMaxItemCount());
    ASSERT_TRUE(!distDesc->getReservedFlag());
    ASSERT_TRUE(!distDesc->getUpdateTotalHitFlag());
    gradeThresholds = distDesc->getGradeThresholds();
    ASSERT_EQ((size_t)0, gradeThresholds.size());
    AtomicSyntaxExpr checkExpr2("price2", vt_unknown, ATTRIBUTE_NAME);
    ASSERT_TRUE(checkExpr2 ==  (distDesc->getRootSyntaxExpr()));

    distDesc = distinctClause->getDistinctDescription(4);
    ASSERT_TRUE(!distDesc);
}

TEST_F(RequestParserTest, testParseMultiEmptyDistinctClause) {
    HA3_LOG(DEBUG, "Begin Test");

    string prefixRequestStr = "config=cluster:cluster1,start:0,hit:10&&"
                              "distinct=";
    const char* distClauseStr = " ;;   ;";

    RequestPtr requestPtr(new Request);
    requestPtr->setOriginalString(prefixRequestStr + string(distClauseStr));
    bool ret = _requestParser->parseRequest(requestPtr,
            _clusterConfigMapPtr);
    ASSERT_TRUE(!ret);
    ASSERT_EQ(ERROR_DISTINCT_CLAUSE,
                         _requestParser->getErrorResult().getErrorCode());
}

TEST_F(RequestParserTest, testParseDistinctClauseWithErrorGradeKey) {
    HA3_LOG(DEBUG, "Begin Test");

    string prefixRequestStr = "config=cluster:cluster1,start:0,hit:10&&"
                              "distinct=";
    string distClauseStr = "dist_key:price1/price2,dbmodule:testModule,"
                           "dist_count:5,dist_times:7,reserved:false,"
                           "grade_xxx:0.1|0.3 | 0.2";
    RequestPtr requestPtr(new Request);
    requestPtr->setOriginalString(prefixRequestStr + distClauseStr);
    bool ret = _requestParser->parseRequest(requestPtr, _clusterConfigMapPtr);
    ASSERT_TRUE(!ret);
    ASSERT_EQ((ErrorCode)ERROR_DISTINCT_CLAUSE,
                         _requestParser->getErrorResult().getErrorCode());
}

TEST_F(RequestParserTest, testParseDistinctClauseWithErrorGrade) {
    HA3_LOG(DEBUG, "Begin Test");

    string prefixRequestStr = "config=cluster:cluster1,start:0,hit:10&&"
                              "distinct=";
    string distClauseStr = "dist_key:price1/price2,dbmodule:testModule,"
                           "dist_count:5,dist_times:7,reserved:false,"
                           "grade:a";
    RequestPtr requestPtr(new Request);
    requestPtr->setOriginalString(prefixRequestStr + distClauseStr);
    bool ret = _requestParser->parseRequest(requestPtr, _clusterConfigMapPtr);
    ASSERT_TRUE(!ret);
    ASSERT_EQ((ErrorCode)ERROR_DISTINCT_CLAUSE,
                         _requestParser->getErrorResult().getErrorCode());
}

TEST_F(RequestParserTest, testParseFilterClause) {
    HA3_LOG(DEBUG, "Begin Test!");

    string prefixRequestStr = "config=cluster:cluster1,start:0,hit:10&&filter=";
    string filterClauseStr = "(host=\"www.sina.com.cn\")";
    RequestPtr requestPtr(new Request);
    requestPtr->setOriginalString(prefixRequestStr + filterClauseStr);
    ASSERT_TRUE(_requestParser->parseRequest(requestPtr, _clusterConfigMapPtr));

    FilterClause *filterClause = requestPtr->getFilterClause();
    ASSERT_TRUE(filterClause);

    ASSERT_EQ(filterClauseStr, filterClause->getOriginalString());

    AtomicSyntaxExpr *checkExpr1 = new AtomicSyntaxExpr("host", vt_unknown, ATTRIBUTE_NAME);
    AtomicSyntaxExpr *checkExpr2 = new AtomicSyntaxExpr("www.sina.com.cn", vt_unknown, STRING_VALUE);
    EqualSyntaxExpr checkEqualExpr(checkExpr1, checkExpr2);
    ASSERT_TRUE(checkEqualExpr ==  (filterClause->getRootSyntaxExpr()));
}

TEST_F(RequestParserTest, testParseSimpleAggregateClause) {
    HA3_LOG(DEBUG, "Begin Test");
    string aggStr = "group_key:type,agg_fun:count()";
    string requestStr = "config=cluster:cluster1,start:0,hit:10&&aggregate=" + aggStr;

    RequestPtr requestPtr(new Request);
    requestPtr->setOriginalString(requestStr);
    ASSERT_TRUE(_requestParser->parseRequest(requestPtr, _clusterConfigMapPtr));

    auto aggClause = requestPtr->getAggregateClause();
    ASSERT_TRUE(aggClause);
    const AggregateDescriptions &aggDescriptions
        = aggClause->getAggDescriptions();
    ASSERT_EQ((size_t)1, aggDescriptions.size());

    AggregateDescription *aggDes = aggDescriptions[0];
    ASSERT_TRUE(aggDes);
    ASSERT_EQ(aggStr, aggDes->getOriginalString());
    SyntaxExpr *groupKeyExpr = aggDes->getGroupKeyExpr();
    ASSERT_TRUE(groupKeyExpr);
    AtomicSyntaxExpr checkGroupKeyExpr("type", vt_unknown, ATTRIBUTE_NAME);
    ASSERT_TRUE(checkGroupKeyExpr == groupKeyExpr);
    ASSERT_TRUE(!aggDes->isRangeAggregate());
    FilterClause* filterClause = aggDes->getFilterClause();
    ASSERT_TRUE(!filterClause);
    ASSERT_EQ(MAX_AGGREGATE_GROUP_COUNT, aggDes->getMaxGroupCount());

    const vector<AggFunDescription*>& aggFuns
        = aggDes->getAggFunDescriptions();
    ASSERT_EQ((size_t)1, aggFuns.size());

    ASSERT_EQ(string("count"), aggFuns[0]->getFunctionName());
    SyntaxExpr *aggFunExpr = aggFuns[0]->getSyntaxExpr();
    ASSERT_TRUE(!aggFunExpr); // 'count' function has no parameter syntax expression.
}

TEST_F(RequestParserTest, testParseEmptyAggregateClause) {
    HA3_LOG(DEBUG, "Begin Test");
    string requestStr = "config=cluster:cluster1,start:0,hit:10&&aggregate=";

    RequestPtr requestPtr(new Request);
    requestPtr->setOriginalString(requestStr);
    ASSERT_TRUE(!_requestParser->parseRequest(requestPtr, _clusterConfigMapPtr));
    ASSERT_EQ(ERROR_AGGREGATE_CLAUSE, _requestParser->getErrorResult().getErrorCode());
}

TEST_F(RequestParserTest, testParseAggregateClauseFailed) {
    HA3_LOG(DEBUG, "Begin Test");
    string requestStr = "config=cluster:cluster1,start:0,hit:10&&aggregate=group_key:id";

    RequestPtr requestPtr(new Request);
    requestPtr->setOriginalString(requestStr);
    ASSERT_TRUE(!_requestParser->parseRequest(requestPtr, _clusterConfigMapPtr));
    ASSERT_EQ(ERROR_AGGREGATE_CLAUSE, _requestParser->getErrorResult().getErrorCode());
}

TEST_F(RequestParserTest, testParseRequest) {
    HA3_LOG(DEBUG, "Begin Test");
    string requestStr = "cluster=cluster1,cluster3&&query=default:word1";

    RequestPtr requestPtr(new Request);
    requestPtr->setOriginalString(requestStr);
    _requestParser->parseRequest(requestPtr, _clusterConfigMapPtr);

    ConfigClause *configClause = requestPtr->getConfigClause();
    ASSERT_EQ((size_t)2, configClause->getClusterNameVector().size());
    ASSERT_TRUE(configClause);
    ASSERT_EQ(string("cluster1.default"), configClause->getClusterName());

    QueryClause *queryClause = requestPtr->getQueryClause();
    ASSERT_TRUE(queryClause);
}

TEST_F(RequestParserTest, testParseFullAggregateClause) {
    HA3_LOG(DEBUG, "Begin Test");
    string aggregateStr1 = "group_key:price1,range:1~4~10,agg_fun:max(price1)#min(price2),"
                           "agg_filter:price1>price2,agg_sampler_threshold:10,agg_sampler_step:2";
    string aggregateStr2 = "group_key:type,agg_fun:count()";
    string requestStr = "config=cluster:cluster1,start:0,hit:10&&aggregate="
                        + aggregateStr1 + AGGREGATE_CLAUSE_SEPERATOR + aggregateStr2;

    RequestPtr requestPtr(new Request);
    requestPtr->setOriginalString(requestStr);
    _requestParser->parseRequest(requestPtr, _clusterConfigMapPtr);

    auto aggClause = requestPtr->getAggregateClause();
    ASSERT_TRUE(aggClause);
    const AggregateDescriptions &aggDescriptions
        = aggClause->getAggDescriptions();
    ASSERT_EQ((size_t)2, aggDescriptions.size());

    AggregateDescription *aggDes1 = aggDescriptions[0];
    ASSERT_TRUE(aggDes1);
    ASSERT_EQ(aggregateStr1, aggDes1->getOriginalString());
    SyntaxExpr *groupKeyExpr1 = aggDes1->getGroupKeyExpr();
    ASSERT_TRUE(groupKeyExpr1);
    AtomicSyntaxExpr checkGroupKeyExpr1("price1", vt_unknown, ATTRIBUTE_NAME);
    ASSERT_TRUE(checkGroupKeyExpr1 == groupKeyExpr1);

    //test range
    ASSERT_TRUE(aggDes1->isRangeAggregate());
    const vector<string> &range = aggDes1->getRange();
    ASSERT_EQ((size_t)3, range.size());
    ASSERT_EQ(string("1"), range[0]);
    ASSERT_EQ(string("4"), range[1]);
    ASSERT_EQ(string("10"), range[2]);

    //test aggregate filter
    FilterClause* filterClause = aggDes1->getFilterClause();
    ASSERT_TRUE(filterClause);
    const SyntaxExpr *aggFilterExpr = filterClause->getRootSyntaxExpr();
    ASSERT_TRUE(aggFilterExpr);
    AtomicSyntaxExpr *leftExpr = new AtomicSyntaxExpr("price1", vt_unknown, ATTRIBUTE_NAME);
    AtomicSyntaxExpr *rightExpr = new AtomicSyntaxExpr("price2", vt_unknown, ATTRIBUTE_NAME);
    GreaterSyntaxExpr checkFilterExpr(leftExpr, rightExpr);
    ASSERT_TRUE(checkFilterExpr == aggFilterExpr);

    //test aggregate function
    const vector<AggFunDescription*>& aggFuns1
        = aggDes1->getAggFunDescriptions();
    ASSERT_EQ((size_t)2, aggFuns1.size());
    ASSERT_EQ(string("max"), aggFuns1[0]->getFunctionName());
    const SyntaxExpr *aggFunExpr1_1 = aggFuns1[0]->getSyntaxExpr();
    ASSERT_TRUE(aggFunExpr1_1);
    AtomicSyntaxExpr checkFunExpr1_1("price1", vt_unknown, ATTRIBUTE_NAME);
    ASSERT_TRUE(checkFunExpr1_1 == aggFunExpr1_1);

    ASSERT_EQ(string("min"), aggFuns1[1]->getFunctionName());
    const SyntaxExpr *aggFunExpr1_2 = aggFuns1[1]->getSyntaxExpr();
    ASSERT_TRUE(aggFunExpr1_2);
    AtomicSyntaxExpr checkFunExpr1_2("price2", vt_unknown, ATTRIBUTE_NAME);
    ASSERT_TRUE(checkFunExpr1_2 == aggFunExpr1_2);

    //test aggregate sampler
    ASSERT_EQ((uint32_t)10, aggDes1->getAggThreshold());
    ASSERT_EQ((uint32_t)2, aggDes1->getSampleStep());

    //////////////////////// test second 'AggregateDescription' ///////////////////////////////
    AggregateDescription *aggDes2 = aggDescriptions[1];
    ASSERT_TRUE(aggDes2);
    ASSERT_EQ(aggregateStr2, aggDes2->getOriginalString());
    const SyntaxExpr *groupKeyExpr2 = aggDes2->getGroupKeyExpr();
    ASSERT_TRUE(groupKeyExpr2);
    AtomicSyntaxExpr checkGroupKeyExpr2("type", vt_unknown, ATTRIBUTE_NAME);
    ASSERT_TRUE(checkGroupKeyExpr2 == groupKeyExpr2);
    ASSERT_TRUE(!aggDes2->isRangeAggregate());
    FilterClause* filterClause2 = aggDes2->getFilterClause();
    ASSERT_TRUE(!filterClause2);
    ASSERT_EQ(MAX_AGGREGATE_GROUP_COUNT, aggDes2->getMaxGroupCount());

    const vector<AggFunDescription*>& aggFuns2
        = aggDes2->getAggFunDescriptions();
    ASSERT_EQ((size_t)1, aggFuns2.size());
    ASSERT_EQ(string("count"), aggFuns2[0]->getFunctionName());
    const SyntaxExpr *aggFunExpr2 = aggFuns2[0]->getSyntaxExpr();
    ASSERT_TRUE(!aggFunExpr2); // 'count' function has no parameter syntax expression.
}

TEST_F(RequestParserTest, testAggParseWithMultiSameFun) {
    HA3_LOG(DEBUG, "Begin Test");
    string aggStr = "group_key:type,agg_fun:count()#count()#"
                    "max(price1)#max(price2)#max(price2)";
    string requestStr = "config=cluster:cluster1,start:0,hit:10&&aggregate=" + aggStr;

    RequestPtr requestPtr(new Request);
    requestPtr->setOriginalString(requestStr);
    _requestParser->parseRequest(requestPtr, _clusterConfigMapPtr);

    auto aggClause = requestPtr->getAggregateClause();
    ASSERT_TRUE(aggClause);
    const AggregateDescriptions &aggDescriptions
        = aggClause->getAggDescriptions();
    ASSERT_EQ((size_t)1, aggDescriptions.size());

    AggregateDescription *aggDes = aggDescriptions[0];
    ASSERT_TRUE(aggDes);
    ASSERT_EQ(aggStr, aggDes->getOriginalString());
    SyntaxExpr *groupKeyExpr = aggDes->getGroupKeyExpr();
    ASSERT_TRUE(groupKeyExpr);
    AtomicSyntaxExpr checkGroupKeyExpr("type", vt_unknown, ATTRIBUTE_NAME);
    ASSERT_TRUE(checkGroupKeyExpr == groupKeyExpr);
    ASSERT_TRUE(!aggDes->isRangeAggregate());
    FilterClause* filterClause = aggDes->getFilterClause();
    ASSERT_TRUE(!filterClause);
    ASSERT_EQ(MAX_AGGREGATE_GROUP_COUNT, aggDes->getMaxGroupCount());

    const vector<AggFunDescription*>& aggFuns
        = aggDes->getAggFunDescriptions();
    ASSERT_EQ((size_t)3, aggFuns.size());

    ASSERT_EQ(string("count"), aggFuns[0]->getFunctionName());
    ASSERT_EQ(string("max"), aggFuns[1]->getFunctionName());
    ASSERT_EQ(string("max"), aggFuns[2]->getFunctionName());
    SyntaxExpr *aggFunExpr = aggFuns[0]->getSyntaxExpr();
    ASSERT_TRUE(!aggFunExpr); // 'count' function has no parameter syntax expression.
}

TEST_F(RequestParserTest, testBug127774) {
    HA3_LOG(DEBUG, "Begin Test");
    string aggStr = "group_key:type,agg_fun:max(id+1)#max(  ( id + ( 1 )))";
    string requestStr = "config=cluster:cluster1,start:0,hit:10&&aggregate=" + aggStr;

    RequestPtr requestPtr(new Request);
    requestPtr->setOriginalString(requestStr);
    _requestParser->parseRequest(requestPtr, _clusterConfigMapPtr);

    auto aggClause = requestPtr->getAggregateClause();
    ASSERT_TRUE(aggClause);
    const AggregateDescriptions &aggDescriptions
        = aggClause->getAggDescriptions();
    ASSERT_EQ((size_t)1, aggDescriptions.size());

    AggregateDescription *aggDes = aggDescriptions[0];
    ASSERT_TRUE(aggDes);
    ASSERT_EQ(aggStr, aggDes->getOriginalString());
    SyntaxExpr *groupKeyExpr = aggDes->getGroupKeyExpr();
    ASSERT_TRUE(groupKeyExpr);
    AtomicSyntaxExpr checkGroupKeyExpr("type", vt_unknown, ATTRIBUTE_NAME);
    ASSERT_TRUE(checkGroupKeyExpr == groupKeyExpr);
    ASSERT_TRUE(!aggDes->isRangeAggregate());
    FilterClause* filterClause = aggDes->getFilterClause();
    ASSERT_TRUE(!filterClause);
    ASSERT_EQ(MAX_AGGREGATE_GROUP_COUNT, aggDes->getMaxGroupCount());

    const vector<AggFunDescription*>& aggFuns
        = aggDes->getAggFunDescriptions();
    ASSERT_EQ((size_t)1, aggFuns.size());

    ASSERT_EQ(string("max"), aggFuns[0]->getFunctionName());
    ASSERT_EQ(string("max((id+1))"), aggFuns[0]->toString());

    SyntaxExpr *aggFunExpr = aggFuns[0]->getSyntaxExpr();
    ASSERT_TRUE(aggFunExpr);
    ASSERT_EQ(string("(id+1)"), aggFunExpr->getExprString());
}

TEST_F(RequestParserTest, testCheckErrorResult) {
    map<string, ErrorCode> errMap;
    errMap["config=cluster:cluster1,start:10,hit:10&&query=[10,]"] = ERROR_QUERY_CLAUSE;
    errMap["query=str"] = ERROR_CLUSTER_NAME_NOT_EXIST;
    errMap["config=cluster:no_such_cluster,start:0,hit:10&&query=str"] = ERROR_CLUSTER_NAME_NOT_EXIST;
    errMap["config=cluster:cluster1,start:,hit:10&&query=str"] = ERROR_KEY_VALUE_PAIR;
    errMap["config=cluster:cluster1,analyzer:&&query=str"] = ERROR_KEY_VALUE_PAIR;
    errMap["config=cluster:cluster1,cache_info:&&query=str"] = ERROR_KEY_VALUE_PAIR;
    errMap["config=cluster:cluster1,start:10,hit:10&&query="] = ERROR_QUERY_CLAUSE;
    errMap["config=cluster:cluster1,start:10,hit:10&&sort="] = ERROR_SORT_NO_DESCRIPTION;
    errMap["config=cluster:cluster1,start:10,hit:10&&query=phrasenotexist:[10,1]"] = ERROR_QUERY_CLAUSE;

    errMap[""] = ERROR_EMPTY_QUERY_STRING;
    errMap["config=cluster:cluster1&&query=xx&&unknownclause=fake"] = ERROR_UNKNOWN_CLAUSE;
    errMap["config=cluster:cluster1&&query=xx&&rank=rank_profile:defaultprofile,fieldboost:price(100)"] = ERROR_NO_DOT_FIELD_BOOST;
    errMap["config=cluster:cluster1&&query=xx&&rank=rank_profile:defaultprofile,fieldboost:price.a(abc)"] = ERROR_FIELD_BOOST_NUMBER;
    errMap["config=cluster:cluster1&&query=xx&&rank=rank_profile:defaultprofile,fieldboost:price.a(100"] = ERROR_FIELD_BOOST_CONFIG;
    errMap["config=cluster:cluster1&&query=xx&&rank=rank_profile:defaultprofile,fieldboost:price.a(100),unknownkey:1"] = ERROR_UNKNOWN_KEY;
    errMap["config=cluster:cluster1&&query=xx&&distinct=dist_key:price,dist_count:abc"] = ERROR_DISTINCT_CLAUSE;
    errMap["config=cluster:cluster1&&query=xx&&distinct=dist_key:price,dist_count:2,dist_times:abc"] = ERROR_DISTINCT_CLAUSE;
    errMap["config=cluster:cluster1&&query=xx&&distinct=dist_key:price,dist_count:2,dist_times:1,reserved:tru"] = ERROR_DISTINCT_RESERVED_FLAG;

    for (map<string, ErrorCode>::const_iterator it = errMap.begin();
         it != errMap.end(); ++it)
    {
        checkErrorResult(it->first, it->second);
    }
}

TEST_F(RequestParserTest, testParseRequestWhitEmptyDistinctClauseValue) {
    HA3_LOG(DEBUG, "Begin Test");
    string requestStr = "config=cluster:cluster1&&query=default:word1&&distinct=";
    RequestPtr requestPtr(new Request);
    requestPtr->setOriginalString(requestStr);
    bool ret = _requestParser->parseRequest(requestPtr, _clusterConfigMapPtr);
    ASSERT_TRUE(!ret);
    ASSERT_EQ(ERROR_DISTINCT_CLAUSE,
                         _requestParser->getErrorResult().getErrorCode());
}

void RequestParserTest::checkErrorResult(string queryStr, ErrorCode errorCode) {
    HA3_LOG(DEBUG, "begin test. Query string:[%s]", queryStr.c_str());
    RequestPtr requestPtr(new Request);

    requestPtr->setOriginalString(queryStr);
    delete _requestParser;
    _requestParser = new RequestParser;

    ASSERT_TRUE(!_requestParser->parseRequest(requestPtr, _clusterConfigMapPtr))<<queryStr;

    string message = string("Expected: ") + haErrorCodeToString(errorCode) +
                    "\n- Actual: " + _requestParser->getErrorResult().getErrorMsg() +
                    "\n- QueryString: " + queryStr;
    ASSERT_EQ(errorCode, _requestParser->getErrorResult().getErrorCode()) << message;

}

TEST_F(RequestParserTest, testInvalidRequest) {
    HA3_LOG(DEBUG, "Begin Test");
    string requestStr = "1234fdf";

    RequestPtr requestPtr(new Request);
    requestPtr->setOriginalString(requestStr);
    bool ret = _requestParser->parseRequest(requestPtr, _clusterConfigMapPtr);
    ASSERT_TRUE(!ret);
}

TEST_F(RequestParserTest, testParseConfigClauseWithRankTrace)
{
    HA3_LOG(DEBUG, "Begin Test");
    RequestPtr requestPtr(new Request);
    string oriStr = "config=cluster:cluster1,rank_trace:TRACE3";
    requestPtr->setOriginalString(oriStr);

    ConfigClause *configClause = requestPtr->getConfigClause();
    ASSERT_TRUE(configClause);

    ASSERT_TRUE(_requestParser->parseConfigClause(requestPtr));

    string expectedString = "cluster:cluster1,rank_trace:TRACE3";
    ASSERT_EQ(expectedString, configClause->getOriginalString());
    ASSERT_EQ(string("cluster1.default"), configClause->getClusterName());
    ASSERT_EQ(string("TRACE3"), configClause->getRankTrace());
}

TEST_F(RequestParserTest, testParseConfigClauseWithCacheConfig)
{
    HA3_LOG(DEBUG, "Begin Test");
    RequestPtr requestPtr(new Request);
    string oriStr = "config=cluster:cluster1,use_cache:no";
    requestPtr->setOriginalString(oriStr);

    ConfigClause *configClause = requestPtr->getConfigClause();
    ASSERT_TRUE(configClause);

    ASSERT_TRUE(_requestParser->parseConfigClause(requestPtr));

    string expectedString = "cluster:cluster1,use_cache:no";
    ASSERT_EQ(expectedString, configClause->getOriginalString());
    ASSERT_TRUE(!configClause->useCache());
}
TEST_F(RequestParserTest, testParseConfigClauseWithoutCacheConfig) {
    HA3_LOG(DEBUG, "Begin Test");
    RequestPtr requestPtr(new Request);
    string oriStr = "config=cluster:cluster1";
    requestPtr->setOriginalString(oriStr);

    ConfigClause *configClause = requestPtr->getConfigClause();
    ASSERT_TRUE(configClause);

    _requestParser->parseConfigClause(requestPtr);

    string expectedString = "cluster:cluster1";
    ASSERT_EQ(expectedString, configClause->getOriginalString());
    ASSERT_TRUE(configClause->useCache());
}

TEST_F(RequestParserTest, testParseConfigClauseWithCacheConfigNotNo){
    HA3_LOG(DEBUG, "Begin Test");
    RequestPtr requestPtr(new Request);

    string oriStr = "config=cluster:cluster1,use_cache:es";
    requestPtr->setOriginalString(oriStr);

    ConfigClause *configClause = requestPtr->getConfigClause();
    _requestParser->parseConfigClause(requestPtr);

    ASSERT_TRUE(configClause);

    string expectedString = "cluster:cluster1,use_cache:es";
    ASSERT_EQ(expectedString, configClause->getOriginalString());
    ASSERT_TRUE(configClause->useCache());
}

TEST_F(RequestParserTest, testParseConfigClauseForCacheInfo)
{
    HA3_LOG(DEBUG, "Begin Test");
    {
        RequestPtr requestPtr(new Request);
        string oriStr = "config=cluster:cluster1,cache_info:n";
        requestPtr->setOriginalString(oriStr);

        ConfigClause *configClause = requestPtr->getConfigClause();
        ASSERT_TRUE(configClause);
        ASSERT_TRUE(_requestParser->parseConfigClause(requestPtr));
        ASSERT_TRUE(!configClause->getDisplayCacheInfo());
    }

    {
        RequestPtr requestPtr(new Request);
        string oriStr = "config=cluster:cluster1,cache_info:xyz";
        requestPtr->setOriginalString(oriStr);

        ConfigClause *configClause = requestPtr->getConfigClause();
        ASSERT_TRUE(_requestParser->parseConfigClause(requestPtr));
        ASSERT_TRUE(!configClause->getDisplayCacheInfo());
    }

    {
        RequestPtr requestPtr(new Request);
        string oriStr = "config=cluster:cluster1,cache_info:y";
        requestPtr->setOriginalString(oriStr);

        ConfigClause *configClause = requestPtr->getConfigClause();
        ASSERT_TRUE(_requestParser->parseConfigClause(requestPtr));
        ASSERT_TRUE(configClause->getDisplayCacheInfo());
    }

    {
        RequestPtr requestPtr(new Request);
        string oriStr = "config=cluster:cluster1,cache_info:yes";
        requestPtr->setOriginalString(oriStr);

        ConfigClause *configClause = requestPtr->getConfigClause();
        ASSERT_TRUE(_requestParser->parseConfigClause(requestPtr));
        ASSERT_TRUE(configClause->getDisplayCacheInfo());
    }

    {//test error config
        RequestPtr requestPtr(new Request);
        string oriStr = "config=cluster:cluster1,cache_info:";
        requestPtr->setOriginalString(oriStr);

        ASSERT_TRUE(!_requestParser->parseConfigClause(requestPtr));
    }
}
TEST_F(RequestParserTest, testParseConfigClauseForDebugQuery){
    HA3_LOG(DEBUG, "Begin Test");
    {
        RequestPtr requestPtr(new Request);
        string oriStr = "config=cluster:cluster1,start:1,no_summary:yes,use_cache:yes";
        requestPtr->setOriginalString(oriStr);
        ConfigClause *configClause = requestPtr->getConfigClause();
        ASSERT_TRUE(configClause);
        ASSERT_TRUE(_requestParser->parseConfigClause(requestPtr));
        ASSERT_TRUE(configClause->getDebugQueryKey().empty());
        ASSERT_EQ(uint32_t(1), configClause->getStartOffset());
        ASSERT_TRUE(configClause->isNoSummary());
        ASSERT_TRUE(configClause->useCache());
    }
    {
        RequestPtr requestPtr(new Request);
        string oriStr = "config=cluster:cluster1,trace:TRACE2,rank_trace:DEBUG,debug_query_key:123,start:1,no_summary:yes,use_cache:yes";
        requestPtr->setOriginalString(oriStr);
        ConfigClause *configClause = requestPtr->getConfigClause();
        ASSERT_TRUE(configClause);
        ASSERT_TRUE(_requestParser->parseConfigClause(requestPtr));
        ASSERT_EQ(string("123"), configClause->getDebugQueryKey());
        ASSERT_EQ(string("TRACE2"), configClause->getTrace());
        ASSERT_EQ(string("DEBUG"), configClause->getRankTrace());
        ASSERT_EQ(uint32_t(0), configClause->getStartOffset());
        ASSERT_TRUE(!configClause->isNoSummary());
        ASSERT_TRUE(!configClause->useCache());
    }
    {
        RequestPtr requestPtr(new Request);
        string oriStr = "config=cluster:cluster1,trace:ERROR,rank_trace:WARN,debug_query_key:123,start:1,no_summary:no,use_cache:no";
        requestPtr->setOriginalString(oriStr);
        ConfigClause *configClause = requestPtr->getConfigClause();
        ASSERT_TRUE(configClause);
        ASSERT_TRUE(_requestParser->parseConfigClause(requestPtr));
        ASSERT_EQ(string("123"), configClause->getDebugQueryKey());
        ASSERT_EQ(string("INFO"), configClause->getTrace());
        ASSERT_EQ(string("INFO"), configClause->getRankTrace());
        ASSERT_EQ(uint32_t(0), configClause->getStartOffset());
        ASSERT_TRUE(!configClause->isNoSummary());
        ASSERT_TRUE(!configClause->useCache());
    }

}

TEST_F(RequestParserTest, testParseConfigClauseForOptimizeInfo)
{
    HA3_LOG(DEBUG, "Begin Test");
    {
        RequestPtr requestPtr(new Request);
        string oriStr = "config=cluster:cluster1,batch_score:yes";
        requestPtr->setOriginalString(oriStr);
        ConfigClause *configClause = requestPtr->getConfigClause();
        ASSERT_TRUE(configClause);
        ASSERT_TRUE(_requestParser->parseConfigClause(requestPtr));
        ASSERT_TRUE(configClause->isBatchScore());
    }
    {
        RequestPtr requestPtr(new Request);
        string oriStr = "config=cluster:cluster1,batch_score:no";
        requestPtr->setOriginalString(oriStr);
        ConfigClause *configClause = requestPtr->getConfigClause();
        ASSERT_TRUE(configClause);
        ASSERT_TRUE(_requestParser->parseConfigClause(requestPtr));
        ASSERT_TRUE(!configClause->isBatchScore());
    }
    {
        RequestPtr requestPtr(new Request);
        string oriStr = "config=cluster:cluster1,optimize_rank:yes";
        requestPtr->setOriginalString(oriStr);
        ConfigClause *configClause = requestPtr->getConfigClause();
        ASSERT_TRUE(configClause);
        ASSERT_TRUE(_requestParser->parseConfigClause(requestPtr));
        ASSERT_TRUE(configClause->isOptimizeRank());
    }
    {
        RequestPtr requestPtr(new Request);
        string oriStr = "config=cluster:cluster1,optimize_rank:no";
        requestPtr->setOriginalString(oriStr);
        ConfigClause *configClause = requestPtr->getConfigClause();
        ASSERT_TRUE(configClause);
        ASSERT_TRUE(_requestParser->parseConfigClause(requestPtr));
        ASSERT_TRUE(!configClause->isOptimizeRank());
    }
    {
        RequestPtr requestPtr(new Request);
        string oriStr = "config=cluster:cluster1,optimize_comparator:yes";
        requestPtr->setOriginalString(oriStr);
        ConfigClause *configClause = requestPtr->getConfigClause();
        ASSERT_TRUE(configClause);
        ASSERT_TRUE(_requestParser->parseConfigClause(requestPtr));
        ASSERT_TRUE(configClause->isOptimizeComparator());
    }
    {
        RequestPtr requestPtr(new Request);
        string oriStr = "config=cluster:cluster1,optimize_comparator:no";
        requestPtr->setOriginalString(oriStr);
        ConfigClause *configClause = requestPtr->getConfigClause();
        ASSERT_TRUE(configClause);
        ASSERT_TRUE(_requestParser->parseConfigClause(requestPtr));
        ASSERT_TRUE(!configClause->isOptimizeComparator());
    }
}

TEST_F(RequestParserTest, testParseConfigClauseWithErrorRankTrace){
    HA3_LOG(DEBUG, "Begin Test");
    RequestPtr requestPtr(new Request);

    string oriStr = "config=cluster:cluster1,rnk_trace:TRACE3";
    requestPtr->setOriginalString(oriStr);

    ConfigClause *configClause = requestPtr->getConfigClause();
    _requestParser->parseConfigClause(requestPtr);

    ASSERT_TRUE(configClause);

    string expectedString = "cluster:cluster1,rnk_trace:TRACE3";
    ASSERT_EQ(expectedString, configClause->getOriginalString());
    ASSERT_EQ(string("cluster1.default"), configClause->getClusterName());
    ASSERT_EQ(string(""), configClause->getRankTrace());
}

//BugID=62277
TEST_F(RequestParserTest, testParseConfigClauseWithNullValue)
{
    HA3_LOG(DEBUG, "Begin Test");
    RequestPtr requestPtr(new Request);
    string oriStr = ":,start:50,hit:10,format:xml";
    requestPtr->setOriginalString(oriStr);

    ConfigClause *configClause = requestPtr->getConfigClause();
    ASSERT_TRUE(configClause);
    bool ret = _requestParser->parseConfigClause(requestPtr);
    ASSERT_TRUE(ret);

    string oriStr1 = "cluster:daogou1,:";
    requestPtr->setOriginalString(oriStr1);
    ConfigClause *configClause1 = requestPtr->getConfigClause();
    ASSERT_TRUE(configClause1);
    ret = _requestParser->parseConfigClause(requestPtr);
    ASSERT_TRUE(ret);
}

//BugID=62277
TEST_F(RequestParserTest, testParseRequestWithNullValue)
{
    HA3_LOG(DEBUG, "Begin Test");

    string requestStr = "=&&";
    RequestPtr requestPtr(new Request);
    requestPtr->setOriginalString(requestStr);

    bool ret = _requestParser->parseRequest(requestPtr, _clusterConfigMapPtr);

    ASSERT_EQ(ERROR_UNKNOWN_CLAUSE,
                         _requestParser->getErrorResult().getErrorCode());
    ASSERT_TRUE(!ret);
}
TEST_F(RequestParserTest, testParseConfigClauseKVPairsWithNullValue)
{
    HA3_LOG(DEBUG, "Begin Test");
    RequestPtr requestPtr(new Request);
    string oriStr = "config=cluster:cluster1,start:50,hit:10,kvpairs:#;#;key2#value2";
    requestPtr->setOriginalString(oriStr);
    ConfigClause *configClause = requestPtr->getConfigClause();
    ASSERT_TRUE(configClause);
    ASSERT_TRUE(!_requestParser->parseConfigClause(requestPtr));
}

TEST_F(RequestParserTest, testParseConfigClauseTwice)
{
    HA3_LOG(DEBUG, "Begin Test");
    RequestPtr requestPtr(new Request);
    string oriStr = "config=cluster:cluster1,use_cache:no";
    requestPtr->setOriginalString(oriStr);

    ConfigClause *configClause = requestPtr->getConfigClause();
    ASSERT_TRUE(configClause);

    ASSERT_TRUE(_requestParser->parseConfigClause(requestPtr));
    ASSERT_TRUE(_requestParser->parseConfigClause(requestPtr));

    ASSERT_EQ((size_t)1, configClause->getClusterNameCount());
}

TEST_F(RequestParserTest, testParseClusterClause) {
    HA3_LOG(DEBUG, "Begin test");

    RequestPtr requestPtr(new Request);
    string oriStr = "cluster=cluster1:part_ids=0|3|65535,"
                    "cluster2:hash_field=user0|user2,"
                    "cluster3";

    requestPtr->setOriginalString(oriStr);

    ClusterClause *clusterClause = requestPtr->getClusterClause();
    ASSERT_TRUE(clusterClause);

    bool ret = _requestParser->parseClusterClause(requestPtr,
            _clusterConfigMapPtr);
    ASSERT_TRUE(ret)<<clusterClause->getOriginalString();

    string clusterName = "cluster1.default";
    vector<pair<hashid_t, hashid_t> > clusterPartitions;
    ret = clusterClause->getClusterPartIds(clusterName, clusterPartitions);
    ASSERT_TRUE(ret);
    ASSERT_EQ((size_t)3, clusterPartitions.size());
    ASSERT_EQ((hashid_t)0, clusterPartitions[0].first);
    ASSERT_EQ((hashid_t)0, clusterPartitions[0].second);
    ASSERT_EQ((hashid_t)3, clusterPartitions[1].first);
    ASSERT_EQ((hashid_t)3, clusterPartitions[1].second);
    ASSERT_EQ((hashid_t)65535, clusterPartitions[2].first);
    ASSERT_EQ((hashid_t)65535, clusterPartitions[2].second);

    clusterName = "cluster2.default";
    ret = clusterClause->getClusterPartIds(clusterName, clusterPartitions);
    ASSERT_TRUE(ret);
    ASSERT_EQ((size_t)2, clusterPartitions.size());

    ASSERT_EQ((hashid_t)_hashFuncPtr->getHashId("user0"),
                         clusterPartitions[0].first);
    ASSERT_EQ((hashid_t)_hashFuncPtr->getHashId("user0"),
                         clusterPartitions[0].second);
    ASSERT_EQ((hashid_t)_hashFuncPtr->getHashId("user2"),
                         clusterPartitions[1].first);
    ASSERT_EQ((hashid_t)_hashFuncPtr->getHashId("user2"),
                         clusterPartitions[1].second);

    clusterName = "cluster3.default";
    ret = clusterClause->getClusterPartIds(clusterName, clusterPartitions);
    ASSERT_TRUE(ret);
    ASSERT_EQ((size_t)0, clusterPartitions.size());

    clusterName = "otherClusterName";
    ret = clusterClause->getClusterPartIds(clusterName, clusterPartitions);
    ASSERT_TRUE(!ret);
}

TEST_F(RequestParserTest, testParseClusterClauseInvalidParameter) {
    HA3_LOG(DEBUG, "Begin test");

    RequestPtr requestPtr(new Request);
    string oriStr = "cluster=cluster1:part_ids=65536,"
                    "cluster2:hash_field=user0|user2,"
                    "cluster3";

    requestPtr->setOriginalString(oriStr);

    ClusterClause *clusterClause = requestPtr->getClusterClause();
    ASSERT_TRUE(clusterClause);

    bool ret = _requestParser->parseClusterClause(requestPtr,
            _clusterConfigMapPtr);
    ASSERT_TRUE(!ret)<<clusterClause->getOriginalString();
    ASSERT_TRUE(!ret);
}

TEST_F(RequestParserTest, testParseClusterClauseWithPartitionModeForInvalidPartId1) {
    RequestPtr requestPtr(new Request);
    string oriStr = "cluster=cluster1:part_ids=-3";

    requestPtr->setOriginalString(oriStr);

    ClusterClause *clusterClause = requestPtr->getClusterClause();
    ASSERT_TRUE(clusterClause);
    ASSERT_TRUE(!_requestParser->parseClusterClause(requestPtr,
                    _clusterConfigMapPtr));

    ErrorResult errorResult = _requestParser->getErrorResult();
    ASSERT_EQ(ERROR_PART_MODE_INVALID_PARTID,
                         errorResult.getErrorCode());

    ASSERT_EQ(string("Error partition id string. oriString[-3]"),
                         errorResult.getErrorMsg());

}

TEST_F(RequestParserTest, testParseClusterClauseWithPartitionModeForInvalidPartId2) {
    RequestPtr requestPtr(new Request);
    string oriStr = "cluster=cluster1:part_ids=99999";

    requestPtr->setOriginalString(oriStr);

    ClusterClause *configClause = requestPtr->getClusterClause();
    ASSERT_TRUE(configClause);
    ASSERT_TRUE(!_requestParser->parseClusterClause(requestPtr,
                    _clusterConfigMapPtr));

    ErrorResult errorResult = _requestParser->getErrorResult();
    ASSERT_EQ(ERROR_PART_MODE_INVALID_PARTID,
                         errorResult.getErrorCode());

    ASSERT_EQ(string("Error partition id string. oriString[99999]"),
                         errorResult.getErrorMsg());
}

TEST_F(RequestParserTest, testParseClusterClauseWithPartitionModeForInvalidPartId3) {
    RequestPtr requestPtr(new Request);
    string oriStr = "cluster=cluster1:part_ids=";

    requestPtr->setOriginalString(oriStr);

    ClusterClause *configClause = requestPtr->getClusterClause();
    ASSERT_TRUE(configClause);
    ASSERT_TRUE(!_requestParser->parseClusterClause(requestPtr,
                    _clusterConfigMapPtr));

    ErrorResult errorResult = _requestParser->getErrorResult();
    ASSERT_EQ(ERROR_CLUSTER_PART_MODE_CONFIG,
                         errorResult.getErrorCode());

    ASSERT_EQ(string("Error partition mode config. oriString[part_ids=]"),
                         errorResult.getErrorMsg());
}

TEST_F(RequestParserTest, testParseClusterClauseWithInvalidKey_part_ids) {
    RequestPtr requestPtr(new Request);
    string oriStr = "cluster=cluster1:invalid_part_mode=6";

    requestPtr->setOriginalString(oriStr);

    ClusterClause *configClause = requestPtr->getClusterClause();
    ASSERT_TRUE(configClause);
    ASSERT_TRUE(!_requestParser->parseClusterClause(requestPtr,
                    _clusterConfigMapPtr));

    ErrorResult errorResult = _requestParser->getErrorResult();
    ASSERT_EQ(ERROR_CLUSTER_PART_MODE_NAME,
                         errorResult.getErrorCode());

    ASSERT_EQ(string("Error partition mode name. oriString[invalid_part_mode]"),
                         errorResult.getErrorMsg());
}

TEST_F(RequestParserTest, testParseClusterClauseWithEmpty) {
    RequestPtr requestPtr(new Request);
    string oriStr = "cluster=";

    requestPtr->setOriginalString(oriStr);

    ClusterClause *configClause = requestPtr->getClusterClause();
    ASSERT_TRUE(configClause);
    ASSERT_TRUE(!_requestParser->parseClusterClause(requestPtr,
                    _clusterConfigMapPtr));

    ErrorResult errorResult = _requestParser->getErrorResult();
    ASSERT_EQ(ERROR_CLUSTER_CLAUSE,
                         errorResult.getErrorCode());

    ASSERT_EQ(string("Error cluster clause. oriString[]"),
                         errorResult.getErrorMsg());
}

TEST_F(RequestParserTest, testParseClusterClauseWithoutClusterName) {
    RequestPtr requestPtr(new Request);
    string oriStr = "cluster=part_ids=0";

    requestPtr->setOriginalString(oriStr);

    ClusterClause *configClause = requestPtr->getClusterClause();
    ASSERT_TRUE(configClause);
    bool ret = _requestParser->parseClusterClause(requestPtr,
            _clusterConfigMapPtr);
    ASSERT_TRUE(!ret);

    ErrorResult errorResult = _requestParser->getErrorResult();
    ASSERT_EQ(ERROR_CLUSTER_NAME_NOT_EXIST_IN_CLUSTER_CLAUSE,
                         errorResult.getErrorCode());

    ASSERT_EQ(string("ClusterName:part_ids=0"),
                         errorResult.getErrorMsg());
}

TEST_F(RequestParserTest, testParseClusterClauseWithoutClusterName2) {
    RequestPtr requestPtr(new Request);
    string oriStr = "cluster=:part_ids=0";

    requestPtr->setOriginalString(oriStr);

    ClusterClause *configClause = requestPtr->getClusterClause();
    ASSERT_TRUE(configClause);
    bool ret = _requestParser->parseClusterClause(requestPtr,
            _clusterConfigMapPtr);
    ASSERT_TRUE(!ret);

    ErrorResult errorResult = _requestParser->getErrorResult();
    ASSERT_EQ(ERROR_CLUSTER_NAME_NOT_EXIST_IN_CLUSTER_CLAUSE,
                         errorResult.getErrorCode());

    ASSERT_EQ(string("ClusterName:part_ids=0"),
                         errorResult.getErrorMsg());
}

TEST_F(RequestParserTest, testParseClusterClauseWithoutClusterName3) {
    RequestPtr requestPtr(new Request);
    string oriStr = "cluster=:";

    requestPtr->setOriginalString(oriStr);

    ClusterClause *configClause = requestPtr->getClusterClause();
    ASSERT_TRUE(configClause);
    bool ret = _requestParser->parseClusterClause(requestPtr,
            _clusterConfigMapPtr);
    ASSERT_TRUE(!ret);

    ErrorResult errorResult = _requestParser->getErrorResult();
    ASSERT_EQ(ERROR_CLUSTER_CLAUSE,
                         errorResult.getErrorCode());

    ASSERT_EQ(string("Error cluster clause. oriString[:]"),
                         errorResult.getErrorMsg());
}

TEST_F(RequestParserTest, testParseClusterClauseWithErrorClusterName) {
    RequestPtr requestPtr(new Request);
    string oriStr = "cluster=error_cluster_name:part_ids=0";

    requestPtr->setOriginalString(oriStr);

    ClusterClause *configClause = requestPtr->getClusterClause();
    ASSERT_TRUE(configClause);
    ASSERT_TRUE(!_requestParser->parseClusterClause(requestPtr,
                    _clusterConfigMapPtr));

    ErrorResult errorResult = _requestParser->getErrorResult();
    ASSERT_EQ(ERROR_CLUSTER_NAME_NOT_EXIST_IN_CLUSTER_CLAUSE,
                         errorResult.getErrorCode());

    ASSERT_EQ(string("ClusterName:error_cluster_name"),
                         errorResult.getErrorMsg());
}

TEST_F(RequestParserTest, testParseClusterClauseWithErrorCluster) {
    RequestPtr requestPtr(new Request);
    string oriStr = "cluster=cluster1::part_ids=0:hash_field=user1";

    requestPtr->setOriginalString(oriStr);

    ClusterClause *configClause = requestPtr->getClusterClause();
    ASSERT_TRUE(configClause);
    ASSERT_TRUE(!_requestParser->parseClusterClause(requestPtr,
                    _clusterConfigMapPtr));

    ErrorResult errorResult = _requestParser->getErrorResult();
    ASSERT_EQ(ERROR_CLUSTER_CLAUSE,
                         errorResult.getErrorCode());

    string errorMsg = "Error cluster clause. oriString[cluster1::part_ids=0:hash_field=user1]";
    ASSERT_EQ(errorMsg, errorResult.getErrorMsg());
}

TEST_F(RequestParserTest, testParseClusterClauseWithTwoClusterWithSameName) {
    RequestPtr requestPtr(new Request);
    string oriStr = "cluster=cluster1:part_ids=0|1,cluster1:hash_field=user97";

    requestPtr->setOriginalString(oriStr);

    ClusterClause *clusterClause = requestPtr->getClusterClause();
    ASSERT_TRUE(clusterClause);
    ASSERT_TRUE(_requestParser->parseClusterClause(requestPtr,
                    _clusterConfigMapPtr));

    vector<pair<hashid_t, hashid_t> > partIds;
    bool ret = clusterClause->getClusterPartIds("cluster1.default", partIds);
    ASSERT_TRUE(ret);
    ASSERT_EQ((size_t)1, partIds.size());
    ASSERT_EQ((hashid_t)_hashFuncPtr->getHashId("user97"), partIds[0].first);
    ASSERT_EQ((hashid_t)_hashFuncPtr->getHashId("user97"), partIds[0].second);
}

TEST_F(RequestParserTest, testParseRequestWithBothConfigAndClusterClause1) {
    HA3_LOG(DEBUG, "Begin Test");
    RequestPtr requestPtr(new Request);
    string oriStr = "config=start:50,hit:10,format:xml&&cluster=cluster1";
    requestPtr->setOriginalString(oriStr);
    ASSERT_TRUE(_requestParser->parseRequest(requestPtr, _clusterConfigMapPtr));
    ConfigClause *configClause = requestPtr->getConfigClause();
    ASSERT_TRUE(configClause);
    ASSERT_EQ(string("cluster1.default"), configClause->getClusterName());
}

TEST_F(RequestParserTest, testParseRequestWithBothConfigAndClusterClause2) {
    HA3_LOG(DEBUG, "Begin Test");
    RequestPtr requestPtr(new Request);
    string oriStr = "config=cluster:cluster1,start:50,hit:10,format:xml&&cluster=cluster2,cluster3";
    requestPtr->setOriginalString(oriStr);
    ASSERT_TRUE(_requestParser->parseRequest(requestPtr, _clusterConfigMapPtr));
    ConfigClause *configClause = requestPtr->getConfigClause();
    ASSERT_TRUE(configClause);
    ASSERT_EQ((size_t)2, configClause->getClusterNameCount());
    ASSERT_EQ(string("cluster2.default"), configClause->getClusterName(0));
    ASSERT_EQ(string("cluster3.default"), configClause->getClusterName(1));
}

TEST_F(RequestParserTest, testParseRequestWithBothConfigAndClusterClause3) {
    HA3_LOG(DEBUG, "Begin Test");
    RequestPtr requestPtr(new Request);
    string oriStr = "config=start:50,hit:10,format:xml";
    requestPtr->setOriginalString(oriStr);
    ASSERT_TRUE(!_requestParser->parseRequest(requestPtr, _clusterConfigMapPtr));
}

TEST_F(RequestParserTest, testParseTargePartitions) {
    {//first token not equal 2
        RequestParser parser;
        ClusterConfigInfo configInfo;        
        string partStr= "aa=bb=cc";
        vector<pair<hashid_t, hashid_t> > partIds;
        ASSERT_FALSE(parser.parseTargetPartitions(configInfo, partStr, partIds));
        ASSERT_TRUE(parser._errorResult.hasError());        
        ASSERT_EQ(ERROR_CLUSTER_PART_MODE_CONFIG, parser._errorResult.getErrorCode());
    }
    {//first token not equal 2
        RequestParser parser;
        ClusterConfigInfo configInfo;        
        string partStr= "aa=";
        vector<pair<hashid_t, hashid_t> > partIds;
        ASSERT_FALSE(parser.parseTargetPartitions(configInfo, partStr, partIds));
        ASSERT_TRUE(parser._errorResult.hasError());        
        ASSERT_EQ(ERROR_CLUSTER_PART_MODE_CONFIG, parser._errorResult.getErrorCode());
    }
    {//hash func is empty
        RequestParser parser;
        ClusterConfigInfo configInfo;        
        string partStr= "part_ids=1";
        vector<pair<hashid_t, hashid_t> > partIds;
        ASSERT_FALSE(parser.parseTargetPartitions(configInfo, partStr, partIds));
        ASSERT_TRUE(parser._errorResult.hasError());        
        ASSERT_EQ(ERROR_HASH_FUNCTION, parser._errorResult.getErrorCode());
    }
    {// mode is error
        RequestParser parser;
        ClusterConfigInfo configInfo;        
        configInfo._hashMode._hashFunction = "HASH";
        configInfo._hashMode._hashFields.push_back("aa");
        ASSERT_TRUE(configInfo.initHashFunc());
        string partStr= "part_idsxxx=1";
        vector<pair<hashid_t, hashid_t> > partIds;
        ASSERT_FALSE(parser.parseTargetPartitions(configInfo, partStr, partIds));
        ASSERT_TRUE(parser._errorResult.hasError());        
        ASSERT_EQ(ERROR_CLUSTER_PART_MODE_NAME, parser._errorResult.getErrorCode());
    }
    {// parse part id failed
        RequestParser parser;
        ClusterConfigInfo configInfo;        
        configInfo._hashMode._hashFunction = "HASH";
        configInfo._hashMode._hashFields.push_back("aa");
        ASSERT_TRUE(configInfo.initHashFunc());
        string partStr= "part_ids=abc";
        vector<pair<hashid_t, hashid_t> > partIds;
        ASSERT_FALSE(parser.parseTargetPartitions(configInfo, partStr, partIds));
        ASSERT_TRUE(parser._errorResult.hasError());        
        ASSERT_EQ(ERROR_PART_MODE_INVALID_PARTID, parser._errorResult.getErrorCode());
    }
    {// parse hash field
        RequestParser parser;
        ClusterConfigInfo configInfo;        
        configInfo._hashMode._hashFunction = "NUMBER_HASH";
        configInfo._hashMode._hashFields.push_back("aa");
        ASSERT_TRUE(configInfo.initHashFunc());
        string partStr= "hash_field=123|65537";
        vector<pair<hashid_t, hashid_t> > partIds;
        ASSERT_TRUE(parser.parseTargetPartitions(configInfo, partStr, partIds));
        ASSERT_FALSE(parser._errorResult.hasError());        
        ASSERT_EQ(ERROR_NONE, parser._errorResult.getErrorCode());
        ASSERT_EQ(2, partIds.size());
        ASSERT_EQ(123, partIds[0].first);
        ASSERT_EQ(123, partIds[0].second);
        ASSERT_EQ(1, partIds[1].first);
        ASSERT_EQ(1, partIds[1].second);
    }
}

TEST_F(RequestParserTest, testParseHashField) {
    
    auto hashFunc = HashFuncFactory::createHashFunc("NUMBER_HASH");
    ASSERT_TRUE(hashFunc != nullptr);
    {
        RequestParser parser;
        vector<string> tokens = {"123", "65536"};
        vector<pair<hashid_t, hashid_t> > partIds;
        ASSERT_TRUE(parser.parseHashField(tokens, hashFunc, partIds));
        ASSERT_EQ(2, partIds.size());
        ASSERT_EQ(123, partIds[0].first);
        ASSERT_EQ(123, partIds[0].second);
        ASSERT_EQ(0, partIds[1].first);
        ASSERT_EQ(0, partIds[1].second);
        ASSERT_TRUE(!parser._errorResult.hasError());
    }
    {
        RequestParser parser;
        vector<string> tokens;
        vector<pair<hashid_t, hashid_t> > partIds;
        ASSERT_FALSE(parser.parseHashField(tokens, hashFunc, partIds));
        ASSERT_EQ(0, partIds.size());
        ASSERT_TRUE(parser._errorResult.hasError());
    }
    {// invalid hash field
        RequestParser parser;
        vector<string> tokens = {"123#0.2#xxx", "65536#0.2"};
        vector<pair<hashid_t, hashid_t> > partIds;
        ASSERT_FALSE(parser.parseHashField(tokens, hashFunc, partIds));
    }
    {// normal
        RequestParser parser;
        vector<string> tokens = {"100#0.5"};
        vector<pair<hashid_t, hashid_t> > partIds;
        ASSERT_TRUE(parser.parseHashField(tokens, hashFunc, partIds));
        ASSERT_EQ(1, partIds.size());
        ASSERT_EQ(100, partIds[0].first);
        ASSERT_EQ(100, partIds[0].second);
        ASSERT_TRUE(!parser._errorResult.hasError());
    }
}

TEST_F(RequestParserTest, testParseHashFieldWithRoutingHash) {
    map<string, string> param = {{"routing_ratio","0.1"}, {"hash_func", "NUMBER_HASH"}};
    auto hashFunc = HashFuncFactory::createHashFunc("ROUTING_HASH", param);
    ASSERT_TRUE(hashFunc != nullptr);
    {
        RequestParser parser;
        vector<string> tokens = {"123", "65536"};
        vector<pair<hashid_t, hashid_t> > partIds;
        ASSERT_TRUE(parser.parseHashField(tokens, hashFunc, partIds));
        ASSERT_EQ(2, partIds.size());
        ASSERT_EQ(123, partIds[0].first);
        ASSERT_EQ(6676, partIds[0].second);
        ASSERT_EQ(0, partIds[1].first);
        ASSERT_EQ(6553, partIds[1].second);
        ASSERT_TRUE(!parser._errorResult.hasError());
    }
    {
        RequestParser parser;
        vector<string> tokens;
        vector<pair<hashid_t, hashid_t> > partIds;
        ASSERT_FALSE(parser.parseHashField(tokens, hashFunc, partIds));
        ASSERT_EQ(0, partIds.size());
        ASSERT_TRUE(parser._errorResult.hasError());
    }
    {// invalid hash field
        RequestParser parser;
        vector<string> tokens = {"123#0.2#xxx", "65536#0.2"};
        vector<pair<hashid_t, hashid_t> > partIds;
        ASSERT_FALSE(parser.parseHashField(tokens, hashFunc, partIds));
    }
    {// normal
        RequestParser parser;
        vector<string> tokens = {"100#0.5"};
        vector<pair<hashid_t, hashid_t> > partIds;
        ASSERT_TRUE(parser.parseHashField(tokens, hashFunc, partIds));
        ASSERT_EQ(1, partIds.size());
        ASSERT_EQ(100, partIds[0].first);
        ASSERT_EQ(32867, partIds[0].second);
        ASSERT_TRUE(!parser._errorResult.hasError());
    }
}

TEST_F(RequestParserTest, testParsePartitionIds) {
    auto hashFunc = HashFuncFactory::createHashFunc("NUMBER_HASH");
    ASSERT_TRUE(hashFunc != nullptr);
    { // empty token
        RequestParser parser;
        vector<string> tokens;
        vector<pair<hashid_t, hashid_t> > partIds;
        ASSERT_FALSE(parser.parsePartitionIds(tokens, hashFunc, partIds));
        ASSERT_EQ(0, partIds.size());
        ASSERT_TRUE(parser._errorResult.hasError());
    }
    { // invalid token
        RequestParser parser;
        vector<string> tokens = {"abc"};
        vector<pair<hashid_t, hashid_t> > partIds;
        ASSERT_FALSE(parser.parsePartitionIds(tokens, hashFunc, partIds));
        ASSERT_EQ(0, partIds.size());
        ASSERT_TRUE(parser._errorResult.hasError());
    }
    { // invalid token
        RequestParser parser;
        vector<string> tokens = {"65537"};
        vector<pair<hashid_t, hashid_t> > partIds;
        ASSERT_FALSE(parser.parsePartitionIds(tokens, hashFunc, partIds));
        ASSERT_EQ(0, partIds.size());
        ASSERT_TRUE(parser._errorResult.hasError());
    }
    { // normal
        RequestParser parser;
        vector<string> tokens = {"1", "20"};
        vector<pair<hashid_t, hashid_t> > partIds;
        ASSERT_TRUE(parser.parsePartitionIds(tokens, hashFunc, partIds));
        ASSERT_TRUE(!parser._errorResult.hasError());
        ASSERT_EQ(2, partIds.size());
        ASSERT_EQ(1, partIds[0].first);
        ASSERT_EQ(1, partIds[0].second);
        ASSERT_EQ(20, partIds[1].first);
        ASSERT_EQ(20, partIds[1].second);
    }
    { // invalid dynamic ratio
        RequestParser parser;
        vector<string> tokens = {"1#0.5#xxx"};
        vector<pair<hashid_t, hashid_t> > partIds;
        ASSERT_FALSE(parser.parsePartitionIds(tokens, hashFunc, partIds));
        ASSERT_TRUE(parser._errorResult.hasError());
    }
    { // normal dynamic ratio
        RequestParser parser;
        vector<string> tokens = {"1#0.5"};
        vector<pair<hashid_t, hashid_t> > partIds;
        ASSERT_TRUE(parser.parsePartitionIds(tokens, hashFunc, partIds));
        ASSERT_TRUE(!parser._errorResult.hasError());
        ASSERT_EQ(1, partIds.size());
        ASSERT_EQ(1, partIds[0].first);
        ASSERT_EQ(1, partIds[0].second);
    }
}

TEST_F(RequestParserTest, testParsePartitionIdsWithRoutingHash) {
    map<string, string> param = {{"routing_ratio", "0.1"}, {"hash_func", "NUMBER_HASH"}};
    auto hashFunc = HashFuncFactory::createHashFunc("ROUTING_HASH", param);
    ASSERT_TRUE(hashFunc != nullptr);
    { // empty token
        RequestParser parser;
        vector<string> tokens;
        vector<pair<hashid_t, hashid_t> > partIds;
        ASSERT_FALSE(parser.parsePartitionIds(tokens, hashFunc, partIds));
        ASSERT_EQ(0, partIds.size());
        ASSERT_TRUE(parser._errorResult.hasError());
    }
    { // normal
        RequestParser parser;
        vector<string> tokens = {"1", "20"};
        vector<pair<hashid_t, hashid_t> > partIds;
        ASSERT_TRUE(parser.parsePartitionIds(tokens, hashFunc, partIds));
        ASSERT_TRUE(!parser._errorResult.hasError());
        ASSERT_EQ(2, partIds.size());
        ASSERT_EQ(1, partIds[0].first);
        ASSERT_EQ(6554, partIds[0].second);
        ASSERT_EQ(20, partIds[1].first);
        ASSERT_EQ(6573, partIds[1].second);
    }
    { // invalid dynamic ratio
        RequestParser parser;
        vector<string> tokens = {"1#0.5#xxx"};
        vector<pair<hashid_t, hashid_t> > partIds;
        ASSERT_FALSE(parser.parsePartitionIds(tokens, hashFunc, partIds));
        ASSERT_TRUE(parser._errorResult.hasError());
    }
    { // normal dynamic ratio
        RequestParser parser;
        vector<string> tokens = {"1#0.5"};
        vector<pair<hashid_t, hashid_t> > partIds;
        ASSERT_TRUE(parser.parsePartitionIds(tokens, hashFunc, partIds));
        ASSERT_TRUE(!parser._errorResult.hasError());
        ASSERT_EQ(1, partIds.size());
        ASSERT_EQ(1, partIds[0].first);
        ASSERT_EQ(32768, partIds[0].second);
    }
}

TEST_F(RequestParserTest, testParseRequestWithBothConfigAndClusterClause4) {
    HA3_LOG(DEBUG, "Begin Test");
    RequestPtr requestPtr(new Request);
    string oriStr = "config=start:50,hit:10,format:xml&&cluster=,,,";
    requestPtr->setOriginalString(oriStr);
    ASSERT_TRUE(!_requestParser->parseRequest(requestPtr, _clusterConfigMapPtr));
}

TEST_F(RequestParserTest, testParseRequestWithoutConfigClause) {
    HA3_LOG(DEBUG, "Begin Test");
    RequestPtr requestPtr(new Request);
    string oriStr = "cluster=cluster1";
    requestPtr->setOriginalString(oriStr);
    bool ret = _requestParser->parseRequest(requestPtr, _clusterConfigMapPtr);
    ErrorResult errorResult = _requestParser->getErrorResult();
    ASSERT_EQ(ERROR_NONE, errorResult.getErrorCode());
    ASSERT_TRUE(ret);

    ConfigClause* configClause = requestPtr->getConfigClause();
    ASSERT_TRUE(configClause);
    ASSERT_EQ(string("cluster1.default"), configClause->getClusterName());
    ASSERT_EQ((uint32_t)0, configClause->getStartOffset());
    ASSERT_EQ((uint32_t)10, configClause->getHitCount());
    ASSERT_EQ(string("xml"), configClause->getResultFormatSetting());
    ASSERT_EQ((uint32_t)SEARCH_PHASE_ONE, configClause->getPhaseNumber());
    ASSERT_EQ(string(DEFAULT_QRS_CHAIN), configClause->getQrsChainName());
    ASSERT_EQ(string(DEFAULT_SUMMARY_PROFILE), configClause->getSummaryProfileName());
    ASSERT_TRUE(configClause->useCache());
    ASSERT_EQ(uint32_t(0), configClause->getRpcTimeOut());
    ASSERT_TRUE(!configClause->getDisplayCacheInfo());

    ClusterClause* clusterClause = requestPtr->getClusterClause();
    ASSERT_TRUE(clusterClause);
}

TEST_F(RequestParserTest, testForBug74101) {
    HA3_LOG(DEBUG, "Begin Test");
    RequestPtr requestPtr(new Request);
    string oriStr = "query=\"商务笔记本\"&&cluster=cluster1";
    requestPtr->setOriginalString(oriStr);
    bool ret = _requestParser->parseConfigClause(requestPtr);
    ErrorResult errorResult = _requestParser->getErrorResult();
    ASSERT_EQ(ERROR_NONE, errorResult.getErrorCode());
    ASSERT_TRUE(ret);

    ConfigClause* configClause = requestPtr->getConfigClause();
    ASSERT_TRUE(configClause);
    ASSERT_EQ(string(""), configClause->getClusterName());
    ASSERT_EQ((uint32_t)0, configClause->getStartOffset());
    ASSERT_EQ((uint32_t)10, configClause->getHitCount());
    ASSERT_EQ(string("xml"), configClause->getResultFormatSetting());
    ASSERT_EQ((uint32_t)SEARCH_PHASE_ONE, configClause->getPhaseNumber());
    ASSERT_EQ(string(DEFAULT_QRS_CHAIN), configClause->getQrsChainName());
    ASSERT_EQ(string(DEFAULT_SUMMARY_PROFILE), configClause->getSummaryProfileName());
    ASSERT_TRUE(configClause->useCache());
    ASSERT_EQ(uint32_t(0), configClause->getRpcTimeOut());
    ASSERT_TRUE(!configClause->getDisplayCacheInfo());

    ClusterClause* clusterClause = requestPtr->getClusterClause();
    ASSERT_TRUE(clusterClause);
}

TEST_F(RequestParserTest, testParseVirtualAttributeClause) {
    HA3_LOG(DEBUG, "Begin Test");
    RequestPtr requestPtr(new Request);
    string oriStr = "config=cluster:cluster1&&virtual_attribute=va1:a1;va2:a2";
    requestPtr->setOriginalString(oriStr);
    ASSERT_TRUE(_requestParser->parseRequest(requestPtr, _clusterConfigMapPtr));

    VirtualAttributeClause *vaClause = requestPtr->getVirtualAttributeClause();
    ASSERT_TRUE(vaClause);
    ASSERT_EQ((size_t)2, vaClause->getVirtualAttributeSize());
    const vector<VirtualAttribute*>& vas = vaClause->getVirtualAttributes();
    ASSERT_EQ(string("va1"), vas[0]->getVirtualAttributeName());
    ASSERT_EQ(string("va2"), vas[1]->getVirtualAttributeName());
}

TEST_F(RequestParserTest, testParseVirtualAttributeClauseWithEmpty) {
    HA3_LOG(DEBUG, "Begin Test");
    RequestPtr requestPtr(new Request);
    string oriStr = "config=cluster:cluster1&&virtual_attribute=";
    requestPtr->setOriginalString(oriStr);
    ASSERT_TRUE(!_requestParser->parseRequest(requestPtr,
                    _clusterConfigMapPtr));
}

TEST_F(RequestParserTest, testParseAttributeClause) {
    HA3_LOG(DEBUG, "Begin Test");
    RequestPtr requestPtr(new Request);
    string oriStr = "config=cluster:cluster1&&attribute=attr1,attr2";
    requestPtr->setOriginalString(oriStr);
    ASSERT_TRUE(_requestParser->parseRequest(requestPtr, _clusterConfigMapPtr));

    AttributeClause *attributeClause = requestPtr->getAttributeClause();
    ASSERT_TRUE(attributeClause);
    set<string> attributeNames = attributeClause->getAttributeNames();
    ASSERT_TRUE(attributeNames.size() == 2);
    ASSERT_TRUE(attributeNames.find("attr1") != attributeNames.end());
    ASSERT_TRUE(attributeNames.find("attr2") != attributeNames.end());
}

TEST_F(RequestParserTest, testParseAttributeClauseWithEmpty) {
    HA3_LOG(DEBUG, "Begin Test");
    RequestPtr requestPtr(new Request);
    string oriStr = "config=cluster:cluster1&&attribute=";
    requestPtr->setOriginalString(oriStr);
    ASSERT_TRUE(_requestParser->parseRequest(requestPtr, _clusterConfigMapPtr));

    AttributeClause *attributeClause = requestPtr->getAttributeClause();
    ASSERT_TRUE(attributeClause);
    set<string> attributeNames = attributeClause->getAttributeNames();
    ASSERT_TRUE(attributeNames.size() == 0);
}

TEST_F(RequestParserTest, testParseHealthCheckClause) {
    HA3_LOG(DEBUG, "Begin Test");
    RequestPtr requestPtr(new Request);
    string oriStr = "config=cluster:cluster1&&healthcheck=check:true,check_times:3,recover_time:32";
    requestPtr->setOriginalString(oriStr);
    ASSERT_TRUE(_requestParser->parseRequest(requestPtr, _clusterConfigMapPtr));

    HealthCheckClause *healthCheckClause = requestPtr->getHealthCheckClause();
    ASSERT_TRUE(healthCheckClause);
    ASSERT_TRUE(healthCheckClause->isCheck());
    ASSERT_EQ((int32_t)3, healthCheckClause->getCheckTimes());
    ASSERT_EQ((int64_t)32000000, healthCheckClause->getRecoverTime());


    oriStr = "config=cluster:cluster1&&healthcheck=check:false,check_times:3";
    requestPtr->setOriginalString(oriStr);
    ASSERT_TRUE(_requestParser->parseRequest(requestPtr, _clusterConfigMapPtr));

    healthCheckClause = requestPtr->getHealthCheckClause();
    ASSERT_TRUE(healthCheckClause);
    ASSERT_TRUE(!healthCheckClause->isCheck());
    ASSERT_EQ((int32_t)3, healthCheckClause->getCheckTimes());

    oriStr = "config=cluster:cluster1&&healthcheck=check:TRUE,check_times:3";
    requestPtr->setOriginalString(oriStr);
    ASSERT_TRUE(_requestParser->parseRequest(requestPtr, _clusterConfigMapPtr));

    healthCheckClause = requestPtr->getHealthCheckClause();
    ASSERT_TRUE(healthCheckClause);
    ASSERT_TRUE(healthCheckClause->isCheck());
}

TEST_F(RequestParserTest, testParseHealthCheckClauseForFailed) {
    HA3_LOG(DEBUG, "Begin Test");
    RequestPtr requestPtr(new Request);
    string oriStr = "config=cluster:cluster1&&healthcheck1=check:true,check_times:3";
    requestPtr->setOriginalString(oriStr);
    ASSERT_TRUE(!_requestParser->parseRequest(requestPtr, _clusterConfigMapPtr));

    oriStr = "config=cluster:cluster1&&healthCheck=check:true,check_times:3";
    requestPtr->setOriginalString(oriStr);
    ASSERT_TRUE(!_requestParser->parseRequest(requestPtr, _clusterConfigMapPtr));

    oriStr = "config=cluster:cluster1&&healthcheck=check2:true,check_times:3";
    requestPtr->setOriginalString(oriStr);
    ASSERT_TRUE(!_requestParser->parseRequest(requestPtr, _clusterConfigMapPtr));
    ASSERT_EQ(ERROR_HEALTHCHECK_CLAUSE, _requestParser->getErrorResult().getErrorCode());

    oriStr = "config=cluster:cluster1&&healthcheck=check:33,check_times:3";
    requestPtr->setOriginalString(oriStr);
    ASSERT_TRUE(!_requestParser->parseRequest(requestPtr, _clusterConfigMapPtr));
    ASSERT_EQ(ERROR_HEALTHCHECK_CHECK_FLAG, _requestParser->getErrorResult().getErrorCode());

    oriStr = "config=cluster:cluster1&&healthcheck=check:true,checktimes:3";
    requestPtr->setOriginalString(oriStr);
    ASSERT_TRUE(!_requestParser->parseRequest(requestPtr, _clusterConfigMapPtr));
    ASSERT_EQ(ERROR_HEALTHCHECK_CLAUSE, _requestParser->getErrorResult().getErrorCode());

    oriStr = "config=cluster:cluster1&&healthcheck=check:true,check_times:-1";
    requestPtr->setOriginalString(oriStr);
    ASSERT_TRUE(!_requestParser->parseRequest(requestPtr, _clusterConfigMapPtr));
    ASSERT_EQ(ERROR_HEALTHCHECK_CHECK_TIMES, _requestParser->getErrorResult().getErrorCode());

    oriStr = "config=cluster:cluster1&&healthcheck=check:true,check_times:123a";
    requestPtr->setOriginalString(oriStr);
    ASSERT_TRUE(!_requestParser->parseRequest(requestPtr, _clusterConfigMapPtr));
    ASSERT_EQ(ERROR_HEALTHCHECK_CHECK_TIMES, _requestParser->getErrorResult().getErrorCode());

    oriStr = "config=cluster:cluster1&&healthcheck=check:true,recover_time:xxx";
    requestPtr->setOriginalString(oriStr);
    ASSERT_TRUE(!_requestParser->parseRequest(requestPtr, _clusterConfigMapPtr));
    ASSERT_EQ(ERROR_HEALTHCHECK_RECOVER_TIME, _requestParser->getErrorResult().getErrorCode());
}

TEST_F(RequestParserTest, testParseFetchSummaryClause) {
    HA3_LOG(DEBUG, "Begin Test");
    RequestPtr requestPtr(new Request);
    string oriStr = "config=cluster:cluster1&&fetch_summary="
                    "cluster_name_1_1_2_3_4, cluster_|1|2|3|6|0|7";
    requestPtr->setOriginalString(oriStr);

    FetchSummaryClause *fetchSummaryClause = requestPtr->getFetchSummaryClause();
    ASSERT_TRUE(fetchSummaryClause);

    string origFetchSummaryClauseStr = "cluster_name_1_1_2_3_4, cluster_|1|2|3|6|0|7";

    ASSERT_TRUE(_requestParser->parseRequest(requestPtr, _clusterConfigMapPtr));
    ASSERT_EQ(origFetchSummaryClauseStr,
                         fetchSummaryClause->getOriginalString());
    const vector<string> &names = fetchSummaryClause->getClusterNames();
    ASSERT_EQ((size_t)2, names.size());
    ASSERT_EQ(string("cluster_name_1.default"), names[0]);
    ASSERT_EQ(string("cluster_.default"), names[1]);

    const vector<GlobalIdentifier> &gids = fetchSummaryClause->getGids();
    ASSERT_EQ((size_t)2, gids.size());
    ASSERT_TRUE(GlobalIdentifier(4, 3, 2, 0, 0, 1, 0) == gids[0]);
    ASSERT_TRUE(GlobalIdentifier(6, 3, 2, 0, 0, 1, 7) == gids[1]);

    oriStr = "config=cluster:cluster1&&fetch_summary=";
    requestPtr->setOriginalString(oriStr);
    ASSERT_TRUE(!_requestParser->parseRequest(requestPtr, _clusterConfigMapPtr));
    ASSERT_EQ(ERROR_FETCH_SUMMARY_CLAUSE, _requestParser->getErrorResult().getErrorCode());
}

TEST_F(RequestParserTest, testParseResultCompressType) {
    HA3_LOG(DEBUG, "Begin Test");
    RequestPtr requestPtr(new Request);
    HaCompressType compressType;

#define TEST_PARSE_COMPRESS_TYPE(oriStr, parseResult, expectCompressType) {                                \
        requestPtr->setOriginalString(oriStr);                                                             \
        ASSERT_EQ(parseResult, _requestParser->parseRequest(requestPtr, _clusterConfigMapPtr)); \
        requestPtr->getConfigClause()->getCompressType(compressType);                                      \
        ASSERT_EQ(expectCompressType, compressType);                                            \
}
    string oriStr = "config=cluster:cluster1,result_compress:no_compress&&query=phrase:mp3";
    TEST_PARSE_COMPRESS_TYPE(oriStr, true, NO_COMPRESS);

    oriStr = "config=cluster:cluster1,result_compress:z_speed_compress&&query=phrase:mp3";
    TEST_PARSE_COMPRESS_TYPE(oriStr, true, Z_SPEED_COMPRESS);

    oriStr = "config=cluster:cluster1,result_compress:z_default_compress&&query=phrase:mp3";
    TEST_PARSE_COMPRESS_TYPE(oriStr, true, Z_DEFAULT_COMPRESS);

    oriStr = "config=cluster:cluster1,result_compress:z_unknown_compress&&query=phrase:mp3";
    TEST_PARSE_COMPRESS_TYPE(oriStr, true, Z_DEFAULT_COMPRESS);

#undef TEST_PARSE_COMPRESS_TYPE
}

TEST_F(RequestParserTest, testParseGid) {
    HA3_LOG(DEBUG, "Begin Test!");
    checkParseGidResult("c1_2_3_4_5", true, "c1", 2, 3, 4, 5, (primarykey_t)0, 0);
    checkParseGidResult("c1__2_3_4_5", true, "c1_", 2, 3, 4, 5, (primarykey_t)0, 0);
    checkParseGidResult("__2_3_4_5", true, "_", 2, 3, 4, 5, (primarykey_t)0, 0);

    checkParseGidResult("c1|2|3|4|5|11|6", true, "c1", 2, 3, 4, 5, (primarykey_t)11, 6);
    checkParseGidResult("c1_|2|3|4|5|12|7", true, "c1_", 2, 3, 4, 5, (primarykey_t)12, 7);
    checkParseGidResult("_|2|3|4|5|7|8", true, "_", 2, 3, 4, 5, (primarykey_t)7, 8);
    checkParseGidResult("_c1|2|3|4|5|8|9", true, "_c1", 2, 3, 4, 5, (primarykey_t)8, 9);
    checkParseGidResult("c1_6|2|3|4|5|9|10", true, "c1_6", 2, 3, 4, 5, (primarykey_t)9, 10);
    checkParseGidResult("1|2|3|4|5|001f|11", true, "1", 2, 3, 4, 5, (primarykey_t)31, 11);

    checkParseGidResult("c1_2_3_4", false);
    checkParseGidResult("c1__a_3_4_5", false);

    checkParseGidResult("c1|2|3|4", false);
    checkParseGidResult("c1|2|3|4||3", false);
    checkParseGidResult("c1|2|3|xx|5|11|6", false);
    checkParseGidResult("c1|2|3|4|5|111111111111111111123123123123123123123123123|6", false); // pk too long, limit 32
}

void RequestParserTest::checkParseGidResult(
        const string& gidStr, bool expectParseRet,
        const string& expectClusterName, uint32_t expectFullInexVerison,
        int32_t expectIndexVersion, hashid_t expectHashId, docid_t expectDocId,
        primarykey_t expectPk, uint32_t expectIp)
{
    string clusterName;
    GlobalIdentifier gid;

    bool ret = _requestParser->parseGid(gidStr, clusterName, gid);
    ASSERT_TRUE(expectParseRet == ret);
    ASSERT_EQ(expectClusterName, clusterName);
    ASSERT_EQ(expectFullInexVerison, gid.getFullIndexVersion());
    ASSERT_EQ(expectIndexVersion, gid.getIndexVersion());
    ASSERT_EQ(expectHashId, gid.getHashId());
    ASSERT_EQ(expectDocId, gid.getDocId());
    ASSERT_EQ(expectIp, gid.getIp());
}

TEST_F(RequestParserTest, testParseConfigClauseFetchSummaryType) {
    RequestPtr requestPtr(new Request);

    {
        string oriStr = "cluster:cluster1,start:50,hit:10,fetch_summary_type:pk";
        requestPtr->setOriginalString("config=" + oriStr);
        ConfigClause *configClause = requestPtr->getConfigClause();
        ASSERT_TRUE(configClause);
        ASSERT_TRUE(_requestParser->parseConfigClause(requestPtr));
        ASSERT_EQ(oriStr, configClause->getOriginalString());
        ASSERT_EQ(string("cluster1.default"), configClause->getClusterName());
        ASSERT_EQ((uint32_t)50, configClause->getStartOffset());
        ASSERT_EQ((uint32_t)10, configClause->getHitCount());
        ASSERT_EQ((uint32_t)BY_PK, configClause->getFetchSummaryType());
    }

    {
        string oriStr = "cluster:cluster1,start:50,hit:10,fetch_summary_type:docid";
        requestPtr->setOriginalString("config=" + oriStr);
        ConfigClause *configClause = requestPtr->getConfigClause();
        ASSERT_TRUE(configClause);
        ASSERT_TRUE(_requestParser->parseConfigClause(requestPtr));
        ASSERT_EQ(oriStr, configClause->getOriginalString());
        ASSERT_EQ(string("cluster1.default"), configClause->getClusterName());
        ASSERT_EQ((uint32_t)50, configClause->getStartOffset());
        ASSERT_EQ((uint32_t)10, configClause->getHitCount());
        ASSERT_EQ((uint32_t)BY_DOCID, configClause->getFetchSummaryType());
    }

    {
        string oriStr = "cluster:cluster1,start:50,hit:10,fetch_summary_type:rawpk";
        requestPtr->setOriginalString("config=" + oriStr);
        ConfigClause *configClause = requestPtr->getConfigClause();
        ASSERT_TRUE(configClause);
        ASSERT_TRUE(_requestParser->parseConfigClause(requestPtr));
        ASSERT_EQ(oriStr, configClause->getOriginalString());
        ASSERT_EQ(string("cluster1.default"), configClause->getClusterName());
        ASSERT_EQ((uint32_t)50, configClause->getStartOffset());
        ASSERT_EQ((uint32_t)10, configClause->getHitCount());
        ASSERT_EQ((uint32_t)BY_RAWPK, configClause->getFetchSummaryType());
    }
    {
        string oriStr = "cluster:cluster1,start:50,hit:10,fetch_summary_type:xxx";
        requestPtr->setOriginalString("config=" + oriStr);
        ConfigClause *configClause = requestPtr->getConfigClause();
        ASSERT_TRUE(configClause);
        ASSERT_TRUE(!_requestParser->parseConfigClause(requestPtr));
    }

}

TEST_F(RequestParserTest, testParseConfigClauseFetchSummaryGroup) {
    RequestPtr requestPtr(new Request);

    string oriStr = "cluster:cluster1,start:50,hit:10,fetch_summary_group:common|tpp";
    requestPtr->setOriginalString("config=" + oriStr);
    ConfigClause *configClause = requestPtr->getConfigClause();
    ASSERT_TRUE(configClause);
    ASSERT_TRUE(_requestParser->parseConfigClause(requestPtr));
    ASSERT_EQ(oriStr, configClause->getOriginalString());
    ASSERT_EQ(string("cluster1.default"), configClause->getClusterName());
    ASSERT_EQ((uint32_t)50, configClause->getStartOffset());
    ASSERT_EQ((uint32_t)10, configClause->getHitCount());
    ASSERT_EQ("common|tpp", configClause->getFetchSummaryGroup());
}

TEST_F(RequestParserTest, testParseFetchSummaryClauseWithRawPk) {
    RequestPtr requestPtr(new Request);
    string configStr = "cluster=cluster1&&config=fetch_summary_type:rawpk&&";
    string fetchSummaryStr = "fetch_summary=daogou:rawpk1, rawpk2, rawpk2;"
                             "simple:\\&\\&, \\,\\,, \\;\\:\\\\, =\\=;";
    FetchSummaryClause *fetchSummaryClause = NULL;

    requestPtr->setOriginalString(configStr + fetchSummaryStr);
    ASSERT_TRUE(_requestParser->parseRequest(requestPtr, _clusterConfigMapPtr));
    fetchSummaryClause = requestPtr->getFetchSummaryClause();
    ASSERT_EQ((size_t)7, fetchSummaryClause->getRawPkCount());
    vector<string> rawPks = fetchSummaryClause->getRawPks();
    ASSERT_EQ((size_t)7, rawPks.size());
    ASSERT_EQ(string("rawpk1"), rawPks[0]);
    ASSERT_EQ(string("rawpk2"), rawPks[1]);
    ASSERT_EQ(string("rawpk2"), rawPks[2]);
    ASSERT_EQ(string("&&"), rawPks[3]);
    ASSERT_EQ(string(",,"), rawPks[4]);
    ASSERT_EQ(string(";:\\"), rawPks[5]);
    ASSERT_EQ(string("=="), rawPks[6]);
    vector<string> clusterNames = fetchSummaryClause->getClusterNames();
    ASSERT_EQ((size_t)7, clusterNames.size());
    for (size_t i = 0; i < clusterNames.size(); ++i) {
        ASSERT_EQ((i < 3) ? string("daogou.default") : string("simple.default"),
                             clusterNames[i]);
    }

    fetchSummaryStr = "fetch_summary=";
    requestPtr->setOriginalString(configStr + fetchSummaryStr);
    ASSERT_TRUE(!_requestParser->parseRequest(requestPtr, _clusterConfigMapPtr));

    fetchSummaryStr = "fetch_summary=daogou";
    requestPtr->setOriginalString(configStr + fetchSummaryStr);
    ASSERT_TRUE(!_requestParser->parseRequest(requestPtr, _clusterConfigMapPtr));

    fetchSummaryStr = "fetch_summary=;";
    requestPtr->setOriginalString(configStr + fetchSummaryStr);
    ASSERT_TRUE(!_requestParser->parseRequest(requestPtr, _clusterConfigMapPtr));

    fetchSummaryStr = "fetch_summary=\\";
    requestPtr->setOriginalString(configStr + fetchSummaryStr);
    ASSERT_TRUE(!_requestParser->parseRequest(requestPtr, _clusterConfigMapPtr));

    fetchSummaryStr = "fetch_summary=c:1:";
    requestPtr->setOriginalString(configStr + fetchSummaryStr);
    ASSERT_TRUE(!_requestParser->parseRequest(requestPtr, _clusterConfigMapPtr));

    fetchSummaryStr = "fetch_summary= :pk";
    requestPtr->setOriginalString(configStr + fetchSummaryStr);
    ASSERT_TRUE(!_requestParser->parseRequest(requestPtr, _clusterConfigMapPtr));

    fetchSummaryStr = "fetch_summary=c:pk,,";
    requestPtr->setOriginalString(configStr + fetchSummaryStr);
    ASSERT_TRUE(!_requestParser->parseRequest(requestPtr, _clusterConfigMapPtr));

    fetchSummaryStr = "fetch_summary=pk1,pk2";
    requestPtr->setOriginalString(configStr + fetchSummaryStr);
    ASSERT_TRUE(!_requestParser->parseRequest(requestPtr, _clusterConfigMapPtr));

    fetchSummaryStr = "fetch_summary=daogou:&&";
    requestPtr->setOriginalString(configStr + fetchSummaryStr);
    ASSERT_TRUE(!_requestParser->parseRequest(requestPtr, _clusterConfigMapPtr));

    fetchSummaryStr = "fetch_summary=daogou: ";
    requestPtr->setOriginalString(configStr + fetchSummaryStr);
    ASSERT_TRUE(!_requestParser->parseRequest(requestPtr, _clusterConfigMapPtr));

    fetchSummaryStr = "fetch_summary=c:1; :2";
    requestPtr->setOriginalString(configStr + fetchSummaryStr);
    ASSERT_TRUE(!_requestParser->parseRequest(requestPtr, _clusterConfigMapPtr));

    fetchSummaryStr = "fetch_summary=c:;";
    requestPtr->setOriginalString(configStr + fetchSummaryStr);
    ASSERT_TRUE(!_requestParser->parseRequest(requestPtr, _clusterConfigMapPtr));
}

TEST_F(RequestParserTest, testParseConfigClauseWithProtoFormatOption) {
    RequestPtr requestPtr(new Request);

    string oriStr = "cluster:cluster1,proto_format_option:unkown&unkown";
    requestPtr->setOriginalString("config=" + oriStr);
    ConfigClause *configClause = requestPtr->getConfigClause();
    ASSERT_TRUE(configClause);
    ASSERT_TRUE(!_requestParser->parseConfigClause(requestPtr));
    ASSERT_EQ(ERROR_CONFIG_CLAUSE_PROTO_FORMAT_OPTION,
                         _requestParser->getErrorResult().getErrorCode());

    oriStr = "cluster:cluster1,proto_format_option:summary_in_bytes";
    requestPtr->setOriginalString("config=" + oriStr);
    configClause = requestPtr->getConfigClause();
    ASSERT_TRUE(configClause);
    ASSERT_TRUE(_requestParser->parseConfigClause(requestPtr));
    uint32_t option = configClause->getProtoFormatOption();
    uint32_t expectedOption = PBResultFormatter::SUMMARY_IN_BYTES;
    ASSERT_EQ(option, expectedOption);
    ASSERT_TRUE(!configClause->hasPBMatchDocFormat());

    oriStr = "cluster:cluster1,proto_format_option:summary_in_bytes&summary_in_bytes";
    requestPtr->setOriginalString("config=" + oriStr);
    configClause = requestPtr->getConfigClause();
    ASSERT_TRUE(configClause);
    ASSERT_TRUE(_requestParser->parseConfigClause(requestPtr));
    option = configClause->getProtoFormatOption();
    expectedOption = PBResultFormatter::SUMMARY_IN_BYTES;
    ASSERT_EQ(option, expectedOption);
    ASSERT_TRUE(!configClause->hasPBMatchDocFormat());

    oriStr = "cluster:cluster1,proto_format_option:pb_matchdoc_format";
    requestPtr->setOriginalString("config=" + oriStr);
    configClause = requestPtr->getConfigClause();
    ASSERT_TRUE(configClause);
    ASSERT_TRUE(_requestParser->parseConfigClause(requestPtr));
    option = configClause->getProtoFormatOption();
    ASSERT_TRUE(!(option & PBResultFormatter::SUMMARY_IN_BYTES));
    ASSERT_TRUE(configClause->hasPBMatchDocFormat());

    oriStr = "cluster:cluster1,proto_format_option:"
             "pb_matchdoc_format&pb_matchdoc_format";
    requestPtr->setOriginalString("config=" + oriStr);
    configClause = requestPtr->getConfigClause();
    ASSERT_TRUE(configClause);
    ASSERT_TRUE(_requestParser->parseConfigClause(requestPtr));
    option = configClause->getProtoFormatOption();
    ASSERT_TRUE(!(option & PBResultFormatter::SUMMARY_IN_BYTES));
    ASSERT_TRUE(configClause->hasPBMatchDocFormat());

    oriStr = "cluster:cluster1,proto_format_option:"
             "pb_matchdoc_format&summary_in_bytes";
    requestPtr->setOriginalString("config=" + oriStr);
    configClause = requestPtr->getConfigClause();
    ASSERT_TRUE(configClause);
    ASSERT_TRUE(_requestParser->parseConfigClause(requestPtr));
    option = configClause->getProtoFormatOption();
    ASSERT_TRUE(option & PBResultFormatter::SUMMARY_IN_BYTES);
    ASSERT_TRUE(configClause->hasPBMatchDocFormat());

    oriStr = "cluster:cluster1,proto_format_option:pb_matchdoc_format&"
             "summary_in_bytes&pb_matchdoc_format";
    requestPtr->setOriginalString("config=" + oriStr);
    configClause = requestPtr->getConfigClause();
    ASSERT_TRUE(configClause);
    ASSERT_TRUE(_requestParser->parseConfigClause(requestPtr));
    option = configClause->getProtoFormatOption();
    ASSERT_TRUE(option & PBResultFormatter::SUMMARY_IN_BYTES);
    ASSERT_TRUE(configClause->hasPBMatchDocFormat());
}

TEST_F(RequestParserTest, testParseKVPairClause) {
    RequestPtr requestPtr(new Request);
    string oriStr = "config=cluster:cluster1&&kvpairs=key:value,key2:value2,key\\:aa\\,:value\\:bb\\,,";
    requestPtr->setOriginalString(oriStr);
    ConfigClause *configClause = requestPtr->getConfigClause();
    ASSERT_TRUE(configClause);
    ASSERT_TRUE(_requestParser->parseRequest(requestPtr, _clusterConfigMapPtr));
    ASSERT_EQ(string("cluster:cluster1"), configClause->getOriginalString());
    ASSERT_EQ(string("cluster1.default"), configClause->getClusterName());
    KVPairClause *kvPairClause = requestPtr->getKVPairClause();
    ASSERT_TRUE(kvPairClause);
    ASSERT_EQ(string("key:value,key2:value2,key\\:aa\\,:value\\:bb\\,,"), kvPairClause->getOriginalString());

    map<string, string> mp = configClause->getKVPairs();
    ASSERT_EQ(size_t(3), mp.size());
    ASSERT_EQ(string("value"), mp["key"]);
    ASSERT_EQ(string("value2"), mp["key2"]);
    ASSERT_EQ(string("value:bb,"), mp["key:aa,"]);
}

TEST_F(RequestParserTest, testKVPairInBothConfigClauseAndKVPairCluase) {
    RequestPtr requestPtr(new Request);
    string oriStr = "config=cluster:cluster1,kvpairs:key1#value1;key2#value2&&kvpairs=key:value ,:, key2 : value3";
    requestPtr->setOriginalString(oriStr);
    ConfigClause *configClause = requestPtr->getConfigClause();
    ASSERT_TRUE(configClause);
    ASSERT_TRUE(_requestParser->parseRequest(requestPtr, _clusterConfigMapPtr));
    ASSERT_EQ(string("cluster:cluster1,kvpairs:key1#value1;key2#value2"), configClause->getOriginalString());
    ASSERT_EQ(string("cluster1.default"), configClause->getClusterName());
    KVPairClause *kvPairClause = requestPtr->getKVPairClause();
    ASSERT_TRUE(kvPairClause);
    ASSERT_EQ(string("key:value ,:, key2 : value3"), kvPairClause->getOriginalString());

    map<string, string> mp = configClause->getKVPairs();
    ASSERT_EQ(size_t(4), mp.size());
    ASSERT_EQ(string("value"), mp["key"]);
    ASSERT_EQ(string("value1"), mp["key1"]);
    ASSERT_EQ(string("value3"), mp["key2"]);
    ASSERT_EQ(string(""), mp[""]);
}

TEST_F(RequestParserTest, testParseKVPairClauseFailed) {
    RequestPtr requestPtr(new Request);
    string oriStr = "config=cluster:cluster1&&kvpairs=key:value,key2:value3,invalid";
    requestPtr->setOriginalString(oriStr);
    ASSERT_TRUE(!_requestParser->parseRequest(requestPtr, _clusterConfigMapPtr));
    ASSERT_EQ(ERROR_PARSE_KVPAIR_CLAUSE, _requestParser->getErrorResult().getErrorCode());
}

TEST_F(RequestParserTest, testGetNextTerm) {

    string input = "abc\\:def:aa";
    size_t start = 0;
    string output = RequestParser::getNextTerm(input, ':', start);
    ASSERT_EQ(string("abc:def"), output);
    ASSERT_EQ(size_t(9), start);

    input = ":abc\\:def:aa";
    start = 0;
    output = RequestParser::getNextTerm(input, ':', start);
    ASSERT_EQ(string(""), output);
    ASSERT_EQ(size_t(1), start);

    input = "\\:abc\\:def:aa";
    start = 0;
    output = RequestParser::getNextTerm(input, ':', start);
    ASSERT_EQ(string(":abc:def"), output);
    ASSERT_EQ(size_t(11), start);

    input = "abc:";
    start = 0;
    output = RequestParser::getNextTerm(input, ':', start);
    ASSERT_EQ(string("abc"), output);
    ASSERT_EQ(size_t(4), start);

    input = "abc\\:";
    start = 0;
    output = RequestParser::getNextTerm(input, ':', start);
    ASSERT_EQ(string("abc:"), output);
    ASSERT_EQ(size_t(5), start);

}

TEST_F(RequestParserTest, testParseLayerClause) {
    RequestPtr requestPtr(new Request);
    string oriStr = "config=cluster:cluster1&&layer=range:cat{1,2,3,[10,20],[99,],66,[,44]}*ends{[555,]},quota:1000;range:cat{2,3,[10,20],[,]},quota:100&&query=mp3";
    requestPtr->setOriginalString(oriStr);
    ASSERT_TRUE(_requestParser->parseRequest(requestPtr, _clusterConfigMapPtr));
    QueryLayerClause *layerClause = requestPtr->getQueryLayerClause();
    ASSERT_TRUE(layerClause);
    ASSERT_EQ(string("range:cat{1,2,3,[10,20],[99,],66,[,44]}*ends{[555,]},quota:1000;range:cat{2,3,[10,20],[,]},quota:100"), layerClause->getOriginalString());

    ASSERT_EQ(size_t(2), layerClause->getLayerCount());
    const LayerDescription *layerDescription = layerClause->getLayerDescription(0);
    ASSERT_EQ(uint32_t(1000), layerDescription->getQuota());
    const LayerKeyRange *range = layerDescription->getLayerRange(0);
    ASSERT_EQ(size_t(4), range->values.size());
    ASSERT_EQ(string("cat"), range->attrName);
    ASSERT_EQ(string("1"), range->values[0]);
    ASSERT_EQ(string("2"), range->values[1]);
    ASSERT_EQ(string("3"), range->values[2]);
    ASSERT_EQ(string("66"), range->values[3]);
    ASSERT_EQ(size_t(3), range->ranges.size());
    ASSERT_EQ(string("10"), range->ranges[0].from);
    ASSERT_EQ(string("20"), range->ranges[0].to);
    ASSERT_EQ(string("99"), range->ranges[1].from);
    ASSERT_EQ(string("INFINITE"), range->ranges[1].to);
    ASSERT_EQ(string("INFINITE"), range->ranges[2].from);
    ASSERT_EQ(string("44"), range->ranges[2].to);

    range = layerDescription->getLayerRange(1);
    ASSERT_EQ(string("ends"), range->attrName);
    ASSERT_EQ(size_t(0), range->values.size());
    ASSERT_EQ(size_t(1), range->ranges.size());
    ASSERT_EQ(string("555"), range->ranges[0].from);
    ASSERT_EQ(string("INFINITE"), range->ranges[0].to);

    layerDescription = layerClause->getLayerDescription(1);
    ASSERT_EQ(uint32_t(100), layerDescription->getQuota());
    range = layerDescription->getLayerRange(0);
    ASSERT_EQ(size_t(2), range->values.size());
    ASSERT_EQ(string("2"), range->values[0]);
    ASSERT_EQ(string("3"), range->values[1]);
    ASSERT_EQ(size_t(2), range->ranges.size());
    ASSERT_EQ(string("10"), range->ranges[0].from);
    ASSERT_EQ(string("20"), range->ranges[0].to);
    ASSERT_EQ(string("INFINITE"), range->ranges[1].from);
    ASSERT_EQ(string("INFINITE"), range->ranges[1].to);
}

TEST_F(RequestParserTest, testParseSearcherCacheClause) {
    HA3_LOG(DEBUG, "Begin Test");
    RequestPtr requestPtr(new Request);
    string oriStr = "config=cluster:cluster1&&query=mp3&&searcher_cache="
                    "use:yes,"
                    "key:123456,"
                    "expire_time:func(end_time, 15, 10),"
                    "cur_time:123456,"
                    "cache_filter:olu=yes,"
                    "cache_doc_num_limit:200;700";

    requestPtr->setOriginalString(oriStr);
    ASSERT_TRUE(_requestParser->parseRequest(requestPtr,
                    _clusterConfigMapPtr));
    ASSERT_EQ(ERROR_NONE,
                         _requestParser->getErrorResult().getErrorCode());
    SearcherCacheClause* cacheClause = requestPtr->getSearcherCacheClause();
    ASSERT_TRUE(cacheClause);
    ASSERT_TRUE(cacheClause->getUse());
    ASSERT_EQ((uint64_t)123456, cacheClause->getKey());
    ASSERT_EQ((uint32_t)123456, cacheClause->getCurrentTime());

    SyntaxExpr *expireTimeExpr = cacheClause->getExpireExpr();
    ASSERT_TRUE(expireTimeExpr);
    ASSERT_EQ(string("func(end_time , 15 , 10)"),
                         expireTimeExpr->getExprString());

    FilterClause *filterClause = cacheClause->getFilterClause();
    ASSERT_TRUE(filterClause);
    ASSERT_TRUE(filterClause->getRootSyntaxExpr());
    ASSERT_EQ(string("(olu=yes)"),
                         filterClause->getRootSyntaxExpr()->getExprString());
    ASSERT_EQ((size_t)2, cacheClause->getCacheDocNumVec().size());
    ASSERT_EQ((uint32_t)200, cacheClause->getCacheDocNumVec()[0]);
    ASSERT_EQ((uint32_t)700, cacheClause->getCacheDocNumVec()[1]);
}

TEST_F(RequestParserTest, testParseSearcherCacheClauseFailed) {
    HA3_LOG(DEBUG, "Begin Test");
    RequestPtr requestPtr(new Request);
    string oriStr = "config=cluster:cluster1&&query=mp3&&searcher_cache="
                    "use_wrong:yes";

    requestPtr->setOriginalString(oriStr);
    ASSERT_TRUE(!_requestParser->parseRequest(requestPtr,
                    _clusterConfigMapPtr));
    ASSERT_EQ(ERROR_CACHE_CLAUSE,
                         _requestParser->getErrorResult().getErrorCode());
}

TEST_F(RequestParserTest, testParseConfigClauseAllowLackOfSummary) {
    RequestPtr requestPtr(new Request);
    {
        string oriStr = "config=cluster:cluster1,start:50,hit:10";
        requestPtr->setOriginalString(oriStr);
        ConfigClause *configClause = requestPtr->getConfigClause();
        ASSERT_TRUE(configClause);
        ASSERT_TRUE(_requestParser->parseConfigClause(requestPtr));
        ASSERT_TRUE(configClause->allowLackOfSummary());
    }

    {
        string oriStr = "config=cluster:cluster1,start:50,hit:10,allow_lack_summary:yes";
        requestPtr->setOriginalString(oriStr);
        ConfigClause *configClause = requestPtr->getConfigClause();
        ASSERT_TRUE(configClause);
        ASSERT_TRUE(_requestParser->parseConfigClause(requestPtr));
        ASSERT_TRUE(configClause->allowLackOfSummary());
    }

    {
        string oriStr = "config=cluster:cluster1,start:50,hit:10,allow_lack_summary:no";
        requestPtr->setOriginalString(oriStr);
        ConfigClause *configClause = requestPtr->getConfigClause();
        ASSERT_TRUE(configClause);
        ASSERT_TRUE(_requestParser->parseConfigClause(requestPtr));
        ASSERT_TRUE(!(configClause->allowLackOfSummary()));
    }

    {
        string oriStr = "config=cluster:cluster1,start:50,hit:10,allow_lack_summary:invalid_value";
        requestPtr->setOriginalString(oriStr);
        ConfigClause *configClause = requestPtr->getConfigClause();
        ASSERT_TRUE(configClause);
        ASSERT_TRUE(_requestParser->parseConfigClause(requestPtr));
        ASSERT_TRUE(configClause->allowLackOfSummary());
    }
}

TEST_F(RequestParserTest, testParseFinalSortClause) {
    RequestPtr requestPtr(new Request);
    {
        string oriStr = "final_sort=sort_name:DEFAULT";
        requestPtr->setOriginalString(oriStr);
        FinalSortClause *finalSortClause = requestPtr->getFinalSortClause();
        ASSERT_TRUE(finalSortClause);
        ASSERT_TRUE(_requestParser->parseFinalSortClause(requestPtr));
        ASSERT_EQ(string("DEFAULT"),
                             finalSortClause->getSortName());
    }

    {
        string oriStr = "final_sort=sort_name:UserDefinedSort;"
                        "sort_param:key1#value1,key2#value2";
        requestPtr->setOriginalString(oriStr);
        FinalSortClause *finalSortClause = requestPtr->getFinalSortClause();
        ASSERT_TRUE(finalSortClause);
        ASSERT_TRUE(_requestParser->parseFinalSortClause(requestPtr));
        ASSERT_EQ(string("UserDefinedSort"),
                             finalSortClause->getSortName());
        map<string, string> paramMap = finalSortClause->getParams();
        ASSERT_EQ(string("value1"), paramMap["key1"]);
        ASSERT_EQ(string("value2"), paramMap["key2"]);
    }

    {
        string oriStr = "final_sort=sort_name_error:UserDefinedSort;"
                        "sort_param:key1#value1,key2#value2";
        requestPtr->setOriginalString(oriStr);
        FinalSortClause *finalSortClause = requestPtr->getFinalSortClause();
        ASSERT_TRUE(finalSortClause);
        ASSERT_TRUE(!_requestParser->parseFinalSortClause(requestPtr));
    }

    {
        string oriStr = "final_sort=sort_name:UserDefinedSort;"
                        "sort_param_error:key1#value1,key2#value2";
        requestPtr->setOriginalString(oriStr);
        FinalSortClause *finalSortClause = requestPtr->getFinalSortClause();
        ASSERT_TRUE(finalSortClause);
        ASSERT_TRUE(!_requestParser->parseFinalSortClause(requestPtr));
    }

    {
        string oriStr = "final_sort=sort_param:key1#value1,key2#value2;"
                        "sort_param:key1#value1,key2#value2";
        requestPtr->setOriginalString(oriStr);
        FinalSortClause *finalSortClause = requestPtr->getFinalSortClause();
        ASSERT_TRUE(finalSortClause);
        ASSERT_TRUE(!_requestParser->parseFinalSortClause(requestPtr));
    }

    {
        string oriStr = "final_sort=sort_name:UserDefinedSort;"
                        "sort_param:key1,key2#value2";
        requestPtr->setOriginalString(oriStr);
        FinalSortClause *finalSortClause = requestPtr->getFinalSortClause();
        ASSERT_TRUE(finalSortClause);
        ASSERT_TRUE(!_requestParser->parseFinalSortClause(requestPtr));
    }

    {
        string oriStr = "final_sort=sort_name:UserDefinedSort;"
                        "sort_param:key1#value1#value2";
        requestPtr->setOriginalString(oriStr);
        FinalSortClause *finalSortClause = requestPtr->getFinalSortClause();
        ASSERT_TRUE(finalSortClause);
        ASSERT_TRUE(!_requestParser->parseFinalSortClause(requestPtr));
    }

    {
        string oriStr = "final_sort=sort_name:UserDefinedSort;"
                        "sort_param:key1#value1,key2#value2;sort_param2:key1#value1";
        requestPtr->setOriginalString(oriStr);
        FinalSortClause *finalSortClause = requestPtr->getFinalSortClause();
        ASSERT_TRUE(finalSortClause);
        ASSERT_TRUE(!_requestParser->parseFinalSortClause(requestPtr));
    }
}

TEST_F(RequestParserTest, testParseLevelClause) {
#define prepareRequest(oriStr)                                  \
    RequestPtr requestPtr(new Request);                         \
    requestPtr->setOriginalString(oriStr);                      \
    LevelClause *levelClause = requestPtr->getLevelClause();    \
    ASSERT_TRUE(levelClause)

    {
        prepareRequest("level=uselevel:no");
        ASSERT_TRUE(_requestParser->parseLevelClause(requestPtr));
        ASSERT_TRUE(!levelClause->useLevel());
    }
    {
        prepareRequest("level=min_docs:12x");
        ASSERT_TRUE(!_requestParser->parseLevelClause(requestPtr));
    }
    {
        prepareRequest("level=second_level_cluster:c2,level_type:both");
        ASSERT_TRUE(_requestParser->parseLevelClause(requestPtr));
        ASSERT_EQ(string("c2.default"), levelClause->getSecondLevelCluster());
        ASSERT_EQ(BOTH_LEVEL, levelClause->getLevelQueryType());
    }
    {
        prepareRequest("level=second_level_cluster:c2,min_docs:123");
        ASSERT_TRUE(_requestParser->parseLevelClause(requestPtr));
        ASSERT_EQ(string("c2.default"), levelClause->getSecondLevelCluster());
        ASSERT_EQ((uint32_t)123, levelClause->getMinDocs());
        ASSERT_EQ(CHECK_FIRST_LEVEL, levelClause->getLevelQueryType());
    }
    {
        prepareRequest("level=level_clusters:good1;bad1&bad2;worse1&worse2&worse3,min_docs:123");
        ASSERT_TRUE(_requestParser->parseLevelClause(requestPtr));
        ASSERT_EQ(string("good1.default"), levelClause->getSecondLevelCluster());
        ASSERT_EQ(string("good1.default"), levelClause->getLevelClusters()[0][0]);
        ASSERT_EQ(string("bad1.default"), levelClause->getLevelClusters()[1][0]);
        ASSERT_EQ(string("bad2.default"), levelClause->getLevelClusters()[1][1]);
        ASSERT_EQ(string("worse1.default"), levelClause->getLevelClusters()[2][0]);
        ASSERT_EQ(string("worse2.default"), levelClause->getLevelClusters()[2][1]);
        ASSERT_EQ(string("worse3.default"), levelClause->getLevelClusters()[2][2]);
        ASSERT_EQ((uint32_t)123, levelClause->getMinDocs());
        ASSERT_EQ(CHECK_FIRST_LEVEL, levelClause->getLevelQueryType());
    }
    {
        prepareRequest("level=level_clusters:;bad1&bad2;worse1&worse2&worse3,min_docs:123");
        ASSERT_TRUE(_requestParser->parseLevelClause(requestPtr));
        ASSERT_EQ(string("bad1.default"), levelClause->getSecondLevelCluster());
        ASSERT_EQ(string("bad1.default"), levelClause->getLevelClusters()[0][0]);
        ASSERT_EQ(string("bad2.default"), levelClause->getLevelClusters()[0][1]);
        ASSERT_EQ(string("worse1.default"), levelClause->getLevelClusters()[1][0]);
        ASSERT_EQ(string("worse2.default"), levelClause->getLevelClusters()[1][1]);
        ASSERT_EQ(string("worse3.default"), levelClause->getLevelClusters()[1][2]);
        ASSERT_EQ((uint32_t)123, levelClause->getMinDocs());
        ASSERT_EQ(CHECK_FIRST_LEVEL, levelClause->getLevelQueryType());
    }
    {
        prepareRequest("level=level_clusters:bad1,min_docs:123");
        ASSERT_TRUE(_requestParser->parseLevelClause(requestPtr));
        ASSERT_EQ(string("bad1.default"), levelClause->getSecondLevelCluster());
        ASSERT_EQ(string("bad1.default"), levelClause->getLevelClusters()[0][0]);
    }
    {
        prepareRequest("level=level_clusters:;;,min_docs:123");
        ASSERT_TRUE(!_requestParser->parseLevelClause(requestPtr));
    }
    {
        prepareRequest("level=level_clusters:good1&good2;bad1&bad2;,second_level_cluster:good1,min_docs:123");
        ASSERT_TRUE(!_requestParser->parseLevelClause(requestPtr));
    }


#undef prepareRequest
}

TEST_F(RequestParserTest, testParseConfigClauseWithTimeOutFailed) {
    HA3_LOG(DEBUG, "Begin Test");
    {
        RequestPtr requestPtr(new Request);
        string oriStr = "seek_timeout:abc";
        requestPtr->setOriginalString("config=" + oriStr);
        ConfigClause *configClause = requestPtr->getConfigClause();
        ASSERT_TRUE(configClause);
        ASSERT_TRUE(!_requestParser->parseConfigClause(requestPtr));
        ErrorResult errResult = _requestParser->getErrorResult();
        ASSERT_EQ(ERROR_CONFIG_CLAUSE_TIMEOUT,
                             errResult.getErrorCode());
    }

    {
        RequestPtr requestPtr(new Request);
        string oriStr = "seek_timeout:-20";
        requestPtr->setOriginalString("config=" + oriStr);
        ConfigClause *configClause = requestPtr->getConfigClause();
        ASSERT_TRUE(configClause);
        ASSERT_TRUE(!_requestParser->parseConfigClause(requestPtr));
        ErrorResult errResult = _requestParser->getErrorResult();
        ASSERT_EQ(ERROR_CONFIG_CLAUSE_TIMEOUT,
                             errResult.getErrorCode());
    }
}

TEST_F(RequestParserTest, testParseConfigClauseForResearchThreshold) {
    {
        RequestPtr requestPtr(new Request);
        string oriStr = "config=research_threshold:100";
        requestPtr->setOriginalString(oriStr);
        ConfigClause *configClause = requestPtr->getConfigClause();
        ASSERT_TRUE(configClause);
        ASSERT_TRUE(_requestParser->parseConfigClause(requestPtr));
        ASSERT_EQ((uint32_t)100, configClause->getResearchThreshold());
    }
    {
        RequestPtr requestPtr(new Request);
        string oriStr = "config=research_threshold:-1";
        requestPtr->setOriginalString(oriStr);
        ConfigClause *configClause = requestPtr->getConfigClause();
        ASSERT_TRUE(configClause);
        ASSERT_TRUE(!_requestParser->parseConfigClause(requestPtr));
        ASSERT_EQ(ERROR_CONFIG_CLAUSE_RESEARCH_THRESHOLD,
                             _requestParser->getErrorResult().getErrorCode());
    }
}

TEST_F(RequestParserTest, testParseConfigClauseForNeedRerank) {
    {
        RequestPtr requestPtr(new Request);
        string oriStr = "config=research_threshold:100";
        requestPtr->setOriginalString(oriStr);
        ConfigClause *configClause = requestPtr->getConfigClause();
        ASSERT_TRUE(configClause);
        ASSERT_TRUE(_requestParser->parseConfigClause(requestPtr));
        ASSERT_TRUE(!configClause->needRerank());
    }
    {
        RequestPtr requestPtr(new Request);
        string oriStr = "config=research_threshold:100,rerank_hint:true";
        requestPtr->setOriginalString(oriStr);
        ConfigClause *configClause = requestPtr->getConfigClause();
        ASSERT_TRUE(configClause);
        ASSERT_TRUE(_requestParser->parseConfigClause(requestPtr));
        ASSERT_TRUE(configClause->needRerank());
    }
}

TEST_F(RequestParserTest, testParseAnalyzerClause)
{
    HA3_LOG(DEBUG, "Begin Test");
    {
        RequestPtr requestPtr(new Request);
        string oriStr = "analyzer=specific_index_analyzer:index1#aliws;"
                        "index2#singlews,no_tokenize_indexes:index3";
        requestPtr->setOriginalString(oriStr);

        AnalyzerClause *analyzerClause = requestPtr->getAnalyzerClause();
        ASSERT_TRUE(analyzerClause);
        ASSERT_EQ(
                string("specific_index_analyzer:index1#aliws;"
                       "index2#singlews,no_tokenize_indexes:index3"),
                analyzerClause->getOriginalString());

        ASSERT_TRUE(_requestParser->parseAnalyzerClause(requestPtr));
        ConfigClause *configClause = requestPtr->getConfigClause();
        ASSERT_TRUE(configClause);
        ASSERT_EQ(string("aliws"),
                             configClause->getIndexAnalyzerName("index1"));
        ASSERT_EQ(string("singlews"),
                             configClause->getIndexAnalyzerName("index2"));
        ASSERT_EQ(string(""),
                             configClause->getIndexAnalyzerName("index3"));
        const set<string>& noTokenizeIndexs = configClause->getNoTokenizeIndexes();
        ASSERT_EQ((size_t)1, noTokenizeIndexs.size());
        ASSERT_TRUE(noTokenizeIndexs.find("index3") != noTokenizeIndexs.end());
        ASSERT_TRUE(configClause->isIndexNoTokenize("index3"));
        ASSERT_EQ(string(""),
                             configClause->getIndexAnalyzerName("index4"));
    }
    {
        RequestPtr requestPtr(new Request);
        string oriStr = "analyzer=global_analyzer:singlews";
        requestPtr->setOriginalString(oriStr);
        AnalyzerClause *analyzerClause = requestPtr->getAnalyzerClause();
        ASSERT_EQ(string("global_analyzer:singlews"),
                             analyzerClause->getOriginalString());

        ASSERT_TRUE(analyzerClause);
        ASSERT_TRUE(_requestParser->parseAnalyzerClause(requestPtr));

        ConfigClause *configClause = requestPtr->getConfigClause();
        ASSERT_TRUE(configClause);
        ASSERT_EQ(string("singlews"),
                             configClause->getIndexAnalyzerName("index1"));
        ASSERT_EQ(string("singlews"),
                             configClause->getIndexAnalyzerName("index2"));
        ASSERT_EQ(string("singlews"),
                             configClause->getIndexAnalyzerName("index3"));
        const set<string>& noTokenizeIndexs = configClause->getNoTokenizeIndexes();
        ASSERT_EQ((size_t)0, noTokenizeIndexs.size());
    }
    {
        RequestPtr requestPtr(new Request);
        string oriStr = "analyzer=global_analyzer:singlews,"
                        "specific_index_analyzer:index1#aliws";

        requestPtr->setOriginalString(oriStr);

        AnalyzerClause *analyzerClause = requestPtr->getAnalyzerClause();
        ASSERT_TRUE(analyzerClause);
        ASSERT_TRUE(_requestParser->parseAnalyzerClause(requestPtr));
        ConfigClause *configClause = requestPtr->getConfigClause();
        ASSERT_EQ(string("aliws"),
                             configClause->getIndexAnalyzerName("index1"));
        ASSERT_EQ(string("singlews"),
                             configClause->getIndexAnalyzerName("index2"));
        ASSERT_EQ(string("singlews"),
                             configClause->getIndexAnalyzerName("index3"));
        const set<string>& noTokenizeIndexs = configClause->getNoTokenizeIndexes();
        ASSERT_EQ((size_t)0, noTokenizeIndexs.size());
    }
    {
        RequestPtr requestPtr(new Request);
        string oriStr = "analyzer=global_analyzer:singlews,"
                        "specific_index_analyzer:index1#aliws;index1#alibaba";

        requestPtr->setOriginalString(oriStr);

        AnalyzerClause *analyzerClause = requestPtr->getAnalyzerClause();
        ASSERT_TRUE(analyzerClause);
        ASSERT_TRUE(_requestParser->parseAnalyzerClause(requestPtr));
        ConfigClause *configClause = requestPtr->getConfigClause();
        ASSERT_EQ(string("alibaba"),
                             configClause->getIndexAnalyzerName("index1"));
        ASSERT_EQ(string("singlews"),
                             configClause->getIndexAnalyzerName("index2"));
        const set<string>& noTokenizeIndexs = configClause->getNoTokenizeIndexes();
        ASSERT_EQ((size_t)0, noTokenizeIndexs.size());
    }
    {
        RequestPtr requestPtr(new Request);
        string oriStr = "analyzer=global_analyzer:singlews,"
                        "specific_index_analyzer:index1#aliws;index2#alibaba,"
                        "no_tokenize_indexes:index1;index3";
        requestPtr->setOriginalString(oriStr);

        AnalyzerClause *analyzerClause = requestPtr->getAnalyzerClause();
        ASSERT_TRUE(analyzerClause);
        ASSERT_TRUE(_requestParser->parseAnalyzerClause(requestPtr));
        ConfigClause *configClause = requestPtr->getConfigClause();
        ASSERT_EQ(string("aliws"),
                             configClause->getIndexAnalyzerName("index1"));
        ASSERT_EQ(string("alibaba"),
                             configClause->getIndexAnalyzerName("index2"));
        ASSERT_EQ(string("singlews"),
                             configClause->getIndexAnalyzerName("index3"));
        ASSERT_TRUE(configClause->isIndexNoTokenize("index1"));
        ASSERT_TRUE(!configClause->isIndexNoTokenize("index2"));
        ASSERT_TRUE(configClause->isIndexNoTokenize("index3"));
    }
}

TEST_F(RequestParserTest, testParseInvalidAnalyzerClause)
{
    HA3_LOG(DEBUG, "Begin Test");
    {
        RequestPtr requestPtr(new Request);
        string oriStr = "analyzer=global_analyzer:,"
                        "specific_index_analyzer:index1#aliws;index2#alibaba,"
                        "no_tokenize_indexes:index1;index3";

        requestPtr->setOriginalString(oriStr);

        AnalyzerClause *analyzerClause = requestPtr->getAnalyzerClause();
        ASSERT_TRUE(analyzerClause);
        ASSERT_TRUE(!_requestParser->parseAnalyzerClause(requestPtr));
        ASSERT_EQ(ERROR_INVALID_ANALYZER_GRAMMAR,
                             _requestParser->getErrorResult().getErrorCode());
    }
    {
        RequestPtr requestPtr(new Request);
        string oriStr = "analyzer=global_analyzer:singlews,"
                        "specific_index_analyzer:,"
                        "no_tokenize_indexes:index1;index3";

        requestPtr->setOriginalString(oriStr);

        AnalyzerClause *analyzerClause = requestPtr->getAnalyzerClause();
        ASSERT_TRUE(analyzerClause);
        ASSERT_TRUE(!_requestParser->parseAnalyzerClause(requestPtr));
        ASSERT_EQ(ERROR_INVALID_ANALYZER_GRAMMAR,
                             _requestParser->getErrorResult().getErrorCode());
    }
    {
        RequestPtr requestPtr(new Request);
        string oriStr = "analyzer=global_analyzer:singlews,"
                        "specific_index_analyzer:index1#aliws,"
                        "no_tokenize_indexes:";

        requestPtr->setOriginalString(oriStr);

        AnalyzerClause *analyzerClause = requestPtr->getAnalyzerClause();
        ASSERT_TRUE(analyzerClause);
        ASSERT_TRUE(!_requestParser->parseAnalyzerClause(requestPtr));
        ASSERT_EQ(ERROR_INVALID_ANALYZER_GRAMMAR,
                             _requestParser->getErrorResult().getErrorCode());
    }

}

TEST_F(RequestParserTest, testParseAnalyzerClauseCompatible)
{
    HA3_LOG(DEBUG, "Begin Test");
    {
        RequestPtr requestPtr(new Request);
        string oriStr = "config=cluster:daogou,analyzer:aliws";
        requestPtr->setOriginalString(oriStr);

        ConfigClause *configClause = requestPtr->getConfigClause();
        ASSERT_TRUE(configClause);

        ASSERT_TRUE(_requestParser->parseConfigClause(requestPtr));

        AnalyzerClause* analyzerClause = requestPtr->getAnalyzerClause();
        ASSERT_TRUE(!analyzerClause);

        ASSERT_EQ(string("aliws"),
                             configClause->getIndexAnalyzerName("index1"));
        ASSERT_EQ(string("aliws"),
                             configClause->getIndexAnalyzerName("index2"));
        ASSERT_EQ(string("aliws"),
                             configClause->getIndexAnalyzerName("index3"));
        ASSERT_TRUE(configClause->getNoTokenizeIndexes().empty());
    }
    {
        RequestPtr requestPtr(new Request);
        string oriStr = "config=cluster:daogou,no_tokenize_indexes:index1;index2";
        requestPtr->setOriginalString(oriStr);

        ConfigClause *configClause = requestPtr->getConfigClause();
        ASSERT_TRUE(configClause);

        ASSERT_TRUE(_requestParser->parseConfigClause(requestPtr));
        AnalyzerClause* analyzerClause = requestPtr->getAnalyzerClause();
        ASSERT_TRUE(!analyzerClause);

        ASSERT_EQ(size_t(2), configClause->getNoTokenizeIndexes().size());
        ASSERT_TRUE(configClause->isIndexNoTokenize("index1"));
        ASSERT_EQ(string(""),
                             configClause->getIndexAnalyzerName("index1"));
        ASSERT_TRUE(configClause->isIndexNoTokenize("index2"));
        ASSERT_EQ(string(""),
                             configClause->getIndexAnalyzerName("index2"));
        ASSERT_EQ(string(""),
                             configClause->getIndexAnalyzerName("index3"));
    }
    {
        RequestPtr requestPtr(new Request);
        string oriStr = "config=cluster:daogou,analyzer:aliws,"
                        "no_tokenize_indexes:index1;index2";
        requestPtr->setOriginalString(oriStr);

        ConfigClause *configClause = requestPtr->getConfigClause();
        ASSERT_TRUE(configClause);

        ASSERT_TRUE(_requestParser->parseConfigClause(requestPtr));
        AnalyzerClause* analyzerClause = requestPtr->getAnalyzerClause();
        ASSERT_TRUE(!analyzerClause);

        ASSERT_EQ(size_t(2), configClause->getNoTokenizeIndexes().size());
        ASSERT_TRUE(configClause->isIndexNoTokenize("index1"));
        ASSERT_EQ(string("aliws"),
                             configClause->getIndexAnalyzerName("index1"));
        ASSERT_TRUE(configClause->isIndexNoTokenize("index2"));
        ASSERT_EQ(string("aliws"),
                             configClause->getIndexAnalyzerName("index2"));
        ASSERT_EQ(string("aliws"),
                             configClause->getIndexAnalyzerName("index3"));
    }
    {
        RequestPtr requestPtr(new Request);
        string oriStr = "config=cluster:daogou,analyzer:aliws,"
                        "no_tokenize_indexes:index1;index2"
                        "&&analyzer=specific_index_analyzer:"
                        "index1#singlews;index2#aliws,"
                        "global_analyzer:alibaba";
        requestPtr->setOriginalString(oriStr);

        ConfigClause *configClause = requestPtr->getConfigClause();
        ASSERT_TRUE(configClause);
        AnalyzerClause* analyzerClause = requestPtr->getAnalyzerClause();
        ASSERT_TRUE(analyzerClause);
        ASSERT_TRUE(_requestParser->parseConfigClause(requestPtr));
        ASSERT_TRUE(_requestParser->parseAnalyzerClause(requestPtr));
        ASSERT_EQ(string("singlews"),
                             configClause->getIndexAnalyzerName("index1"));
        ASSERT_TRUE(configClause->isIndexNoTokenize("index1"));
        ASSERT_EQ(string("aliws"),
                             configClause->getIndexAnalyzerName("index2"));
        ASSERT_TRUE(configClause->isIndexNoTokenize("index2"));
        ASSERT_EQ(string("alibaba"),
                             configClause->getIndexAnalyzerName("index3"));
    }
    {
        RequestPtr requestPtr(new Request);
        string oriStr = "config=cluster:daogou,analyzer:aliws,"
                        "no_tokenize_indexes:index1;index2"
                        "&&analyzer=specific_index_analyzer:"
                        "index1#singlews;index2#aliws,"
                        "global_analyzer:alibaba,"
                        "no_tokenize_indexes:index3";
        requestPtr->setOriginalString(oriStr);

        ConfigClause *configClause = requestPtr->getConfigClause();
        ASSERT_TRUE(configClause);
        AnalyzerClause* analyzerClause = requestPtr->getAnalyzerClause();
        ASSERT_TRUE(analyzerClause);
        ASSERT_TRUE(_requestParser->parseConfigClause(requestPtr));
        ASSERT_TRUE(_requestParser->parseAnalyzerClause(requestPtr));
        ASSERT_EQ(string("singlews"),
                             configClause->getIndexAnalyzerName("index1"));
        ASSERT_TRUE(configClause->isIndexNoTokenize("index1"));
        ASSERT_EQ(string("aliws"),
                             configClause->getIndexAnalyzerName("index2"));
        ASSERT_TRUE(configClause->isIndexNoTokenize("index2"));
        ASSERT_EQ(string("alibaba"),
                             configClause->getIndexAnalyzerName("index3"));
        ASSERT_TRUE(configClause->isIndexNoTokenize("index3"));
        ASSERT_EQ(size_t(3), configClause->getNoTokenizeIndexes().size());
    }
}

TEST_F(RequestParserTest, testParseAuxFilterClause) {
    HA3_LOG(DEBUG, "Begin Test!");

    string prefixRequestStr = "config=cluster:cluster1,start:0,hit:10&&aux_filter=";
    string auxFilterClauseStr = "(host=\"www.sina.com.cn\")";
    RequestPtr requestPtr(new Request);
    requestPtr->setOriginalString(prefixRequestStr + auxFilterClauseStr);
    ASSERT_TRUE(_requestParser->parseRequest(requestPtr, _clusterConfigMapPtr));

    AuxFilterClause *auxFilterClause = requestPtr->getAuxFilterClause();
    ASSERT_TRUE(auxFilterClause);

    ASSERT_EQ(auxFilterClauseStr, auxFilterClause->getOriginalString());

    AtomicSyntaxExpr *checkExpr1 = new AtomicSyntaxExpr("host", vt_unknown, ATTRIBUTE_NAME);
    AtomicSyntaxExpr *checkExpr2 = new AtomicSyntaxExpr("www.sina.com.cn", vt_unknown, STRING_VALUE);
    EqualSyntaxExpr checkEqualExpr(checkExpr1, checkExpr2);
    ASSERT_TRUE(checkEqualExpr == (auxFilterClause->getRootSyntaxExpr()));
}

END_HA3_NAMESPACE(queryparser);
