#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/RequestCacheKeyGenerator.h>
#include <ha3/queryparser/RequestSymbolDefine.h>
#include <ha3_sdk/testlib/qrs/RequestCreator.h>

using namespace std;
BEGIN_HA3_NAMESPACE(common);

class RequestCacheKeyGeneratorTest : public TESTBASE {
public:
    RequestCacheKeyGeneratorTest();
    ~RequestCacheKeyGeneratorTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(common, RequestCacheKeyGeneratorTest);


RequestCacheKeyGeneratorTest::RequestCacheKeyGeneratorTest() { 
}

RequestCacheKeyGeneratorTest::~RequestCacheKeyGeneratorTest() { 
}

void RequestCacheKeyGeneratorTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void RequestCacheKeyGeneratorTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

static string mapToStr(const map<string, string> &strMap) {
    string mapStr;
    for (map<string, string>::const_iterator it = strMap.begin(); 
         it != strMap.end(); ++it) 
    {
        mapStr += it->first;
        mapStr += ":";
        mapStr += it->second;
        mapStr += ",";
    }
    return mapStr;
}

TEST_F(RequestCacheKeyGeneratorTest, testInitRequst) { 
    HA3_LOG(DEBUG, "Begin Test!");
    string requestStr = "config=cluster:mainse_searcher,no_summary:yes,start:0,"
                        "hit:20,rerank_size:3000,format:xml,trace:DEBUG,analyzer:taobao_analyzer,"
                        "batch_score:true,no_tokenize_indexes:index1;index2,optimize_comparator:true,"
                        "optimize_rank:true,rank_trace:DEBUG,debug_query_key:xxx,searcher_return_hits:10&&"
                        "query=('大皂角' OR '皂角') AND auction_type:'b'&&"
                        "kvpairs=_ps:static_trans_score,_ss:ss_score&&"
                        "sort=-RANK;-ss_score&&"
                        "rank=rank_profile:ScorerRank&&"
                        "virtual_attribute=DISCNT_T:discnt_t(zk_time,zk_rate);&&"
                        "analyzer=specific_index_analyzer:index3#aliws;index4#alibaba";
    RequestPtr requestPtr = qrs::RequestCreator::prepareRequest(requestStr);
    ASSERT_TRUE(requestPtr);
    RequestCacheKeyGenerator keyGenerator(requestPtr);
    map<string, ClauseOptionKVMap> expectClauseOptionKVMaps;
    ClauseOptionKVMap& kvConfigMap = expectClauseOptionKVMaps["config"];
    ClauseOptionKVMap& kvPairsMap = expectClauseOptionKVMaps["kvpairs"];
    ClauseOptionKVMap& kvVAMap = expectClauseOptionKVMaps["virtual_attribute"];
    kvConfigMap["analyzer"] = "taobao_analyzer";
    kvConfigMap["batch_score"] = "true";
    kvConfigMap["no_tokenize_indexes"] = "index1|index2|";
    kvConfigMap["optimize_comparator"] = "true";
    kvConfigMap["optimize_rank"] = "true";
    kvConfigMap["rank_size"] = "0";
    kvConfigMap["rank_trace"] = "DEBUG";
    kvConfigMap["rerank_size"] = "3000";
    kvConfigMap["debug_query_key"] = "xxx";
    kvConfigMap["specific_index_analyzer"] = "index3#aliws|index4#alibaba|";
    kvConfigMap["sub_doc"] = "0";
    kvConfigMap["fetch_summary_type"] = "1";
    kvPairsMap["_ps"] = "static_trans_score";
    kvPairsMap["_ss"] = "ss_score";
    kvVAMap["DISCNT_T"] = "discnt_t(zk_time , zk_rate)";
    
    ASSERT_EQ(mapToStr(expectClauseOptionKVMaps["virtual_attribute"]),
                         mapToStr(keyGenerator._clauseKVMaps["virtual_attribute"]));
    ASSERT_EQ(mapToStr(expectClauseOptionKVMaps["kvpairs"]),
                         mapToStr(keyGenerator._clauseKVMaps["kvpairs"]));
    ASSERT_EQ(mapToStr(expectClauseOptionKVMaps["config"]),
                         mapToStr(keyGenerator._clauseKVMaps["config"]));

    map<string, string> expectClauseCacheStrs;
    expectClauseCacheStrs["query"] = "AndQuery:[OrQuery:[TermQuery:[Term:["
                                     "default0||大皂角|100|]], TermQuery:[Term:"
                                     "[default0||皂角|100|]], ], TermQuery:[Term:[auction_type||b|100|]], ]|";
    expectClauseCacheStrs["sort"] = "-(RANK);-(ss_score);";
    expectClauseCacheStrs["rank"] = "rankprofilename:ScorerRank,fieldboostdescription:";
    ASSERT_EQ(mapToStr(expectClauseCacheStrs),
                         mapToStr(keyGenerator._clauseCacheStrs));
}

TEST_F(RequestCacheKeyGeneratorTest, testDisableOption) {
    string requestStr1 = "config=cluster:mainse_searcher,no_summary:yes,start:0,hit:20,rerank_size:3000,format:xml,analyzer:taobao_analyzer,batch_score:true,no_tokenize_indexes:index1;index2,optimize_comparator:true,optimize_rank:true,rank_trace:info&&"
                         "query=('大皂角' OR '皂角') AND auction_type:'b'&&"
                         "kvpairs=_ps:static_trans_score,_ss:ss_score&&"
                         "sort=-RANK;-ss_score&&"
                         "rank=rank_profile:ScorerRank&&"
                         "virtual_attribute=DISCNT_T:discnt_t(zk_time,zk_rate)";
    string requestStr2 = "config=cluster:mainse_searcher,no_summary:yes,start:0,hit:20,rerank_size:3000,format:xml,analyzer:taobao_analyzer,batch_score:true,no_tokenize_indexes:index1;index2,optimize_comparator:true,optimize_rank:true,rank_trace:debug&&"
                         "query=('大皂角' OR '皂角') AND auction_type:'b'&&"
                         "kvpairs=_ps:static_trans_score,_ss:ss_score&&"
                         "sort=-RANK;-ss_score&&"
                         "rank=rank_profile:ScorerRank&&"
                         "virtual_attribute=DISCNT_T:discnt_t(zk_time,zk_rate)";
    RequestPtr requestPtr1 = qrs::RequestCreator::prepareRequest(requestStr1);
    ASSERT_TRUE(requestPtr1);
    RequestPtr requestPtr2 = qrs::RequestCreator::prepareRequest(requestStr2);
    ASSERT_TRUE(requestPtr2);
    RequestCacheKeyGenerator keyGenerator1(requestPtr1);
    RequestCacheKeyGenerator keyGenerator2(requestPtr2);
    
    keyGenerator1.disableOption(CONFIG_CLAUSE, "none_exist_option");
    ASSERT_TRUE(keyGenerator1.generateRequestCacheKey() != 
                   keyGenerator2.generateRequestCacheKey());
    ASSERT_TRUE(keyGenerator1.generateRequestCacheString() != 
                   keyGenerator2.generateRequestCacheString());

    keyGenerator1.disableOption(CONFIG_CLAUSE, "rank_trace");
    keyGenerator2.disableOption(CONFIG_CLAUSE, "rank_trace");
    ASSERT_EQ(keyGenerator1.generateRequestCacheKey(), 
                         keyGenerator2.generateRequestCacheKey());
    ASSERT_EQ(keyGenerator1.generateRequestCacheString(), 
                         keyGenerator2.generateRequestCacheString());

    keyGenerator1.disableOption(KVPAIR_CLAUSE, "_ps");
    keyGenerator2.disableOption(KVPAIR_CLAUSE, "_ps");
    ASSERT_EQ(keyGenerator1.generateRequestCacheKey(), 
                         keyGenerator2.generateRequestCacheKey());
    ASSERT_EQ(keyGenerator1.generateRequestCacheString(), 
                         keyGenerator2.generateRequestCacheString());
}

TEST_F(RequestCacheKeyGeneratorTest, testDisableClause) {
        string requestStr1 = "config=cluster:mainse_searcher,no_summary:yes,start:0,hit:20,rerank_size:3000,format:xml,trace:DEBUG,analyzer:taobao_analyzer,batch_score:true,no_tokenize_indexes:index1;index2,optimize_comparator:true,optimize_rank:true,rank_trace:debug&&"
                             "query=('大皂角' OR '皂角') AND auction_type:'b'&&"
                             "kvpairs=_ps:static_trans_score,_ss:ss_score&&"
                             "sort=-RANK;-ss_score&&"
                             "virtual_attribute=DISCNT_T:discnt_t(zk_time,zk_rate)";
        string requestStr2 = "config=cluster:mainse_searcher,no_summary:yes,start:0,hit:20,rerank_size:3000,format:xml,analyzer:taobao_analyzer,batch_score:true,no_tokenize_indexes:index1;index2,optimize_comparator:true,optimize_rank:true,rank_trace:info&&"
                             "query=('大皂角' OR '皂角') AND auction_type:'b'&&"
                             "kvpairs=_ps:static_trans_score,_ss:ss_score&&"
                             "sort=-RANK;-ss_score&&"
                             "virtual_attribute=DISCNT_T:discnt_t(zk_time,zk_rate)";
        RequestPtr requestPtr1 = qrs::RequestCreator::prepareRequest(requestStr1);
        ASSERT_TRUE(requestPtr1);
        RequestPtr requestPtr2 = qrs::RequestCreator::prepareRequest(requestStr2);
        ASSERT_TRUE(requestPtr2);
        RequestCacheKeyGenerator keyGenerator1(requestPtr1);
        RequestCacheKeyGenerator keyGenerator2(requestPtr2);
        ASSERT_TRUE(keyGenerator1.generateRequestCacheKey() 
                       != keyGenerator2.generateRequestCacheKey());
        ASSERT_TRUE(keyGenerator1.generateRequestCacheString() 
                       != keyGenerator2.generateRequestCacheString());
        keyGenerator1.disableClause(CONFIG_CLAUSE);
        keyGenerator2.disableClause(CONFIG_CLAUSE);
        ASSERT_EQ(keyGenerator1.generateRequestCacheKey(), 
                             keyGenerator2.generateRequestCacheKey());
        ASSERT_EQ(keyGenerator1.generateRequestCacheString(), 
                             keyGenerator2.generateRequestCacheString());
        
        keyGenerator1.disableClause(VIRTUALATTRIBUTE_CLAUSE);
        keyGenerator2.disableClause(VIRTUALATTRIBUTE_CLAUSE);
        ASSERT_EQ(keyGenerator1.generateRequestCacheKey(), 
                             keyGenerator2.generateRequestCacheKey());
        ASSERT_EQ(keyGenerator1.generateRequestCacheString(), 
                             keyGenerator2.generateRequestCacheString());
}

TEST_F(RequestCacheKeyGeneratorTest, testAccessClauseOption) {
    string requestStr1 = "config=cluster:mainse_searcher,no_summary:yes,start:0,hit:20,rerank_size:3000,format:xml,analyzer:taobao_analyzer,batch_score:true,no_tokenize_indexes:index1;index2,optimize_comparator:true,optimize_rank:true,rank_trace:debug&&"
                         "query=('大皂角' OR '皂角') AND auction_type:'b'&&"
                         "kvpairs=_ps:static_trans_score,_ss:ss_score&&"
                         "sort=-RANK;-ss_score&&"
                         "virtual_attribute=DISCNT_T:discnt_t(zk_time,zk_rate)";
    RequestPtr requestPtr1 = qrs::RequestCreator::prepareRequest(requestStr1);
    ASSERT_TRUE(requestPtr1);
    RequestCacheKeyGenerator keyGenerator1(requestPtr1);
    ASSERT_EQ(string("debug"), 
                         keyGenerator1.getClauseOption(CONFIG_CLAUSE, "rank_trace"));
    ASSERT_EQ(string(""), 
                         keyGenerator1.getClauseOption(CONFIG_CLAUSE, "format"));
    ASSERT_EQ(string("static_trans_score"), 
                         keyGenerator1.getClauseOption(KVPAIR_CLAUSE, "_ps"));
    ASSERT_EQ(string(""), 
                         keyGenerator1.getClauseOption(
                                 KVPAIR_CLAUSE, "none_exist_key"));
    ASSERT_EQ(string("discnt_t(zk_time , zk_rate)"), 
                         keyGenerator1.getClauseOption(
                                 VIRTUALATTRIBUTE_CLAUSE, "DISCNT_T"));
    ASSERT_EQ(string(""), 
                         keyGenerator1.getClauseOption(
                                 VIRTUALATTRIBUTE_CLAUSE, "none_exist_virtual_attribute"));

    ASSERT_TRUE(keyGenerator1.setClauseOption(
                    CONFIG_CLAUSE, "trace", "TRACE3"));
    ASSERT_EQ(string("TRACE3"), 
                         keyGenerator1.getClauseOption(CONFIG_CLAUSE, "trace"));
    ASSERT_TRUE(keyGenerator1.setClauseOption(
                    KVPAIR_CLAUSE, "_ps", "kv"));
    ASSERT_EQ(string("kv"), 
                         keyGenerator1.getClauseOption(KVPAIR_CLAUSE, "_ps"));
    ASSERT_TRUE(keyGenerator1.setClauseOption(
                    VIRTUALATTRIBUTE_CLAUSE, "none_exist_key", "none_exist_key_value"));
    ASSERT_EQ(string("none_exist_key_value"), 
                         keyGenerator1.getClauseOption(
                                 VIRTUALATTRIBUTE_CLAUSE, "none_exist_key"));
    ASSERT_TRUE(!keyGenerator1.setClauseOption(FETCHSUMMARY_CLAUSE, 
                    "none_exist_key", "none_exist_key_value"));
    
}

TEST_F(RequestCacheKeyGeneratorTest, testGenerateRequestCacheString) {
    string requestStr1 = "config=cluster:mainse_searcher,no_summary:yes,start:0,hit:20,rerank_size:3000,format:xml,trace:DEBUG,analyzer:taobao_analyzer,batch_score:true,no_tokenize_indexes:index1;index2,optimize_comparator:true,optimize_rank:true,rank_trace:debug&&"
                         "query=('大皂角' OR '皂角') AND auction_type:'b'&&"
                         "kvpairs=_ps:static_trans_score,_ss:ss_score&&"
                         "sort=-RANK;-ss_score&&"
                         "virtual_attribute=DISCNT_T:discnt_t(zk_time,zk_rate);&&"
                         "analyzer=specific_index_analyzer:index1#aliws;index2#singlews,"
                         "no_tokenize_indexes:index3,global_analyzer:alibaba";
    RequestPtr requestPtr1 = qrs::RequestCreator::prepareRequest(requestStr1);
    ASSERT_TRUE(requestPtr1);
    RequestCacheKeyGenerator keyGenerator1(requestPtr1);
    ASSERT_EQ(string("config={[analyzer:alibaba][batch_score:true][debug_query_key:][fetch_summary_type:1][no_tokenize_indexes:index1|index2|index3|][optimize_comparator:true][optimize_rank:true][rank_size:0][rank_trace:debug][rerank_size:3000][specific_index_analyzer:index1#aliws|index2#singlews|][sub_doc:0]};kvpairs={[_ps:static_trans_score][_ss:ss_score]};virtual_attribute={[DISCNT_T:discnt_t(zk_time , zk_rate)]};query={AndQuery:[OrQuery:[TermQuery:[Term:[default0||大皂角|100|]], TermQuery:[Term:[default0||皂角|100|]], ], TermQuery:[Term:[auction_type||b|100|]], ]|};sort={-(RANK);-(ss_score);};"), keyGenerator1.generateRequestCacheString());
}

TEST_F(RequestCacheKeyGeneratorTest, testWithSubDoc) {
    string requestStr1 = "config=cluster:a,sub_doc:no,start:0,hit:20&&query=test";
    string requestStr2 = "config=cluster:a,sub_doc:flat,start:0,hit:20&&query=test";
    string requestStr3 = "config=cluster:a,sub_doc:group,start:0,hit:20&&query=test";

    RequestPtr request1 = qrs::RequestCreator::prepareRequest(requestStr1);
    RequestPtr request2 = qrs::RequestCreator::prepareRequest(requestStr2);
    RequestPtr request3 = qrs::RequestCreator::prepareRequest(requestStr3);

    RequestCacheKeyGenerator g1(request1);
    RequestCacheKeyGenerator g2(request2);
    RequestCacheKeyGenerator g3(request3);

    ASSERT_TRUE(g1.generateRequestCacheKey() != g2.generateRequestCacheKey());
    ASSERT_TRUE(g1.generateRequestCacheKey() != g3.generateRequestCacheKey());
    ASSERT_TRUE(g2.generateRequestCacheKey() != g3.generateRequestCacheKey());

    ASSERT_TRUE(g1.generateRequestCacheString() != g2.generateRequestCacheString());
    ASSERT_TRUE(g1.generateRequestCacheString() != g3.generateRequestCacheString());
    ASSERT_TRUE(g2.generateRequestCacheString() != g3.generateRequestCacheString());
}

TEST_F(RequestCacheKeyGeneratorTest, testFetchSummaryType) {
    string requestStr1 = "config=cluster:a,start:0,hit:20&&query=test";
    string requestStr2 = "config=cluster:a,start:0,hit:20,fetch_summary_type:pk&&query=test";
    RequestPtr request1 = qrs::RequestCreator::prepareRequest(requestStr1);
    RequestPtr request2 = qrs::RequestCreator::prepareRequest(requestStr2);

    RequestCacheKeyGenerator g1(request1);
    RequestCacheKeyGenerator g2(request2);
    ASSERT_TRUE(g1.generateRequestCacheKey() != g2.generateRequestCacheKey());
}

END_HA3_NAMESPACE(common);

