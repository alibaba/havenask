#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/Request.h>
#include <ha3/common/TermQuery.h>
#include <ha3/common/AndQuery.h>
#include <ha3/common/OrQuery.h>
#include <ha3/common/AndNotQuery.h>
#include <ha3/common/RankQuery.h>
#include <ha3/common/PhraseQuery.h>
#include <suez/turing/expression/syntax/AtomicSyntaxExpr.h>
#include <suez/turing/expression/syntax/BinarySyntaxExpr.h>
#include <ha3/isearch.h>
#include <autil/TimeUtility.h>

USE_HA3_NAMESPACE(search);
USE_HA3_NAMESPACE(queryparser);
USE_HA3_NAMESPACE(util);

BEGIN_HA3_NAMESPACE(common);

class SerializeRequestTest : public TESTBASE {
public:
    SerializeRequestTest();
    ~SerializeRequestTest();
public:
    void setUp();
    void tearDown();
protected:
    RequestPtr prepareRequest();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(common, SerializeRequestTest);

using namespace std;
using namespace autil;
using namespace autil::legacy::json;


SerializeRequestTest::SerializeRequestTest() { 
}

SerializeRequestTest::~SerializeRequestTest() { 
}

void SerializeRequestTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void SerializeRequestTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(SerializeRequestTest, testSimpleProcess) { 
    HA3_LOG(DEBUG, "Begin Test!");
    RequestPtr request = prepareRequest();
    
    uint32_t cycle = 1024;
    uint32_t binarySize;
    uint64_t startTime = TimeUtility::currentTime();
    for (size_t i = 0; i < cycle; ++i) {
         string requestString;
         request->serializeToString(requestString);
         binarySize = requestString.size();
         RequestPtr request2(new Request);
         request2->deserializeFromString(requestString);
    }

    uint64_t endTime = TimeUtility::currentTime();
    HA3_LOG(ERROR, "BinarySerialize performance time cost:\n (%lu us), request length (%d)", 
        endTime - startTime, binarySize);

    startTime = TimeUtility::currentTime();
    for (size_t i = 0; i < cycle; ++i) {
        RequestPtr request2(new Request);
        string requestString = ToJsonString(*request);
        binarySize = requestString.size();
        FromJsonString(*request2, requestString);
    }

    endTime = TimeUtility::currentTime();
    HA3_LOG(ERROR, "JsonSerialize performance time cost:\n (%lu us), request length (%d)", 
        endTime - startTime, binarySize);
}

RequestPtr SerializeRequestTest::prepareRequest()
{
    string requestString = "config=cluster:cluster1, start:1, hit:3&&"
                           "query=\"term1 term2\" AND term3 OR term4 ANDNOT term5 RANK term6&&"
                           "filter=a + b > 10&&"
                           "sort=a + b&&"
                           "distinct=dist_key:userid, dist_count:1, dist_times:2&&"
                           "aggregate=groupkey:type, max(price)&&"
                           "rank=rank_profile:rankfile, fieldboost:package_name.a(10)";
    RequestPtr request(new Request);
    request->setOriginalString(requestString);
    request->setPartitionMode("usrid");

    //add config clause
    ConfigClause *configClause = new ConfigClause();
    configClause->addClusterName("cluster1");
    configClause->setStartOffset(1);
    configClause->setHitCount(3);
    request->setConfigClause(configClause);

    //add query clause
    QueryClause *queryClause = new QueryClause;
    Term term1("term1", "index");
    Term term2("term2", "index");
    PhraseQuery *phraseQuery = new PhraseQuery;
    phraseQuery->addTerm(term1);
    phraseQuery->addTerm(term2);
    
    Term term3("term3", "index");
    TermQuery *termQuery1 = new TermQuery(term3);
    AndQuery *andQuery = new AndQuery;
    andQuery->addQuery(QueryPtr(phraseQuery));
    andQuery->addQuery(QueryPtr(termQuery1));

    Term term4("term4", "index");
    TermQuery *termQuery2 = new TermQuery(term4);
    OrQuery *orQuery = new OrQuery;
    orQuery->addQuery(QueryPtr(andQuery));
    orQuery->addQuery(QueryPtr(termQuery2));

    Term term5("term5", "index");
    TermQuery *termQuery3 = new TermQuery(term5);
    AndNotQuery *andNotQuery = new AndNotQuery;
    andNotQuery->addQuery(QueryPtr(orQuery));
    andNotQuery->addQuery(QueryPtr(termQuery3));
    
    Term term6("term6", "index");
    TermQuery *termQuery4 = new TermQuery(term6);
    RankQuery *rankQuery = new RankQuery;
    rankQuery->addQuery(QueryPtr(andNotQuery));
    rankQuery->addQuery(QueryPtr(termQuery4));
    
    queryClause->setRootQuery(rankQuery);
    string queryStr = "query=\"term1 term2\" AND term3 OR term4 ANDNOT term5 RANK term6&&";
    queryClause->setOriginalString(queryStr);
    request->setQueryClause(queryClause);

    //add filter clause
    AtomicSyntaxExpr *atomicSyntaxExpr1
        = new AtomicSyntaxExpr("a", vt_int32, ATTRIBUTE_NAME);
    AtomicSyntaxExpr *atomicSyntaxExpr2
        = new AtomicSyntaxExpr("b", vt_int32, ATTRIBUTE_NAME);
    AtomicSyntaxExpr *atomicSyntaxExpr3
        = new AtomicSyntaxExpr("10", vt_int32, INTEGER_VALUE);

    AddSyntaxExpr *addSyntaxExpr = new AddSyntaxExpr(atomicSyntaxExpr1,
            atomicSyntaxExpr2);
    GreaterSyntaxExpr *greateSyntaxExpr = new GreaterSyntaxExpr(addSyntaxExpr,
            atomicSyntaxExpr3);
            
    FilterClause *filterClause = new FilterClause(greateSyntaxExpr);
    filterClause->setOriginalString("a + b > 10");
    request->setFilterClause(filterClause);

    //add sort clause
    AtomicSyntaxExpr *atomicSyntaxExpr4
        = new AtomicSyntaxExpr("a", vt_int32, ATTRIBUTE_NAME);
    AtomicSyntaxExpr *atomicSyntaxExpr5
        = new AtomicSyntaxExpr("b", vt_int32, ATTRIBUTE_NAME);
    AddSyntaxExpr *addSyntaxExpr2 = new AddSyntaxExpr(atomicSyntaxExpr4,
            atomicSyntaxExpr5);

    SortDescription *sortDescription = new SortDescription("type");
    sortDescription->setRankFlag(false);
    sortDescription->setRootSyntaxExpr(addSyntaxExpr2);
    SortClause *sortClause = new  SortClause;
    sortClause->addSortDescription(sortDescription);
    sortClause->setOriginalString("a + b");
    request->setSortClause(sortClause);

    //add distinct clause
    SyntaxExpr *distExpr = new AtomicSyntaxExpr("userid", vt_int32, ATTRIBUTE_NAME);
    DistinctClause *distinctClause = new DistinctClause(DEFAULT_DISTINCT_MODULE_NAME,
            "distinct=dist_key:userid,dist_count=1,dist_times=1", 1, 1, false, distExpr, NULL);
    request->setDistinctClause(distinctClause);

    //add aggregation clause
    AggregateDescription *aggDescription 
        = new AggregateDescription("groupkey:type, max(price)");
    AggregateClause *aggClause = new AggregateClause;
    aggClause->addAggDescription(aggDescription);
    aggClause->setOriginalString("aggregate=groupkey:type, max(price)");
    request->setAggregateClause(aggClause);

    //add rank clause
    RankClause *rankClause = new RankClause;
    rankClause->setRankProfileName("rank.conf");
    rankClause->setOriginalString("rank=rank.conf");
    request->setRankClause(rankClause);
    return request;
}

END_HA3_NAMESPACE(common);

