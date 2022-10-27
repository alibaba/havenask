#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/qrs/MatchInfoProcessor.h>
#include <ha3/test/test.h>
#include <ha3/config/HitSummarySchema.h>
#include <suez/turing/expression/util/TableInfoConfigurator.h>
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(config);
using namespace std;
using namespace suez::turing;
BEGIN_HA3_NAMESPACE(qrs);

class MatchInfoProcessorTest : public TESTBASE {

public:
    MatchInfoProcessorTest();
    ~MatchInfoProcessorTest();
public:
    void setUp();
    void tearDown();
protected:
    common::RequestPtr prepareRequest();
    common::RequestPtr prepareRequestWithDebug();
    common::ResultPtr prepareAbnormalResult();
    common::ResultPtr prepareNormalResult();
    common::ResultPtr prepareNoSummaryResult();
    common::ResultPtr prepareNoHitResult();
protected:
    std::string normalResult(const std::string& resStr);
protected:
    config::HitSummarySchemaPtr _hitSummarySchema;
    ClusterTableInfoMapPtr _clusterTableInfoMapPtr;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(qrs, MatchInfoProcessorTest);


MatchInfoProcessorTest::MatchInfoProcessorTest() { 
}

MatchInfoProcessorTest::~MatchInfoProcessorTest() { 
}

void MatchInfoProcessorTest::setUp() { 
    _hitSummarySchema.reset(new HitSummarySchema() );    
    _hitSummarySchema->declareSummaryField("title");
    _hitSummarySchema->declareSummaryField("body");
    string schemaConf = string(TEST_DATA_CONF_PATH_HA3) + "/schemas/test_query_match_schema.json";
    TableInfoConfigurator tableInfoConfigurator;
    TableInfoPtr tableInfoPtr = tableInfoConfigurator.createFromFile(schemaConf);
    ASSERT_TRUE(tableInfoPtr);
    string tableName = tableInfoPtr->getTableName();
    _clusterTableInfoMapPtr.reset(new ClusterTableInfoMap);
    (*_clusterTableInfoMapPtr)["default.default"] = tableInfoPtr;
}

void MatchInfoProcessorTest::tearDown() { 
    _hitSummarySchema.reset();    
}

TEST_F(MatchInfoProcessorTest, testProcessorInit){
    KeyValueMap keyValues;
    keyValues["query_indexs"] = ",a1,a2,,a3,";
    keyValues["summary_fields"] = "title:a1,body:b1";
    MatchInfoProcessor processor(_clusterTableInfoMapPtr);
    ASSERT_TRUE(processor.init(keyValues, NULL));
    ASSERT_EQ(size_t(3), processor._queryIndexs.size());
    ASSERT_TRUE(processor._queryIndexs.count("a1") > 0);
    ASSERT_TRUE(processor._queryIndexs.count("a2") > 0);
    ASSERT_TRUE(processor._queryIndexs.count("a3") > 0);
    ASSERT_EQ(size_t(2), processor._summaryFields.size());
    ASSERT_EQ(string("a1"), processor._summaryFields["title"]);
    ASSERT_EQ(string("b1"), processor._summaryFields["body"]);    
}

TEST_F(MatchInfoProcessorTest, testProcessNormal) {
    RequestPtr requestPtr = prepareRequest();
    ResultPtr resultPtr = prepareNormalResult();
    MatchInfoProcessor processor(_clusterTableInfoMapPtr);
    processor.setTracer(resultPtr->getTracer());
    processor._queryIndexs.insert("abc");
    processor._summaryFields["body"] = "internet_analyzer";
    processor.process(requestPtr, resultPtr);
    processor.fillSummary(requestPtr, resultPtr);
    Tracer *tracer = resultPtr->getTracer();
    ASSERT_TRUE(tracer != NULL);
    string traceInfo = tracer->getTraceInfo();
    traceInfo = normalResult(traceInfo);
    string errorStr = "\"type\":\"TOKENIZE\"";
    size_t pos = traceInfo.find(errorStr);
    ASSERT_TRUE(pos != string::npos);
    errorStr = "\"term\":\"pack_index:ccc\"";
    pos = traceInfo.find(errorStr);
    ASSERT_TRUE(pos != string::npos);
    errorStr = "\"is_tokenize_part\":false";
    pos = traceInfo.find(errorStr);
    ASSERT_TRUE(pos != string::npos);
    errorStr = "\"title\"";
    pos = traceInfo.find(errorStr);
    ASSERT_TRUE(pos == string::npos);
}

TEST_F(MatchInfoProcessorTest, testProcessNormalWithDebug) {
    RequestPtr requestPtr = prepareRequestWithDebug();
    ResultPtr resultPtr = prepareNormalResult();
    MatchInfoProcessor processor(_clusterTableInfoMapPtr);
    processor.setTracer(resultPtr->getTracer());
    processor.process(requestPtr, resultPtr);
    processor.fillSummary(requestPtr, resultPtr);

    Tracer *tracer = resultPtr->getTracer();
    ASSERT_TRUE(tracer != NULL);
    string traceInfo = tracer->getTraceInfo();
    traceInfo = normalResult(traceInfo);
    string errorStr = "\"type\":\"TOKENIZE\"";
    size_t pos = traceInfo.find(errorStr);
    ASSERT_TRUE(pos != string::npos);
    errorStr = "\"term\":\"pack_index:ccc\"";
    pos = traceInfo.find(errorStr);
    ASSERT_TRUE(pos != string::npos);
    errorStr = "\"is_tokenize_part\":true";
    pos = traceInfo.find(errorStr);
    ASSERT_TRUE(pos != string::npos);
}

TEST_F(MatchInfoProcessorTest, testProcessNoSummary) {
    RequestPtr requestPtr = prepareRequest();
    ResultPtr resultPtr = prepareNoSummaryResult();
    MatchInfoProcessor processor(_clusterTableInfoMapPtr);
    processor.setTracer(resultPtr->getTracer());
    processor.process(requestPtr, resultPtr);

    Tracer *tracer = resultPtr->getTracer();
    ASSERT_TRUE(tracer != NULL);
    string traceInfo = tracer->getTraceInfo();
    string errorStr = "###check_result:{";
    size_t pos = traceInfo.find(errorStr);
    ASSERT_TRUE(pos == string::npos);
}

TEST_F(MatchInfoProcessorTest, testProcessAbnormal) {
    RequestPtr requestPtr = prepareRequest();
    ResultPtr resultPtr = prepareAbnormalResult();
    MatchInfoProcessor processor(_clusterTableInfoMapPtr);
    processor.setTracer(resultPtr->getTracer());
    processor.process(requestPtr, resultPtr);
    processor.fillSummary(requestPtr, resultPtr);
    Hits *hits = resultPtr->getHits();
    ASSERT_TRUE(hits == NULL);
    Tracer *tracer = resultPtr->getTracer();
    ASSERT_TRUE(tracer == NULL);
}

TEST_F(MatchInfoProcessorTest, testProcessNoHits) {
    RequestPtr requestPtr = prepareRequest();
    ResultPtr resultPtr = prepareNoHitResult();
    MatchInfoProcessor processor(_clusterTableInfoMapPtr);
    processor.setTracer(resultPtr->getTracer());
    processor.process(requestPtr, resultPtr);
    processor.fillSummary(requestPtr, resultPtr);
    Hits *hits = resultPtr->getHits();
    ASSERT_TRUE(hits != NULL);
    Tracer *tracer = resultPtr->getTracer();
    ASSERT_TRUE(tracer != NULL);
    string traceInfo = tracer->getTraceInfo();
    string errorStr = "###check_result:{";
    size_t pos = traceInfo.find(errorStr);
    ASSERT_TRUE(pos != string::npos);
}

RequestPtr MatchInfoProcessorTest::prepareRequest() {
    RequestPtr requestPtr(new Request());
    ConfigClause *configClause = new ConfigClause();
    configClause->addClusterName("default");
    requestPtr->setConfigClause(configClause);
    RequiredFields requiredField;
    AndQuery *andQuery = new AndQuery("");
    QueryPtr query1(new TermQuery("aaa", 
                    "pack_index", requiredField, ""));
    QueryPtr query2(new TermQuery("bbb", 
                    "pack_index", requiredField, ""));
    QueryPtr query3(new TermQuery("ccc", 
                    "pack_index", requiredField, ""));
    andQuery->addQuery(query1);
    andQuery->addQuery(query2);
    andQuery->addQuery(query3);
    QueryClause *queryClause = new QueryClause(andQuery);
    requestPtr->setQueryClause(queryClause);
    return requestPtr;
}

RequestPtr MatchInfoProcessorTest::prepareRequestWithDebug() {
    RequestPtr requestPtr(new Request());
    ConfigClause *configClause = new ConfigClause();
    configClause->addClusterName("default");
    configClause->addClusterName("default2");
    configClause->setDebugQueryKey("2");
    requestPtr->setConfigClause(configClause);
    RequiredFields requiredField;
    AndQuery *andQuery = new AndQuery("");
    QueryPtr query1(new TermQuery("aaa", 
                    "pack_index", requiredField, ""));
    QueryPtr query2(new TermQuery("bbb", 
                    "pack_index", requiredField, ""));
    QueryPtr query3(new TermQuery("ccc", 
                    "pack_index", requiredField, ""));
    andQuery->addQuery(query1);
    andQuery->addQuery(query2);
    andQuery->addQuery(query3);
    QueryClause *queryClause = new QueryClause(andQuery);
    requestPtr->setQueryClause(queryClause);
    return requestPtr;
}

ResultPtr MatchInfoProcessorTest::prepareNormalResult() {
    ResultPtr resultPtr(new Result());
    Hits *hits = new Hits();
    HitPtr hit(new Hit(10));
    Tracer *hitTracer = new Tracer();
    hitTracer->setLevel("INFO");
    char sep = ':';
    string traceStr = "###||###" + string("pack_index:aaa") + sep + 
                      string("1|||pack_index:bbb") + sep + string("1") + "###||###";
    hitTracer->trace(traceStr);
    hit->setTracer(hitTracer);
    delete hitTracer;

    SummaryHit *summaryHit = new SummaryHit(_hitSummarySchema.get(), NULL);
    summaryHit->setSummaryValue("title","aaa bbb ab c");
    summaryHit->setSummaryValue("body","body field");
    hit->setSummaryHit(summaryHit);
    
    hits->addHit(hit);
    resultPtr->setHits(hits);
    TracerPtr resTracer(new Tracer());
    resTracer->setLevel("INFO");
    resultPtr->setTracer(resTracer);
    return resultPtr;
}

ResultPtr MatchInfoProcessorTest::prepareNoSummaryResult() {
    ResultPtr resultPtr(new Result());
    Hits *hits = new Hits();
    HitPtr hit(new Hit(10));
    char sep = ':';
    Tracer *tracer = new Tracer();
    string traceStr = "###||###" + string("|||default:aaa") + sep + 
                      string("1|||default:bbb") + sep + string("1") + "###||###";
    tracer->trace(traceStr);
    hit->setTracer(tracer);
    delete tracer;

    hits->addHit(hit);
    resultPtr->setHits(hits);
    TracerPtr resTracer(new Tracer());
    resTracer->setLevel("INFO");
    resultPtr->setTracer(resTracer);

    return resultPtr;
}

ResultPtr MatchInfoProcessorTest::prepareNoHitResult() {
    ResultPtr resultPtr(new Result());
    Hits *hits = new Hits();
    resultPtr->setHits(hits);
    TracerPtr resTracer(new Tracer());
    resTracer->setLevel("INFO");
    resultPtr->setTracer(resTracer);
    return resultPtr;
}


ResultPtr MatchInfoProcessorTest::prepareAbnormalResult() {
    ResultPtr resultPtr(new Result());
    return resultPtr;
}

string MatchInfoProcessorTest::normalResult(const string& resStr){
    string normalStr;
    for(size_t i = 0; i < resStr.size(); i++){
        if(resStr[i] != '\n' && resStr[i] != ' '
           &&resStr[i] != '\t'){
            normalStr += resStr[i];
        }
    }
    return normalStr;
}

END_HA3_NAMESPACE(qrs);

