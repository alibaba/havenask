#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <suez/turing/expression/util/TableInfo.h>
#include <ha3/common/Request.h>
#include <ha3/common/Result.h>
#include <ha3/qrs/RequestValidator.h>
#include <ha3/qrs/QueryTokenizer.h>
#include <ha3/qrs/RequestValidateProcessor.h>
#include <build_service/analyzer/AnalyzerFactory.h>
#include <ha3/config/ClusterConfigInfo.h>
#include <ha3_sdk/testlib/qrs/RequestCreator.h>
#include <autil/StringUtil.h>
#include <ha3/common/ErrorDefine.h>
#include <ha3/qrs/RequestValidateProcessor.h>
#include <ha3/qrs/RequestValidator.h>
#include <ha3/qrs/test/ExceptionQrsProcessor.h>
#include <ha3/monitor/SessionMetricsCollector.h>
#include <build_service/analyzer/AnalyzerFactory.h>
#include <build_service/analyzer/AnalyzerInfos.h>
#include <ha3/test/test.h>
#include <ha3/common/TermQuery.h>
#include <ha3/queryparser/RequestParser.h>
#include <build_service/analyzer/TokenizerManager.h>
using namespace std;
using namespace autil;
using namespace suez::turing;

USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(monitor);
USE_HA3_NAMESPACE(common);
using namespace build_service::analyzer;
USE_HA3_NAMESPACE(util);

BEGIN_HA3_NAMESPACE(qrs);

class RequestValidateProcessorTest : public TESTBASE {
public:
    RequestValidateProcessorTest();
    ~RequestValidateProcessorTest();
public:
    static build_service::analyzer::AnalyzerFactoryPtr initFactory();
public:
    void setUp();
    void tearDown();
protected:
    common::RequestPtr prepareCorrectRequest();
    common::RequestPtr prepareErrorRequest();
    common::RequestPtr prepareRequest(const std::string &word1 = "stopword1",
            const std::string &word2 = "stopword2");

    TableInfo* createFakeTableInfo(const std::string& tableName);
    TableInfo* createFakeAuxTableInfo(const std::string& tableName);
    IndexInfos* createIndexInfos(
            const std::vector<std::string> &indexNames,
            const std::vector<std::string> &analyzerNames);
    AttributeInfos* createAttributeInfos(
            const std::vector<std::string> &attrNames,
            const std::vector<bool> &mutiFlags,
            const std::vector<FieldType> &fieldTypes);

    bool validateFetchSummaryCluster(common::ConfigClause* configClause,
            const ClusterTableInfoMapPtr &clusterTableInfoMapPtr);
    bool validateMultiCluster(common::ConfigClause* configClause,
                              common::LevelClause *levelClause,
                              const ClusterTableInfoMapPtr &clusterTableInfoMapPtr);

    common::ConfigClause createConfigClause(const std::string& oriString);
    ClusterTableInfoMapPtr createClusterTableInfoMapPtr(
            const std::string &indexStr, const std::string &clusterStr);
protected:
    RequestValidatorPtr _requestValidatorPtr;
    RequestValidateProcessorPtr _validateProcessorPtr;
    std::map<std::string, TableInfoPtr> _tableInfoMap;
    config::ClusterConfigMapPtr _clusterConfigMapPtr;
    common::ResultPtr _result;
protected:
    static build_service::analyzer::AnalyzerFactoryPtr _analyzerFactoryPtr;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(qrs, RequestValidateProcessorTest);


#define RVP_INNER_CHECK_AND_RESET_ERROR(errorCode)                      \
    common::MultiErrorResultPtr errorResult = _result->getMultiErrorResult(); \
    size_t errorCount = errorResult->getErrorCount();                   \
    ASSERT_EQ((size_t)1, errorCount);                        \
    ErrorResult error = errorResult->getErrorResults().back();          \
    errorResult->resetErrors();                                         \
    ASSERT_EQ(errorCode, error.getErrorCode())

#define RVP_CHECK_AND_RESET_ERROR(errorCode)            \
    {                                                   \
        RVP_INNER_CHECK_AND_RESET_ERROR(errorCode);     \
    }

#define RVP_CHECK_AND_RESET_ERROR_WITH_MESSAGE(errorCode, errorMsg)     \
    {                                                                   \
        RVP_INNER_CHECK_AND_RESET_ERROR(errorCode);                     \
        ASSERT_EQ(errorMsg, error.getErrorMsg());            \
    }

#define RVP_CHECK_NO_ERROR()                    \
    ASSERT_TRUE(!_result->hasError()) << _result->getMultiErrorResult()->toDebugString()

#define CREATE_CONFIG_CLAUSE(oriString)                                 \
    queryparser::RequestParser parser;                                  \
    common::RequestPtr request(new Request);                            \
    request->setOriginalString(oriString);                              \
    ASSERT_TRUE(parser.parseConfigClause(request));                  \
    common::ConfigClause* configClause = request->getConfigClause()

#define RVP_CHECK_FETCHSUMMARY_ERROR(tableStr, clusterStr, configStr, errorCode) \
    do {                                                                \
        ClusterTableInfoMapPtr clusterTableInfoMapPtr = createClusterTableInfoMapPtr(tableStr, clusterStr); \
        CREATE_CONFIG_CLAUSE(configStr);                                \
        ASSERT_TRUE(!validateFetchSummaryCluster(configClause, clusterTableInfoMapPtr)); \
        RVP_CHECK_AND_RESET_ERROR(errorCode);                           \
    } while (0)

#define RVP_CHECK_FETCHSUMMARY_INFO(tableStr, clusterStr, configStr, flags) \
    do {                                                                \
        ClusterTableInfoMapPtr clusterTableInfoMapPtr = createClusterTableInfoMapPtr(tableStr, clusterStr); \
        CREATE_CONFIG_CLAUSE(configStr);                                \
        ASSERT_TRUE(validateFetchSummaryCluster(configClause, clusterTableInfoMapPtr)); \
        RVP_CHECK_NO_ERROR();                                           \
        ASSERT_EQ((uint32_t)(flags),                         \
                             (uint32_t)configClause->getPhaseOneBasicInfoMask()); \
    } while (0)


//static (init 'AliWS' take too much time)
build_service::analyzer::AnalyzerFactoryPtr RequestValidateProcessorTest::_analyzerFactoryPtr =
    RequestValidateProcessorTest::initFactory();


RequestValidateProcessorTest::RequestValidateProcessorTest() {
}

RequestValidateProcessorTest::~RequestValidateProcessorTest() {
}

void RequestValidateProcessorTest::setUp() {
    HA3_LOG(DEBUG, "setUp!");

    TableInfoPtr tableInfoPtr(createFakeTableInfo("table1"));
    ClusterTableInfoMapPtr clusterTableInfoMapPtr(new ClusterTableInfoMap);
    (*clusterTableInfoMapPtr)["cluster1.default"] = tableInfoPtr;

    TableInfoPtr auxTableInfoPtr(createFakeAuxTableInfo("aux_table"));
    (*clusterTableInfoMapPtr)["aux_cluster.default"] = auxTableInfoPtr;

    ClusterConfigInfo clusterConfigInfo;
    clusterConfigInfo._tableName = "table1";
    clusterConfigInfo._joinConfig.setScanJoinCluster("aux_cluster");
    _clusterConfigMapPtr.reset(new ClusterConfigMap);
    _clusterConfigMapPtr->insert(make_pair("cluster1.default", clusterConfigInfo));
    _clusterConfigMapPtr->insert(make_pair("cluster2.default", clusterConfigInfo));
    _clusterConfigMapPtr->insert(make_pair("cluster3.default", clusterConfigInfo));

    ClusterConfigInfo auxClusterConfigInfo;
    clusterConfigInfo._tableName = "aux_table";
    _clusterConfigMapPtr->insert(make_pair("aux_cluster.default", auxClusterConfigInfo));

    _requestValidatorPtr.reset(new RequestValidator(clusterTableInfoMapPtr, QRS_RETURN_HITS_LIMIT,
                    _clusterConfigMapPtr, ClusterFuncMapPtr(), CavaPluginManagerMapPtr(),
                    suez::turing::ClusterSorterNamesPtr()));
    QueryTokenizer queryTokenizer(_analyzerFactoryPtr.get());
    _validateProcessorPtr.reset(new RequestValidateProcessor(clusterTableInfoMapPtr,
                    _clusterConfigMapPtr, _requestValidatorPtr, queryTokenizer));
    SessionMetricsCollectorPtr metricsCollectorPtr(new SessionMetricsCollector);
    _validateProcessorPtr->setSessionMetricsCollector(metricsCollectorPtr);
    _result.reset(new Result);
}

void RequestValidateProcessorTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");
    _validateProcessorPtr.reset();
}

TEST_F(RequestValidateProcessorTest, testSimpleProcess) {
    HA3_LOG(DEBUG, "Begin Test!");

    RequestPtr requestPtr = prepareCorrectRequest();

    _validateProcessorPtr->process(requestPtr, _result);
    RVP_CHECK_NO_ERROR();
}

TEST_F(RequestValidateProcessorTest, testErrorRequest) {
    HA3_LOG(DEBUG, "Begin Test!");

    RequestPtr requestPtr = prepareErrorRequest();

    _validateProcessorPtr->process(requestPtr, _result);
    RVP_CHECK_AND_RESET_ERROR(ERROR_NO_CONFIG_CLAUSE);
}

TEST_F(RequestValidateProcessorTest, testTokenizeWithDefaultOperator_AND) {
    HA3_LOG(DEBUG, "Begin Test!");

    RequestPtr requestPtr = prepareCorrectRequest();

    QueryInfo queryInfo("default", OP_AND);
    (*_clusterConfigMapPtr)["cluster1"].setQueryInfo(queryInfo);

    ASSERT_TRUE(_validateProcessorPtr->validateRequest(requestPtr, _result));
    RVP_CHECK_NO_ERROR();

    QueryClause *queryClause = requestPtr->getQueryClause();
    ASSERT_TRUE(queryClause);
    Query *query = queryClause->getRootQuery();
    ASSERT_TRUE(query);
    ASSERT_EQ(string("AndQuery"), query->getQueryName());
    const std::vector<QueryPtr>* children = query->getChildQuery();
    ASSERT_TRUE(children);
    ASSERT_EQ((size_t)2, children->size());
}

TEST_F(RequestValidateProcessorTest, testTokenizeWithDefaultOperator_AND_withAuxQuery) {
    HA3_LOG(DEBUG, "Begin Test!");

    RequestPtr requestPtr = prepareCorrectRequest();

    QueryInfo queryInfo("default", OP_AND);
    (*_clusterConfigMapPtr)["cluster1"].setQueryInfo(queryInfo);

    ASSERT_TRUE(_validateProcessorPtr->validateRequest(requestPtr, _result));
    RVP_CHECK_NO_ERROR();

    {
        QueryClause *queryClause = requestPtr->getQueryClause();
        ASSERT_TRUE(queryClause);
        Query *query = queryClause->getRootQuery();
        ASSERT_TRUE(query);
        ASSERT_EQ(string("AndQuery"), query->getQueryName());
        const std::vector<QueryPtr>* children = query->getChildQuery();
        ASSERT_TRUE(children);
        ASSERT_EQ((size_t)2, children->size());
    }

    {
        AuxQueryClause *auxQueryClause = requestPtr->getAuxQueryClause();
        ASSERT_TRUE(auxQueryClause);
        Query *query = auxQueryClause->getRootQuery();
        ASSERT_TRUE(query);
        ASSERT_EQ(string("AndQuery"), query->getQueryName());
        const std::vector<QueryPtr>* children = query->getChildQuery();
        ASSERT_TRUE(children);
        ASSERT_EQ((size_t)2, children->size());
        ASSERT_EQ(string("TermQuery:[Term:[aux_default||ccc|100|]]"), (*children)[0]->toString());
        ASSERT_EQ(string("TermQuery:[Term:[aux_default||ddd|100|]]"), (*children)[1]->toString());
    }
}

TEST_F(RequestValidateProcessorTest, testTokenizeWithDefaultOperator_AND2) {
    HA3_LOG(DEBUG, "Begin Test!");

    RequestPtr requestPtr = prepareCorrectRequest();
    ConfigClause *configClause = requestPtr->getConfigClause();
    assert(configClause);
    configClause->setDefaultOP("AND");

    QueryInfo queryInfo("default", OP_OR);
    (*_clusterConfigMapPtr)["cluster1"].setQueryInfo(queryInfo);

    ASSERT_TRUE(_validateProcessorPtr->validateRequest(requestPtr, _result));
    RVP_CHECK_NO_ERROR();

    QueryClause *queryClause = requestPtr->getQueryClause();
    ASSERT_TRUE(queryClause);
    Query *query = queryClause->getRootQuery();
    ASSERT_TRUE(query);
    ASSERT_EQ(string("AndQuery"), query->getQueryName());
    const std::vector<QueryPtr>* children = query->getChildQuery();
    ASSERT_TRUE(children);
    ASSERT_EQ((size_t)2, children->size());

}

TEST_F(RequestValidateProcessorTest, testTokenizeWithDefaultOperator_OR) {
    HA3_LOG(DEBUG, "Begin Test!");

    RequestPtr requestPtr = prepareCorrectRequest();

    QueryInfo queryInfo("default", OP_OR);
    (*_clusterConfigMapPtr)["cluster1.default"].setQueryInfo(queryInfo);

    ASSERT_TRUE(_validateProcessorPtr->validateRequest(requestPtr, _result));
    RVP_CHECK_NO_ERROR();

    QueryClause *queryClause = requestPtr->getQueryClause();
    ASSERT_TRUE(queryClause);
    Query *query = queryClause->getRootQuery();
    ASSERT_TRUE(query);
    ASSERT_EQ(string("OrQuery"), query->getQueryName());
    const std::vector<QueryPtr>* children = query->getChildQuery();
    ASSERT_TRUE(children);
    ASSERT_EQ((size_t)2, children->size());
}

TEST_F(RequestValidateProcessorTest, testTokenizeWithDefaultOperator_OR_withAuxQuery) {
    HA3_LOG(DEBUG, "Begin Test!");

    RequestPtr requestPtr = prepareCorrectRequest();

    QueryInfo queryInfo("default", OP_OR);
    (*_clusterConfigMapPtr)["cluster1.default"].setQueryInfo(queryInfo);

    ASSERT_TRUE(_validateProcessorPtr->validateRequest(requestPtr, _result));
    RVP_CHECK_NO_ERROR();

    {
        QueryClause *queryClause = requestPtr->getQueryClause();
        ASSERT_TRUE(queryClause);
        Query *query = queryClause->getRootQuery();
        ASSERT_TRUE(query);
        ASSERT_EQ(string("OrQuery"), query->getQueryName());
        const std::vector<QueryPtr>* children = query->getChildQuery();
        ASSERT_TRUE(children);
        ASSERT_EQ((size_t)2, children->size());
    }

    {
        AuxQueryClause *auxQueryClause = requestPtr->getAuxQueryClause();
        ASSERT_TRUE(auxQueryClause);
        Query *query = auxQueryClause->getRootQuery();
        ASSERT_TRUE(query);
        ASSERT_EQ(string("AndQuery"), query->getQueryName());
        const std::vector<QueryPtr>* children = query->getChildQuery();
        ASSERT_TRUE(children);
        ASSERT_EQ((size_t)2, children->size());
        ASSERT_EQ(string("TermQuery:[Term:[aux_default||ccc|100|]]"), (*children)[0]->toString());
        ASSERT_EQ(string("TermQuery:[Term:[aux_default||ddd|100|]]"), (*children)[1]->toString());
    }
}

TEST_F(RequestValidateProcessorTest, testTokenizeWithDefaultOperator_OR2) {
    HA3_LOG(DEBUG, "Begin Test!");

    RequestPtr requestPtr = prepareCorrectRequest();
    ConfigClause *configClause = requestPtr->getConfigClause();
    assert(configClause);
    configClause->setDefaultOP("OR");

    QueryInfo queryInfo("default", OP_AND);
    (*_clusterConfigMapPtr)["cluster1"].setQueryInfo(queryInfo);

    ASSERT_TRUE(_validateProcessorPtr->validateRequest(requestPtr, _result));
    RVP_CHECK_NO_ERROR();

    QueryClause *queryClause = requestPtr->getQueryClause();
    ASSERT_TRUE(queryClause);
    Query *query = queryClause->getRootQuery();
    ASSERT_TRUE(query);
    ASSERT_EQ(string("OrQuery"), query->getQueryName());
    const std::vector<QueryPtr>* children = query->getChildQuery();
    ASSERT_TRUE(children);
    ASSERT_EQ((size_t)2, children->size());
}

TEST_F(RequestValidateProcessorTest, testValidateStringIndex) {
    HA3_LOG(DEBUG, "Begin Test!");

    RequestPtr requestPtr = prepareCorrectRequest();
    RequiredFields requiredField;
    Query *inputQuery = new TermQuery("aaa bbb", "stringindex", requiredField, "");
    requestPtr->getQueryClause()->setRootQuery(inputQuery);


    Query *inputAuxQuery = new TermQuery("ccc ddd", "aux_stringindex",
            requiredField, "");
    requestPtr->getAuxQueryClause()->setRootQuery(inputAuxQuery);

    ASSERT_TRUE(_validateProcessorPtr->validateRequest(requestPtr, _result));
    RVP_CHECK_NO_ERROR();

    {
        QueryClause *queryClause = requestPtr->getQueryClause();
        ASSERT_TRUE(queryClause);
        Query *query = queryClause->getRootQuery();
        ASSERT_TRUE(query);
        ASSERT_EQ(string("TermQuery"), query->getQueryName());
        TermQuery *checkQuery = dynamic_cast<TermQuery*>(query);
        ASSERT_TRUE(checkQuery);
        const Term& checkTerm = checkQuery->getTerm();
        ASSERT_EQ(string("aaa bbb"), checkTerm.getWord());
        ASSERT_EQ(string("stringindex"), checkTerm.getIndexName());
    }

    {
        QueryClause *queryClause = requestPtr->getAuxQueryClause();
        ASSERT_TRUE(queryClause);
        Query *query = queryClause->getRootQuery();
        ASSERT_TRUE(query);
        ASSERT_EQ(string("TermQuery"), query->getQueryName());
        TermQuery *checkQuery = dynamic_cast<TermQuery*>(query);
        ASSERT_TRUE(checkQuery);
        const Term& checkTerm = checkQuery->getTerm();
        ASSERT_EQ(string("ccc ddd"), checkTerm.getWord());
        ASSERT_EQ(string("aux_stringindex"), checkTerm.getIndexName());
    }
}

TEST_F(RequestValidateProcessorTest, testValidateMultiClusterWithOnlyOneCluster) {
    ConfigClause configClause;
    configClause.addClusterName("cluster1");

    ClusterTableInfoMapPtr clusterTableInfoMapPtr = createClusterTableInfoMapPtr("index1", "cluster1.default");

    ASSERT_TRUE(validateMultiCluster(&configClause, NULL, clusterTableInfoMapPtr));
    RVP_CHECK_NO_ERROR();
}

TEST_F(RequestValidateProcessorTest, testValidateMultiClusterCompatible) {
    ClusterTableInfoMapPtr clusterTableInfoMapPtr(new ClusterTableInfoMap);

    ConfigClause configClause;
    configClause.addClusterName("cluster1");
    configClause.addClusterName("cluster2");


    TableInfoPtr tableInfoPtr1(new TableInfo);
    tableInfoPtr1->setTableName("table1");
    IndexInfos* indexInfos1 = createIndexInfos(
            StringUtil::split("index1,index2", ","),
            StringUtil::split("analyzer1,analyzer2", ","));
    tableInfoPtr1->setIndexInfos(indexInfos1);
    vector<bool> multiFlag1;
    multiFlag1.push_back(false);
    multiFlag1.push_back(true);
    vector<FieldType> fieldTypes1;
    fieldTypes1.push_back(ft_string);
    fieldTypes1.push_back(ft_uint32);
    AttributeInfos* attrInfos1 = createAttributeInfos(
            StringUtil::split("a1,a2", ","), multiFlag1, fieldTypes1);
    tableInfoPtr1->setAttributeInfos(attrInfos1);
    (*clusterTableInfoMapPtr)["cluster1.default"] = tableInfoPtr1;

    TableInfoPtr tableInfoPtr2(new TableInfo);
    tableInfoPtr2->setTableName("table2");
    IndexInfos* indexInfos2 = createIndexInfos(
            StringUtil::split("index2,index3", ","),
            StringUtil::split("analyzer2,analyzer3", ","));
    tableInfoPtr2->setIndexInfos(indexInfos2);

    vector<bool> multiFlag2;
    multiFlag2.push_back(false);
    multiFlag2.push_back(false);
    vector<FieldType> fieldTypes2;
    fieldTypes2.push_back(ft_string);
    fieldTypes2.push_back(ft_uint64);
    AttributeInfos* attrInfos2 = createAttributeInfos(
            StringUtil::split("a1,a3", ","), multiFlag2, fieldTypes2);
    tableInfoPtr2->setAttributeInfos(attrInfos2);
    (*clusterTableInfoMapPtr)["cluster2.default"] = tableInfoPtr2;

    ASSERT_TRUE(validateMultiCluster(&configClause, NULL, clusterTableInfoMapPtr));
    RVP_CHECK_NO_ERROR();
}

TEST_F(RequestValidateProcessorTest, testValidateMultiClusterIndexIncompatible) {
    ClusterTableInfoMapPtr clusterTableInfoMapPtr(new ClusterTableInfoMap);

    ConfigClause configClause;
    configClause.addClusterName("cluster1");
    configClause.addClusterName("cluster2");

    TableInfoPtr tableInfoPtr1(new TableInfo);
    tableInfoPtr1->setTableName("table1");
    IndexInfos* indexInfos1 = createIndexInfos(
            StringUtil::split("index1,index2", ","),
            StringUtil::split("analyzer1,analyzer2", ","));
    tableInfoPtr1->setIndexInfos(indexInfos1);
    vector<bool> multiFlag1;
    multiFlag1.push_back(false);
    multiFlag1.push_back(true);
    vector<FieldType> fieldTypes1;
    fieldTypes1.push_back(ft_string);
    fieldTypes1.push_back(ft_uint32);
    AttributeInfos* attrInfos1 = createAttributeInfos(
            StringUtil::split("a1,a2", ","), multiFlag1, fieldTypes1);
    tableInfoPtr1->setAttributeInfos(attrInfos1);
    (*clusterTableInfoMapPtr)["cluster1.default"] = tableInfoPtr1;
    TableInfoPtr tableInfoPtr2(new TableInfo);
    tableInfoPtr2->setTableName("table2");
    IndexInfos* indexInfos2 = createIndexInfos(
            StringUtil::split("index2,index3", ","),
            StringUtil::split("analyzer4,analyzer3", ","));
    tableInfoPtr2->setIndexInfos(indexInfos2);
    vector<bool> multiFlag2;
    multiFlag2.push_back(false);
    multiFlag2.push_back(true);
    vector<FieldType> fieldTypes2;
    fieldTypes2.push_back(ft_string);
    fieldTypes2.push_back(ft_uint32);
    AttributeInfos* attrInfos2 = createAttributeInfos(
            StringUtil::split("a1,a3", ","), multiFlag2, fieldTypes2);
    tableInfoPtr2->setAttributeInfos(attrInfos2);
    (*clusterTableInfoMapPtr)["cluster2.default"] = tableInfoPtr2;

    ASSERT_TRUE(!validateMultiCluster(&configClause, NULL, clusterTableInfoMapPtr));
    RVP_CHECK_AND_RESET_ERROR(ERROR_MULTI_CLUSTERNAME_WITH_DIFFERENT_TABLE);
}

TEST_F(RequestValidateProcessorTest, testValidateMultiClusterAttributeIncompatible) {
    ClusterTableInfoMapPtr clusterTableInfoMapPtr(new ClusterTableInfoMap);

    ConfigClause configClause;
    configClause.addClusterName("cluster1");
    configClause.addClusterName("cluster2");

    TableInfoPtr tableInfoPtr1(new TableInfo);
    tableInfoPtr1->setTableName("table1");
    IndexInfos* indexInfos1 = createIndexInfos(
            StringUtil::split("index1,index2", ","),
            StringUtil::split("analyzer1,analyzer2", ","));
    tableInfoPtr1->setIndexInfos(indexInfos1);

    vector<bool> multiFlag1;
    multiFlag1.push_back(false);
    multiFlag1.push_back(true);
    vector<FieldType> fieldTypes1;
    fieldTypes1.push_back(ft_string);
    fieldTypes1.push_back(ft_uint32);
    AttributeInfos* attrInfos1 = createAttributeInfos(
            StringUtil::split("a1,a2", ","), multiFlag1, fieldTypes1);
    tableInfoPtr1->setAttributeInfos(attrInfos1);
    (*clusterTableInfoMapPtr)["cluster1.default"] = tableInfoPtr1;

    TableInfoPtr tableInfoPtr2(new TableInfo);
    tableInfoPtr2->setTableName("table2");
    IndexInfos* indexInfos2 = createIndexInfos(
            StringUtil::split("index2,index3", ","),
            StringUtil::split("analyzer2,analyzer3", ","));
    tableInfoPtr2->setIndexInfos(indexInfos2);

    vector<bool> multiFlag2;
    multiFlag2.push_back(false);
    multiFlag2.push_back(true);
    vector<FieldType> fieldTypes2;
    fieldTypes2.push_back(ft_string);
    fieldTypes2.push_back(ft_uint64);
    AttributeInfos* attrInfos2 = createAttributeInfos(
            StringUtil::split("a1,a2", ","), multiFlag2, fieldTypes2);
    tableInfoPtr2->setAttributeInfos(attrInfos2);
    (*clusterTableInfoMapPtr)["cluster2.default"] = tableInfoPtr2;

    ASSERT_TRUE(!validateMultiCluster(&configClause, NULL, clusterTableInfoMapPtr));
    RVP_CHECK_AND_RESET_ERROR(ERROR_MULTI_CLUSTERNAME_WITH_DIFFERENT_TABLE);
}

TEST_F(RequestValidateProcessorTest, testValidateMultiClusterWithLevelClause) {
    HA3_LOG(DEBUG, "Begin Test!");
    ClusterTableInfoMapPtr clusterTableInfoMapPtr(new ClusterTableInfoMap);

    ConfigClause configClause;
    configClause.addClusterName("cluster1");

    TableInfoPtr tableInfoPtr1(new TableInfo);
    tableInfoPtr1->setTableName("table1");
    IndexInfos* indexInfos1 = createIndexInfos(
            StringUtil::split("index1,index2", ","),
            StringUtil::split("analyzer1,analyzer2", ","));
    tableInfoPtr1->setIndexInfos(indexInfos1);

    vector<bool> multiFlag1;
    multiFlag1.push_back(false);
    multiFlag1.push_back(true);
    vector<FieldType> fieldTypes1;
    fieldTypes1.push_back(ft_string);
    fieldTypes1.push_back(ft_uint32);
    AttributeInfos* attrInfos1 = createAttributeInfos(
            StringUtil::split("a1,a2", ","), multiFlag1, fieldTypes1);
    tableInfoPtr1->setAttributeInfos(attrInfos1);
    (*clusterTableInfoMapPtr)["cluster1.default"] = tableInfoPtr1;

    TableInfoPtr tableInfoPtr2(new TableInfo);
    tableInfoPtr2->setTableName("table2");
    IndexInfos* indexInfos2 = createIndexInfos(
            StringUtil::split("index2,index3", ","),
            StringUtil::split("analyzer2,analyzer3", ","));
    tableInfoPtr2->setIndexInfos(indexInfos2);

    vector<bool> multiFlag2;
    multiFlag2.push_back(false);
    multiFlag2.push_back(true);
    vector<FieldType> fieldTypes2;
    fieldTypes2.push_back(ft_string);
    fieldTypes2.push_back(ft_uint64);
    AttributeInfos* attrInfos2 = createAttributeInfos(
            StringUtil::split("a1,a2", ","), multiFlag2, fieldTypes2);
    tableInfoPtr2->setAttributeInfos(attrInfos2);
    (*clusterTableInfoMapPtr)["cluster2.default"] = tableInfoPtr2;

    LevelClausePtr levelClausePtr(new LevelClause);
    levelClausePtr->setLevelQueryType(BOTH_LEVEL);
    levelClausePtr->setSecondLevelCluster("cluster2");

    ASSERT_TRUE(!validateMultiCluster(&configClause, levelClausePtr.get(), clusterTableInfoMapPtr));
    RVP_CHECK_AND_RESET_ERROR(ERROR_MULTI_CLUSTERNAME_WITH_DIFFERENT_TABLE);
}


TEST_F(RequestValidateProcessorTest, testValidateFetchSummaryClusterNotByRawpkDiffCluster) {
    // docid, diff cluster not supported
    RVP_CHECK_FETCHSUMMARY_ERROR("pk64index1,pk64index2",
                                 "cluster1.default,cluster2.default",
                                "config=cluster:cluster1,fetch_summary_cluster:cluster2,"
                                "fetch_summary_type:docid",
                                 ERROR_ONLY_SUPPORT_FETCH_SUMMARY_BY_RAWPK);

    RVP_CHECK_FETCHSUMMARY_ERROR("pk64index1,pk64index2",
                                 "cluster1.default,cluster2.default",
                                "config=cluster:cluster1,fetch_summary_cluster:cluster2,"
                                "fetch_summary_type:pk",
                                 ERROR_ONLY_SUPPORT_FETCH_SUMMARY_BY_RAWPK);
}

TEST_F(RequestValidateProcessorTest, testValidateFetchSummaryClusterFillPhaseOneInfo) {
    // both pk64, no dedup
    RVP_CHECK_FETCHSUMMARY_INFO("pk64index1,pk64index1",
                                "cluster1.default,summary_cluster.default",
                                "config=cluster:cluster1,dedup:no,"
                                "fetch_summary_type:pk",
                                pob_primarykey64_flag | pob_ip_flag);

    // both pk128
    RVP_CHECK_FETCHSUMMARY_INFO("pk128index1,pk128index1",
                                "cluster1.default,summary_cluster.default",
                                "config=cluster:cluster1,"
                                "fetch_summary_type:pk",
                                pob_fullversion_flag | pob_primarykey128_flag | pob_ip_flag);

    // pk64, explicit dedup
    RVP_CHECK_FETCHSUMMARY_INFO("pk64index1,pk64index1",
                                "cluster1.default,summary_cluster.default",
                                "config=cluster:cluster1,dedup:yes,"
                                "fetch_summary_type:pk",
                                pob_primarykey64_flag | pob_fullversion_flag | pob_ip_flag);

    // by default, fetch_summary_cluster is empty
    RVP_CHECK_FETCHSUMMARY_INFO("pk128index1,pk128index1",
                                "cluster1.default,cluster2.default",
                                "config=cluster:cluster1,fetch_summary_type:pk",
                                pob_ip_flag | pob_fullversion_flag | pob_primarykey128_flag);

    // docid, same cluster
    RVP_CHECK_FETCHSUMMARY_INFO("pk64index1,pk64index2",
                                "cluster1.default,cluster2.default",
                                "config=cluster:cluster1,"
                                "fetch_summary_type:docid",
                                pob_ip_flag | pob_fullversion_flag | pob_indexversion_flag);

    // docid, same cluster
    RVP_CHECK_FETCHSUMMARY_INFO("index1,index2",
                                "cluster1.default,cluster2.default",
                                "config=cluster:cluster1,"
                                "fetch_summary_type:docid",
                                pob_ip_flag | pob_fullversion_flag | pob_indexversion_flag);

    // raw, same cluster
    RVP_CHECK_FETCHSUMMARY_INFO("pk64index1,pk64index2",
                                "cluster1.default,cluster2.default",
                                "config=cluster:cluster1,"
                                "fetch_summary_type:rawpk",
                                pob_ip_flag | pob_fullversion_flag | pob_rawpk_flag);

    // raw, same cluster
    RVP_CHECK_FETCHSUMMARY_INFO("pk64index1,pk64index2",
                                "cluster1.default,cluster2.default",
                                "config=cluster:cluster1,dedup:no,"
                                "fetch_summary_type:rawpk",
                                pob_ip_flag | pob_rawpk_flag);

    // raw, diff cluster
    RVP_CHECK_FETCHSUMMARY_INFO("pk64index1,pk64index2",
                                "cluster1.default,cluster2.default",
                                "config=cluster:cluster1,"
                                "fetch_summary_cluster:cluster2,"
                                "fetch_summary_type:rawpk",
                                pob_rawpk_flag | pob_fullversion_flag);
}

TEST_F(RequestValidateProcessorTest, testProcessWithStopWord1) {
    HA3_LOG(DEBUG, "Begin Test!");
    RequestPtr requestPtr(new Request());

    ConfigClause *configClause = new ConfigClause();
    configClause->addClusterName("cluster1");
    requestPtr->setConfigClause(configClause);
    {
        AndQuery *andQuery = new AndQuery("");
        RequiredFields requiredField;
        QueryPtr queryPtr1(new TermQuery("aaa bbb", "default", requiredField, ""));
        OrQuery *orQuery = new OrQuery("");
        QueryPtr queryPtr2;
        QueryPtr queryPtr21(new TermQuery("stopword1", "default", requiredField, ""));
        QueryPtr queryPtr22(new TermQuery("stopword2", "default", requiredField, ""));
        orQuery->addQuery(queryPtr21);
        orQuery->addQuery(queryPtr22);
        queryPtr2.reset(orQuery);
        andQuery->addQuery(queryPtr1);
        andQuery->addQuery(queryPtr2);
        QueryClause *queryClause = new QueryClause(andQuery);
        requestPtr->setQueryClause(queryClause);
    }

    {
        AndQuery *andQuery = new AndQuery("");
        RequiredFields requiredField;
        QueryPtr queryPtr1(new TermQuery("aaa bbb", "aux_default", requiredField, ""));
        OrQuery *orQuery = new OrQuery("");
        QueryPtr queryPtr2;
        QueryPtr queryPtr21(new TermQuery("stopword1", "aux_default", requiredField, ""));
        QueryPtr queryPtr22(new TermQuery("stopword2", "aux_default", requiredField, ""));
        orQuery->addQuery(queryPtr21);
        orQuery->addQuery(queryPtr22);
        queryPtr2.reset(orQuery);
        andQuery->addQuery(queryPtr1);
        andQuery->addQuery(queryPtr2);
        AuxQueryClause *queryClause = new AuxQueryClause(andQuery);
        requestPtr->setAuxQueryClause(queryClause);
    }

    ASSERT_TRUE(_validateProcessorPtr->validateRequest(requestPtr, _result));

    RVP_CHECK_NO_ERROR();
}

TEST_F(RequestValidateProcessorTest, testNoneStopWordProcess) {
    HA3_LOG(DEBUG, "Begin Test!");
    RequestPtr requestPtr(new Request());

    ConfigClause *configClause = new ConfigClause();
    configClause->addClusterName("cluster1");
    requestPtr->setConfigClause(configClause);
    {
        AndQuery * andQuery = new AndQuery("");
        RequiredFields requiredField;
        QueryPtr queryPtr1(new TermQuery("aaa bbb ccc", "default", requiredField, ""));
        QueryPtr queryPtr2(new TermQuery("ccc ddd eee", "default", requiredField, ""));
        andQuery->addQuery(queryPtr1);
        andQuery->addQuery(queryPtr2);
        QueryClause *queryClause = new QueryClause(andQuery);
        requestPtr->setQueryClause(queryClause);
    }

    {
        AndQuery * andQuery = new AndQuery("");
        RequiredFields requiredField;
        QueryPtr queryPtr1(new TermQuery("aaa bbb ccc", "aux_default", requiredField, ""));
        QueryPtr queryPtr2(new TermQuery("ccc ddd eee", "aux_default", requiredField, ""));
        andQuery->addQuery(queryPtr1);
        andQuery->addQuery(queryPtr2);
        AuxQueryClause *queryClause = new AuxQueryClause(andQuery);
        requestPtr->setAuxQueryClause(queryClause);
    }

    ASSERT_TRUE(_validateProcessorPtr->validateRequest(requestPtr, _result));

    RVP_CHECK_NO_ERROR();
}

TEST_F(RequestValidateProcessorTest, testProcessRequestWithStopWord) {
    HA3_LOG(DEBUG, "Begin Test!");
    {
        RequestPtr requestPtr = RequestCreator::prepareRequest(
                "config=cluster:cluster1&&query=(stopword1 ANDNOT aaa) ANDNOT bbb",
                "default");
        ASSERT_TRUE(requestPtr);
        ASSERT_TRUE(!_validateProcessorPtr->validateRequest(requestPtr, _result));
        string errorMsg = "AndNotQuery or RankQuery's leftQuery is NULL or stop word.";
        RVP_CHECK_AND_RESET_ERROR_WITH_MESSAGE(ERROR_QUERY_INVALID, errorMsg);
    }

    {
        RequestPtr requestPtr = RequestCreator::prepareRequest(
                "config=cluster:cluster1&&query= bbb ANDNOT (stopword1 RANK aaa)",
                "default");
        ASSERT_TRUE(requestPtr);
        ASSERT_TRUE(!_validateProcessorPtr->validateRequest(requestPtr, _result));
        string errorMsg = "AndNotQuery or RankQuery's leftQuery is NULL or stop word.";
        RVP_CHECK_AND_RESET_ERROR_WITH_MESSAGE(ERROR_QUERY_INVALID, errorMsg);
    }

    {
        RequestPtr requestPtr = RequestCreator::prepareRequest(
                "config=cluster:cluster1&&query=(aaa AND stopword1) ANDNOT bbb",
                "default");
        ASSERT_TRUE(requestPtr);
        ASSERT_TRUE(_validateProcessorPtr->validateRequest(requestPtr, _result));
        RVP_CHECK_NO_ERROR();
    }

    {
        RequestPtr requestPtr = RequestCreator::prepareRequest(
                "config=cluster:cluster1&&query=(stopword1 ANDNOT aaa) AND bbb",
                "default");
        ASSERT_TRUE(requestPtr);
        ASSERT_TRUE(!_validateProcessorPtr->validateRequest(requestPtr, _result));
        string errorMsg = "AndNotQuery or RankQuery's leftQuery is NULL or stop word.";
        RVP_CHECK_AND_RESET_ERROR_WITH_MESSAGE(ERROR_QUERY_INVALID, errorMsg);
    }

    {
        RequestPtr requestPtr = RequestCreator::prepareRequest(
                "config=cluster:cluster1&&query=aaa AND (stopword1 ANDNOT bbb)",
                "default");
        ASSERT_TRUE(requestPtr);

        ASSERT_TRUE(!_validateProcessorPtr->validateRequest(requestPtr, _result));
        string errorMsg = "AndNotQuery or RankQuery's leftQuery is NULL or stop word.";
        RVP_CHECK_AND_RESET_ERROR_WITH_MESSAGE(ERROR_QUERY_INVALID, errorMsg);
    }

    {
        RequestPtr requestPtr = RequestCreator::prepareRequest(
                "config=cluster:cluster1&&query=(stopword1 RANK  aaa)"
                "OR (stopword2 ANDNOT bbb)", "default");
        ASSERT_TRUE(requestPtr);
        ASSERT_TRUE(!_validateProcessorPtr->validateRequest(requestPtr, _result));
        string errorMsg = "AndNotQuery or RankQuery's leftQuery is NULL or stop word.";
        RVP_CHECK_AND_RESET_ERROR_WITH_MESSAGE(ERROR_QUERY_INVALID, errorMsg);
    }
}

TEST_F(RequestValidateProcessorTest, testProcessRequestWithStopWordAuxQuery) {
    HA3_LOG(DEBUG, "Begin Test!");
    {
        RequestPtr requestPtr = RequestCreator::prepareRequest(
                "config=cluster:cluster1&&query=aaa&&aux_query=(aux_default:stopword1"
                " ANDNOT aux_default:aaa) ANDNOT aux_default:bbb",
                "default");
        ASSERT_TRUE(requestPtr);
        ASSERT_TRUE(!_validateProcessorPtr->validateRequest(requestPtr, _result));
        string errorMsg = "AndNotQuery or RankQuery's leftQuery is NULL or stop word.";
        RVP_CHECK_AND_RESET_ERROR_WITH_MESSAGE(ERROR_QUERY_INVALID, errorMsg);
    }

    {
        RequestPtr requestPtr = RequestCreator::prepareRequest(
                "config=cluster:cluster1&&query=aaa&&"
                "aux_query=aux_default:bbb ANDNOT"
                " (aux_default:stopword1 RANK aux_default:aaa)", "default");
        ASSERT_TRUE(requestPtr);
        ASSERT_TRUE(!_validateProcessorPtr->validateRequest(requestPtr, _result));
        string errorMsg = "AndNotQuery or RankQuery's leftQuery is NULL or stop word.";
        RVP_CHECK_AND_RESET_ERROR_WITH_MESSAGE(ERROR_QUERY_INVALID, errorMsg);
    }

    {
        RequestPtr requestPtr = RequestCreator::prepareRequest(
                "config=cluster:cluster1&&query=aaa&&aux_query=(aux_default:aaa"
                " AND aux_default:stopword1) ANDNOT aux_default:bbb",
                "default");
        ASSERT_TRUE(requestPtr);
        ASSERT_TRUE(_validateProcessorPtr->validateRequest(requestPtr, _result));
        RVP_CHECK_NO_ERROR();
    }

    {
        RequestPtr requestPtr = RequestCreator::prepareRequest(
                "config=cluster:cluster1&&query=aaa&&aux_query=(aux_default:stopword1"
                " ANDNOT aux_default:aaa) AND aux_default:bbb",
                "default");
        ASSERT_TRUE(requestPtr);
        ASSERT_TRUE(!_validateProcessorPtr->validateRequest(requestPtr, _result));
        string errorMsg = "AndNotQuery or RankQuery's leftQuery is NULL or stop word.";
        RVP_CHECK_AND_RESET_ERROR_WITH_MESSAGE(ERROR_QUERY_INVALID, errorMsg);
    }

    {
        RequestPtr requestPtr = RequestCreator::prepareRequest(
                "config=cluster:cluster1&&query=aaa&&aux_query=aux_default:aaa"
                " AND (aux_default:stopword1 ANDNOT aux_default:bbb)",
                "default");
        ASSERT_TRUE(requestPtr);

        ASSERT_TRUE(!_validateProcessorPtr->validateRequest(requestPtr, _result));
        string errorMsg = "AndNotQuery or RankQuery's leftQuery is NULL or stop word.";
        RVP_CHECK_AND_RESET_ERROR_WITH_MESSAGE(ERROR_QUERY_INVALID, errorMsg);
    }

    {
        RequestPtr requestPtr = RequestCreator::prepareRequest(
                "config=cluster:cluster1&&query=aaa&&aux_query=(aux_default:stopword1"
                " RANK  aux_default:aaa)"
                "OR (aux_default:stopword2 ANDNOT aux_default:bbb)", "default");
        ASSERT_TRUE(requestPtr);
        ASSERT_TRUE(!_validateProcessorPtr->validateRequest(requestPtr, _result));
        string errorMsg = "AndNotQuery or RankQuery's leftQuery is NULL or stop word.";
        RVP_CHECK_AND_RESET_ERROR_WITH_MESSAGE(ERROR_QUERY_INVALID, errorMsg);
    }
}

TEST_F(RequestValidateProcessorTest, testProcessWithAllStopWord) {
    HA3_LOG(DEBUG, "Begin Test!");
    RequestPtr requestPtr = prepareRequest();

    ASSERT_TRUE(!_validateProcessorPtr->validateRequest(requestPtr, _result));
    RVP_CHECK_AND_RESET_ERROR(ERROR_NO_QUERY_CLAUSE);
}

TEST_F(RequestValidateProcessorTest, testProcessWithSpecifiedAnayzer) {
    HA3_LOG(DEBUG, "Begin Test!");
    {
        RequestPtr requestPtr = prepareRequest();
        requestPtr->getConfigClause()->setAnalyzerName("simple");
        ASSERT_TRUE(_validateProcessorPtr->validateRequest(requestPtr, _result));
        RVP_CHECK_NO_ERROR();
    }

    {
        RequestPtr requestPtr = prepareRequest();
        requestPtr->getConfigClause()->setAnalyzerName("");
        ASSERT_TRUE(!_validateProcessorPtr->validateRequest(requestPtr, _result));
        RVP_CHECK_AND_RESET_ERROR(ERROR_NO_QUERY_CLAUSE);
    }

    {
        RequestPtr requestPtr = prepareRequest();
        requestPtr->getConfigClause()->setAnalyzerName("errorAnalyzer");
        ASSERT_TRUE(!_validateProcessorPtr->validateRequest(requestPtr, _result));
        RVP_CHECK_AND_RESET_ERROR(ERROR_ANALYZER_NOT_EXIST);
    }
}


TEST_F(RequestValidateProcessorTest, testProcessWithStopWord2) {
    HA3_LOG(DEBUG, "Begin Test!");
    RequestPtr requestPtr(new Request());

    ConfigClause *configClause = new ConfigClause();
    configClause->addClusterName("cluster1");
    requestPtr->setConfigClause(configClause);
    AndNotQuery *andnotQuery = new AndNotQuery("");
    QueryPtr queryPtr1(new RankQuery(""));
    RequiredFields requiredField;
    QueryPtr queryPtr11(new TermQuery("stopword1 stopword2 stopword3",
                    "default", requiredField, ""));
    QueryPtr queryPtr12(new TermQuery("aaa bbb ccc", "default", requiredField, ""));
    queryPtr1->addQuery(queryPtr11);
    queryPtr1->addQuery(queryPtr12);
    OrQuery *orQuery = new OrQuery("");
    QueryPtr queryPtr2;
    QueryPtr queryPtr21(new TermQuery("aaa", "default", requiredField, ""));
    QueryPtr queryPtr22(new TermQuery("stopword2", "default", requiredField, ""));
    orQuery->addQuery(queryPtr21);
    orQuery->addQuery(queryPtr22);
    queryPtr2.reset(orQuery);
    andnotQuery->addQuery(queryPtr1);
    andnotQuery->addQuery(queryPtr2);
    QueryClause *queryClause = new QueryClause(andnotQuery);
    requestPtr->setQueryClause(queryClause);

    ASSERT_TRUE(!_validateProcessorPtr->validateRequest(requestPtr, _result));
    RVP_CHECK_AND_RESET_ERROR(ERROR_QUERY_INVALID);
}

TEST_F(RequestValidateProcessorTest, testProcessWithTraceInfo)
{
    HA3_LOG(DEBUG, "Begin Test!");
    RequestPtr requestPtr(new Request());

    ConfigClause *configClause = new ConfigClause();
    configClause->addClusterName("cluster1");
    requestPtr->setConfigClause(configClause);

    OrQuery *query = new OrQuery("");
    RequiredFields requiredField;
    QueryPtr queryPtr11(new TermQuery("stopword1 stopword2 stopword3",
                    "default", requiredField, ""));
    QueryPtr queryPtr12(new TermQuery("aaa bbb ccc", "default",
                    requiredField, ""));
    query->addQuery(queryPtr11);
    query->addQuery(queryPtr12);

    QueryClause *queryClause = new QueryClause(query);
    requestPtr->setQueryClause(queryClause);

    common::Tracer *tracer = new common::Tracer;
    tracer->setLevel(0);
    _validateProcessorPtr->setTracer(tracer);
    _validateProcessorPtr->process(requestPtr, _result);

    string resultTrace = tracer->getTraceInfo();
    string expectString1 ("aaa");
    string expectString2 ("bbb");
    string expectString3 ("ccc");
    ASSERT_TRUE( resultTrace.find(expectString1) != string::npos);
    ASSERT_TRUE( resultTrace.find(expectString2) != string::npos);
    ASSERT_TRUE( resultTrace.find(expectString3) != string::npos);
    delete tracer;
}


TEST_F(RequestValidateProcessorTest, testQuerClauseWithMultiQuerys)
{
    HA3_LOG(DEBUG, "Begin Test!");
    RequestPtr requestPtr(new Request());

    ConfigClause *configClause = new ConfigClause();
    configClause->addClusterName("cluster1");
    requestPtr->setConfigClause(configClause);

    PhraseQuery *phraseQuery1 = new PhraseQuery("");
    RequiredFields requiredField;
    phraseQuery1->addTerm(TermPtr(new Term("ab bc",
                            "default", requiredField, 20, "second1")));
    PhraseQuery *phraseQuery2 = new PhraseQuery("");
    phraseQuery2->addTerm(TermPtr(new Term("abc ddd",
                            "default", requiredField, 33, "second2")));

    QueryClause *queryClause = new QueryClause();
    queryClause->setRootQuery(phraseQuery1, 0);
    queryClause->setRootQuery(phraseQuery2, 1);
    requestPtr->setQueryClause(queryClause);

    _validateProcessorPtr->process(requestPtr, _result);

    queryClause = requestPtr->getQueryClause();
    ASSERT_EQ(uint32_t(2), queryClause->getQueryCount());
    Query *query1 = queryClause->getRootQuery(0);
    ASSERT_TRUE(query1);
    PhraseQuery expectQuery1("");
    expectQuery1.addTerm(TermPtr(new Term(Token("ab", 0), "default", requiredField, 20, "second1")));
    expectQuery1.addTerm(TermPtr(new Term(Token("bc", 2), "default", requiredField, 20, "second1")));
    ASSERT_EQ(expectQuery1.getQueryName(), query1->getQueryName());
    ASSERT_EQ(*(Query*)&expectQuery1, *query1);

    Query *query2 = queryClause->getRootQuery(1);
    ASSERT_TRUE(query2);
    PhraseQuery expectQuery2("");
    expectQuery2.addTerm(TermPtr(new Term(Token("abc", 0), "default", requiredField, 33, "second2")));
    expectQuery2.addTerm(TermPtr(new Term(Token("ddd", 2), "default", requiredField, 33, "second2")));
    ASSERT_EQ(*(Query*)&expectQuery2, *query2);
}

TEST_F(RequestValidateProcessorTest, testWithExceptionProcessor) {
    ExceptionQrsProcessorPtr exceptionQrsProcessor(new ExceptionQrsProcessor());
    _validateProcessorPtr->setNextProcessor(exceptionQrsProcessor);
    RequestPtr requestPtr = prepareCorrectRequest();
    bool catchException = false;
    try {
        _validateProcessorPtr->process(requestPtr, _result);
    } catch (...) {
        catchException = true;
    }
    ASSERT_TRUE(catchException);
}

TEST_F(RequestValidateProcessorTest, testRankSortClauseConflictWithSortClause) {
    {
        RequestPtr requestPtr = RequestCreator::prepareRequest(
                "config=cluster:cluster1&&query=a"
                "&&sort=RANK"
                "&&rank_sort=sort:RANK,percent:40;sort:+RANK,percent:60",
                "default");
        ASSERT_TRUE(_validateProcessorPtr->validateRequest(requestPtr, _result));
        RVP_CHECK_NO_ERROR();
    }
    {
        RequestPtr requestPtr = RequestCreator::prepareRequest(
                "config=cluster:cluster1&&query=a"
                "&&rank_sort=sort:RANK,percent:100",
                "default");
        ASSERT_TRUE(requestPtr);
        ASSERT_TRUE(_validateProcessorPtr->validateRequest(requestPtr, _result));
        RVP_CHECK_NO_ERROR();
    }
}

TEST_F(RequestValidateProcessorTest, testAnalyzerWithConfigClause) {
    {
        RequestPtr requestPtr = RequestCreator::prepareRequest(
                "config=cluster:cluster1&&query=菊花", "aliwsIndex");
        ASSERT_TRUE(requestPtr);
        ASSERT_TRUE(_validateProcessorPtr->validateRequest(requestPtr, _result));
        QueryClause *queryClause = requestPtr->getQueryClause();
        AndQuery* resultQuery = dynamic_cast<AndQuery*>(queryClause->getRootQuery());
        ASSERT_TRUE(resultQuery);
        RequiredFields requiredField;
        AndQuery expectedQuery("");
        QueryPtr leftQueryPtr(new TermQuery("菊", "aliwsIndex", requiredField, ""));
        QueryPtr rightQueryPtr(new TermQuery(Token("花", 1),
                        "aliwsIndex", requiredField, ""));
        expectedQuery.addQuery(leftQueryPtr);
        expectedQuery.addQuery(rightQueryPtr);
        ASSERT_EQ(expectedQuery, *resultQuery);
    }
    {
        RequestPtr requestPtr = RequestCreator::prepareRequest(
                "query=菊花茶&&config=cluster:cluster1,analyzer:singlews",
                "default");
        ASSERT_TRUE(requestPtr);
        ASSERT_TRUE(_validateProcessorPtr->validateRequest(requestPtr, _result));
        QueryClause *queryClause = requestPtr->getQueryClause();
        AndQuery* resultQuery = dynamic_cast<AndQuery*>(queryClause->getRootQuery());
        ASSERT_TRUE(resultQuery);
        AndQuery expectedQuery("");
        Token token1("菊", 0);
        Token token2("花", 1);
        Token token3("茶", 2);
        RequiredFields requiredField;
        QueryPtr firstQueryPtr(new TermQuery(token1, "default", requiredField, ""));
        QueryPtr secondQueryPtr(new TermQuery(token2,
                        "default", requiredField, ""));
        QueryPtr thirdQueryPtr(new TermQuery(token3,
                        "default", requiredField, ""));
        expectedQuery.addQuery(firstQueryPtr);
        expectedQuery.addQuery(secondQueryPtr);
        expectedQuery.addQuery(thirdQueryPtr);
        ASSERT_EQ(expectedQuery, *resultQuery);
    }
    {
        RequestPtr requestPtr = RequestCreator::prepareRequest(
                "query=菊花茶&&config=cluster:cluster1,"
                "analyzer:singlews,no_tokenize_indexes:default", "default");
        ASSERT_TRUE(requestPtr);
        ASSERT_TRUE(_validateProcessorPtr->validateRequest(requestPtr, _result));
        QueryClause *queryClause = requestPtr->getQueryClause();
        TermQuery* resultQuery = dynamic_cast<TermQuery*>(queryClause->getRootQuery());
        ASSERT_TRUE(resultQuery);
        RequiredFields requiredField;
        TermQuery expectedQuery("菊花茶", "default", requiredField, "");;
        ASSERT_EQ(expectedQuery, *resultQuery);
    }
}

TEST_F(RequestValidateProcessorTest, testAnalyzerClause) {
    {
        RequestPtr requestPtr = RequestCreator::prepareRequest(
                "query=菊花茶&&config=cluster:cluster1&&"
                "analyzer=specific_index_analyzer:default#singlews",
                "default");
        ASSERT_TRUE(requestPtr);
        ASSERT_TRUE(_validateProcessorPtr->validateRequest(requestPtr, _result));
        QueryClause *queryClause = requestPtr->getQueryClause();
        AndQuery* resultQuery = dynamic_cast<AndQuery*>(queryClause->getRootQuery());
        ASSERT_TRUE(resultQuery);
        AndQuery expectedQuery("");
        Token token1("菊", 0);
        Token token2("花", 1);
        Token token3("茶", 2);
        RequiredFields requiredField;
        QueryPtr firstQueryPtr(new TermQuery(token1, "default", requiredField, ""));
        QueryPtr secondQueryPtr(new TermQuery(token2,
                        "default", requiredField, ""));
        QueryPtr thirdQueryPtr(new TermQuery(token3,
                        "default", requiredField, ""));
        expectedQuery.addQuery(firstQueryPtr);
        expectedQuery.addQuery(secondQueryPtr);
        expectedQuery.addQuery(thirdQueryPtr);
        ASSERT_EQ(expectedQuery, *resultQuery);
    }
    {
        RequestPtr requestPtr = RequestCreator::prepareRequest(
                "query=菊花茶&&config=cluster:cluster1&&"
                "analyzer=global_analyzer:singlews",
                "default");
        ASSERT_TRUE(requestPtr);
        ASSERT_TRUE(_validateProcessorPtr->validateRequest(requestPtr, _result));
        QueryClause *queryClause = requestPtr->getQueryClause();
        AndQuery* resultQuery = dynamic_cast<AndQuery*>(queryClause->getRootQuery());
        ASSERT_TRUE(resultQuery);
        AndQuery expectedQuery("");

        RequiredFields requiredField;
        Token token1("菊", 0);
        Token token2("花", 1);
        Token token3("茶", 2);
        QueryPtr firstQueryPtr(new TermQuery(token1, "default", requiredField, ""));
        QueryPtr secondQueryPtr(new TermQuery(token2,
                        "default", requiredField, ""));
        QueryPtr thirdQueryPtr(new TermQuery(token3,
                        "default", requiredField, ""));
        expectedQuery.addQuery(firstQueryPtr);
        expectedQuery.addQuery(secondQueryPtr);
        expectedQuery.addQuery(thirdQueryPtr);
        ASSERT_EQ(expectedQuery, *resultQuery);
    }
    {
        RequestPtr requestPtr = RequestCreator::prepareRequest(
                "query=菊花茶&&config=cluster:cluster1&&"
                "analyzer=global_analyzer:aliws,"
                "specific_index_analyzer:default#singlews", "default");
        ASSERT_TRUE(requestPtr);
        ASSERT_TRUE(_validateProcessorPtr->validateRequest(requestPtr, _result));
        QueryClause *queryClause = requestPtr->getQueryClause();
        AndQuery* resultQuery = dynamic_cast<AndQuery*>(queryClause->getRootQuery());
        ASSERT_TRUE(resultQuery);
        AndQuery expectedQuery("");
        Token token1("菊", 0);
        Token token2("花", 1);
        Token token3("茶", 2);
        RequiredFields requiredField;
        QueryPtr firstQueryPtr(new TermQuery(token1, "default", requiredField, ""));
        QueryPtr secondQueryPtr(new TermQuery(token2, "default", requiredField, ""));
        QueryPtr thirdQueryPtr(new TermQuery(token3, "default", requiredField, ""));
        expectedQuery.addQuery(firstQueryPtr);
        expectedQuery.addQuery(secondQueryPtr);
        expectedQuery.addQuery(thirdQueryPtr);
        ASSERT_EQ(expectedQuery, *resultQuery);
    }
    {
        RequestPtr requestPtr = RequestCreator::prepareRequest(
                "query=菊花茶&&config=cluster:cluster1&&"
                "analyzer=global_analyzer:singlews,"
                "no_tokenize_indexes:default", "default");
        ASSERT_TRUE(requestPtr);
        ASSERT_TRUE(_validateProcessorPtr->validateRequest(requestPtr, _result));
        QueryClause *queryClause = requestPtr->getQueryClause();
        TermQuery* resultQuery = dynamic_cast<TermQuery*>(queryClause->getRootQuery());
        ASSERT_TRUE(resultQuery);
        RequiredFields requiredField;
        TermQuery expectedQuery("菊花茶", "default", requiredField, "");;
        ASSERT_EQ(expectedQuery, *resultQuery);
    }
    {
        RequestPtr requestPtr = RequestCreator::prepareRequest(
                "query=菊花茶&&config=cluster:cluster1&&"
                "analyzer=global_analyzer:singlews,"
                "no_tokenize_indexes:default", "aliwsIndex");
        ASSERT_TRUE(requestPtr);
        ASSERT_TRUE(_validateProcessorPtr->validateRequest(requestPtr, _result));
        QueryClause *queryClause = requestPtr->getQueryClause();
        AndQuery* resultQuery = dynamic_cast<AndQuery*>(queryClause->getRootQuery());
        ASSERT_TRUE(resultQuery);
        AndQuery expectedQuery("");
        Token token1("菊", 0);
        Token token2("花", 1);
        Token token3("茶", 2);
        RequiredFields requiredField;
        QueryPtr firstQueryPtr(new TermQuery(token1, "aliwsIndex", requiredField, ""));
        QueryPtr secondQueryPtr(new TermQuery(token2, "aliwsIndex", requiredField, ""));
        QueryPtr thirdQueryPtr(new TermQuery(token3, "aliwsIndex", requiredField, ""));
        expectedQuery.addQuery(firstQueryPtr);
        expectedQuery.addQuery(secondQueryPtr);
        expectedQuery.addQuery(thirdQueryPtr);
        ASSERT_EQ(expectedQuery, *resultQuery);
    }
    {
        RequestPtr requestPtr = RequestCreator::prepareRequest(
                "query=菊花&&config=cluster:cluster1&&"
                "analyzer:no_tokenize_indexes:default", "aliwsIndex");
        ASSERT_TRUE(requestPtr);
        ASSERT_TRUE(_validateProcessorPtr->validateRequest(requestPtr, _result));
        QueryClause *queryClause = requestPtr->getQueryClause();
        AndQuery* resultQuery = dynamic_cast<AndQuery*>(queryClause->getRootQuery());
        ASSERT_TRUE(resultQuery);

        AndQuery expectedQuery("");
        RequiredFields requiredField;
        QueryPtr leftQueryPtr(new TermQuery("菊", "aliwsIndex", requiredField, ""));
        QueryPtr rightQueryPtr(new TermQuery(Token("花", 1),
                        "aliwsIndex", requiredField, ""));
        expectedQuery.addQuery(leftQueryPtr);
        expectedQuery.addQuery(rightQueryPtr);
        ASSERT_EQ(expectedQuery, *resultQuery);
    }
}

TEST_F(RequestValidateProcessorTest, testAnalyzerClauseCompatibleWithConfigClause) {
    {
        RequestPtr requestPtr = RequestCreator::prepareRequest(
                "query=菊花茶&&config=cluster:cluster1,analyzer:aliws&&"
                "analyzer=global_analyzer:singlews", "default");
        ASSERT_TRUE(requestPtr);
        ASSERT_TRUE(_validateProcessorPtr->validateRequest(requestPtr, _result));
        QueryClause *queryClause = requestPtr->getQueryClause();
        AndQuery* resultQuery = dynamic_cast<AndQuery*>(queryClause->getRootQuery());
        ASSERT_TRUE(resultQuery);
        AndQuery expectedQuery("");
        Token token1("菊", 0);
        Token token2("花", 1);
        Token token3("茶", 2);
        RequiredFields requiredField;
        QueryPtr firstQueryPtr(new TermQuery(token1, "default", requiredField, ""));
        QueryPtr secondQueryPtr(new TermQuery(token2, "default", requiredField, ""));
        QueryPtr thirdQueryPtr(new TermQuery(token3, "default", requiredField, ""));
        expectedQuery.addQuery(firstQueryPtr);
        expectedQuery.addQuery(secondQueryPtr);
        expectedQuery.addQuery(thirdQueryPtr);
        ASSERT_EQ(expectedQuery, *resultQuery);
    }
    {
        RequestPtr requestPtr = RequestCreator::prepareRequest(
                "query=菊花茶&&config=cluster:cluster1,"
                "no_tokenize_indexes:aliwsIndex&&"
                "analyzer=no_tokenize_indexes:default", "default");
        ASSERT_TRUE(requestPtr);
        ASSERT_TRUE(_validateProcessorPtr->validateRequest(requestPtr, _result));
        QueryClause *queryClause = requestPtr->getQueryClause();
        TermQuery* resultQuery = dynamic_cast<TermQuery*>(queryClause->getRootQuery());
        ASSERT_TRUE(resultQuery);
        RequiredFields requiredField;
        TermQuery expectedQuery("菊花茶", "default", requiredField, "");;
        ASSERT_EQ(expectedQuery, *resultQuery);
    }
    {
        RequestPtr requestPtr = RequestCreator::prepareRequest(
                "query=菊花茶&&config=cluster:cluster1,"
                "no_tokenize_indexes:aliwsIndex&&"
                "analyzer=no_tokenize_indexes:default", "aliwsIndex");
        ASSERT_TRUE(requestPtr);
        ASSERT_TRUE(_validateProcessorPtr->validateRequest(requestPtr, _result));
        QueryClause *queryClause = requestPtr->getQueryClause();
        TermQuery* resultQuery = dynamic_cast<TermQuery*>(queryClause->getRootQuery());
        ASSERT_TRUE(resultQuery);
        RequiredFields requiredField;
        TermQuery expectedQuery("菊花茶", "aliwsIndex", requiredField, "");;
        ASSERT_EQ(expectedQuery, *resultQuery);
    }
    {
        RequestPtr requestPtr = RequestCreator::prepareRequest(
                "query=菊花茶&&config=cluster:cluster1,analyzer:aliws&&"
                "analyzer=specific_index_analyzer:default#singlews", "default");
        ASSERT_TRUE(requestPtr);
        ASSERT_TRUE(_validateProcessorPtr->validateRequest(requestPtr, _result));
        QueryClause *queryClause = requestPtr->getQueryClause();
        AndQuery* resultQuery = dynamic_cast<AndQuery*>(queryClause->getRootQuery());
        ASSERT_TRUE(resultQuery);
        AndQuery expectedQuery("");
        Token token1("菊", 0);
        Token token2("花", 1);
        Token token3("茶", 2);
        RequiredFields requiredField;
        QueryPtr firstQueryPtr(new TermQuery(token1, "default", requiredField, ""));
        QueryPtr secondQueryPtr(new TermQuery(token2, "default", requiredField, ""));
        QueryPtr thirdQueryPtr(new TermQuery(token3, "default", requiredField, ""));
        expectedQuery.addQuery(firstQueryPtr);
        expectedQuery.addQuery(secondQueryPtr);
        expectedQuery.addQuery(thirdQueryPtr);
        ASSERT_EQ(expectedQuery, *resultQuery);
    }
    {
        RequestPtr requestPtr = RequestCreator::prepareRequest(
                "query=菊花茶&&config=cluster:cluster1,"
                "no_tokenize_indexes:default&&"
                "analyzer=specific_index_analyzer:default#singlews", "default");
        ASSERT_TRUE(requestPtr);
        ASSERT_TRUE(_validateProcessorPtr->validateRequest(requestPtr, _result));
        QueryClause *queryClause = requestPtr->getQueryClause();
        TermQuery* resultQuery = dynamic_cast<TermQuery*>(queryClause->getRootQuery());
        ASSERT_TRUE(resultQuery);
        RequiredFields requiredField;
        TermQuery expectedQuery("菊花茶", "default", requiredField, "");;
        ASSERT_EQ(expectedQuery, *resultQuery);
    }
}

TEST_F(RequestValidateProcessorTest, testAnalyzerClauseWithInvalidAnalyzer) {
    {
        RequestPtr requestPtr = RequestCreator::prepareRequest(
                "query=菊花茶&&config=cluster:cluster1&&"
                "analyzer=specific_index_analyzer:default#"
                "not_exitsted_analyzer", "default");
        ASSERT_TRUE(requestPtr);
        ASSERT_TRUE(!_validateProcessorPtr->validateRequest(requestPtr, _result));
        RVP_INNER_CHECK_AND_RESET_ERROR(ERROR_ANALYZER_NOT_EXIST);
    }
    {
        RequestPtr requestPtr = RequestCreator::prepareRequest(
                "query=菊花茶&&config=cluster:cluster1&&"
                "analyzer=global_analyzer:not_exitsted_analyzer", "default");
        ASSERT_TRUE(requestPtr);
        ASSERT_TRUE(!_validateProcessorPtr->validateRequest(requestPtr, _result));
        RVP_INNER_CHECK_AND_RESET_ERROR(ERROR_ANALYZER_NOT_EXIST);
    }
    {
        RequestPtr requestPtr = RequestCreator::prepareRequest(
                "query=菊花茶&&config=cluster:cluster1,"
                "analyzer:not_exitsted_analyzer", "default");
        ASSERT_TRUE(requestPtr);
        ASSERT_TRUE(!_validateProcessorPtr->validateRequest(requestPtr, _result));
        RVP_INNER_CHECK_AND_RESET_ERROR(ERROR_ANALYZER_NOT_EXIST);
    }
}

TEST_F(RequestValidateProcessorTest, testQuerClauseWithRequireMatchDocs) {
    {
        RequestPtr requestPtr = RequestCreator::prepareRequest(
                "config=cluster:cluster1,proto_format_option:pb_matchdoc_format&&query=aaa",
                "default");
        ASSERT_TRUE(requestPtr);
        ASSERT_TRUE(!_validateProcessorPtr->validateRequest(requestPtr, _result));
        string errorMsg = "ConfigClause: pb matchdocs format can only be"
                          " used in phase1.";
        RVP_CHECK_AND_RESET_ERROR_WITH_MESSAGE(ERROR_PB_MATCHDOCS_FORMAT_IN_PHASE2, errorMsg);
    }
    {
        RequestPtr requestPtr = RequestCreator::prepareRequest(
                "config=cluster:cluster1,proto_format_option:pb_matchdoc_format,"
                "no_summary:no&&query=aaa", "default");
        ASSERT_TRUE(requestPtr);
        ASSERT_TRUE(!_validateProcessorPtr->validateRequest(requestPtr, _result));
        string errorMsg = "ConfigClause: pb matchdocs format can only be"
                          " used in phase1.";
        RVP_CHECK_AND_RESET_ERROR_WITH_MESSAGE(ERROR_PB_MATCHDOCS_FORMAT_IN_PHASE2, errorMsg);
    }
    {
        RequestPtr requestPtr = RequestCreator::prepareRequest(
                "config=cluster:cluster1,proto_format_option:pb_matchdoc_format,no_summary:yes"
                "&&query=aaa", "default");
        ASSERT_TRUE(requestPtr);
        ASSERT_TRUE(!_validateProcessorPtr->validateRequest(requestPtr, _result));
        string errorMsg = "ConfigClause: pb matchdocs format can only be"
                          " used in pb format.";
        RVP_CHECK_AND_RESET_ERROR_WITH_MESSAGE(ERROR_PB_MATCHDOCS_FORMAT_WITH_XML, errorMsg);
    }
    {
        RequestPtr requestPtr = RequestCreator::prepareRequest(
                "config=cluster:cluster1,proto_format_option:pb_matchdoc_format,no_summary:yes,"
                "format:protobuf&&query=aaa", "default");
        ASSERT_TRUE(requestPtr);
        ASSERT_TRUE(_validateProcessorPtr->validateRequest(requestPtr, _result));
        RVP_CHECK_NO_ERROR();
    }
}

TEST_F(RequestValidateProcessorTest, testIndexInfoCompatible) {
    IndexInfos* indexInfos1 = createIndexInfos(
            StringUtil::split("index1,index2", ","),
            StringUtil::split("analyzer1,analyzer2", ","));
    IndexInfos* indexInfos2 = createIndexInfos(
            StringUtil::split("index1,index2", ","),
            StringUtil::split("analyzer1,analyzer2", ","));
    IndexInfos* indexInfos3 = createIndexInfos(
            StringUtil::split("index1,index2", ","),
            StringUtil::split("analyzer1,analyzer3", ","));
    ASSERT_TRUE(_validateProcessorPtr->indexInfoCompatible(NULL, NULL));
    ASSERT_TRUE(!_validateProcessorPtr->indexInfoCompatible(NULL, indexInfos1));
    ASSERT_TRUE(!_validateProcessorPtr->indexInfoCompatible(indexInfos1, NULL));
    ASSERT_TRUE(_validateProcessorPtr->indexInfoCompatible(indexInfos1, indexInfos2));
    ASSERT_TRUE(!_validateProcessorPtr->indexInfoCompatible(indexInfos1, indexInfos3));
    delete indexInfos1;
    delete indexInfos2;
    delete indexInfos3;
}

TEST_F(RequestValidateProcessorTest, testAttributeInfoCompatible) {
    vector<bool> multiFlag1;
    multiFlag1.push_back(false);
    multiFlag1.push_back(true);
    vector<FieldType> fieldTypes1;
    fieldTypes1.push_back(ft_string);
    fieldTypes1.push_back(ft_uint32);
    AttributeInfos *attrInfos1 = createAttributeInfos(
            StringUtil::split("attr1,attr2", ","), multiFlag1, fieldTypes1);
    AttributeInfos *attrInfos2 = createAttributeInfos(
            StringUtil::split("attr1,attr2", ","), multiFlag1, fieldTypes1);
    fieldTypes1[1] = ft_int32;
    AttributeInfos *attrInfos3 = createAttributeInfos(
            StringUtil::split("attr1,attr2", ","), multiFlag1, fieldTypes1);
    ASSERT_TRUE(_validateProcessorPtr->attributeInfoCompatible(NULL, NULL));
    ASSERT_TRUE(!_validateProcessorPtr->attributeInfoCompatible(attrInfos1, NULL));
    ASSERT_TRUE(!_validateProcessorPtr->attributeInfoCompatible(NULL, attrInfos1));
    ASSERT_TRUE(_validateProcessorPtr->attributeInfoCompatible(attrInfos1, attrInfos2));
    ASSERT_TRUE(!_validateProcessorPtr->attributeInfoCompatible(attrInfos1, attrInfos3));
    delete attrInfos1;
    delete attrInfos2;
    delete attrInfos3;
}

RequestPtr RequestValidateProcessorTest::prepareCorrectRequest() {
    RequestPtr requestPtr(new Request());

    ConfigClause *configClause = new ConfigClause();
    configClause->addClusterName("cluster1");
    requestPtr->setConfigClause(configClause);
    RequiredFields requiredField;
    QueryClause *queryClause = new QueryClause(new TermQuery("aaa bbb",
                    "default", requiredField, ""));
    requestPtr->setQueryClause(queryClause);

    AuxQueryClause *auxQueryClause = new AuxQueryClause(new TermQuery("ccc ddd",
                    "aux_default", requiredField, ""));
    requestPtr->setAuxQueryClause(auxQueryClause);
    return requestPtr;
}

RequestPtr RequestValidateProcessorTest::prepareErrorRequest() {
    RequestPtr requestPtr(new Request());
    requestPtr->setConfigClause(NULL);//config clause can not be null

    return requestPtr;
}

TableInfo* RequestValidateProcessorTest::createFakeTableInfo(const string& tableName) {
    TableInfo* tableInfo = new TableInfo(tableName);
    IndexInfos* indexInfos = new IndexInfos();
    IndexInfo* indexInfo = new IndexInfo();
    indexInfo->setIndexName("default");
    indexInfo->addField("title", 100);
    indexInfo->addField("body", 50);
    indexInfo->setIndexType(it_text);
    indexInfo->setAnalyzerName("alibaba");
    indexInfos->addIndexInfo(indexInfo);

    indexInfo = new IndexInfo();
    indexInfo->setIndexName("price");
    indexInfo->setIndexType(it_number);
    indexInfo->setAnalyzerName("alibaba");
    indexInfos->addIndexInfo(indexInfo);

    indexInfo = new IndexInfo();
    indexInfo->setIndexName("aliwsIndex");
    indexInfo->setIndexType(it_text);
    indexInfo->setAnalyzerName("singlews");
    indexInfos->addIndexInfo(indexInfo);

    indexInfo = new IndexInfo();
    indexInfo->setIndexName("stringindex");
    indexInfo->setIndexType(it_string);
    indexInfos->addIndexInfo(indexInfo);

    tableInfo->setIndexInfos(indexInfos);

    return tableInfo;
}

TableInfo* RequestValidateProcessorTest::createFakeAuxTableInfo(const string& tableName) {
    TableInfo* tableInfo = new TableInfo(tableName);
    IndexInfos* indexInfos = new IndexInfos();
    IndexInfo* indexInfo = new IndexInfo();
    indexInfo->setIndexName("aux_default");
    indexInfo->addField("aux_title", 100);
    indexInfo->setIndexType(it_text);
    indexInfo->setAnalyzerName("alibaba");
    indexInfos->addIndexInfo(indexInfo);

    indexInfo = new IndexInfo();
    indexInfo->setIndexName("aux_stringindex");
    indexInfo->setIndexType(it_string);
    indexInfos->addIndexInfo(indexInfo);

    tableInfo->setIndexInfos(indexInfos);

    return tableInfo;
}

AnalyzerFactoryPtr RequestValidateProcessorTest::initFactory() {
    AnalyzerInfosPtr infosPtr(new AnalyzerInfos());
    unique_ptr<AnalyzerInfo> taobaoInfo(new AnalyzerInfo());
    taobaoInfo->setTokenizerConfig(TOKENIZER_TYPE, SIMPLE_ANALYZER);
    taobaoInfo->setTokenizerConfig(DELIMITER, " ");
    taobaoInfo->addStopWord(string("stopword1"));
    taobaoInfo->addStopWord(string("stopword2"));
    taobaoInfo->addStopWord(string("stopword3"));
    infosPtr->addAnalyzerInfo("alibaba", *taobaoInfo);

    unique_ptr<AnalyzerInfo> singlewsInfo(new AnalyzerInfo());
    singlewsInfo->setTokenizerConfig(TOKENIZER_TYPE, SINGLEWS_ANALYZER);
    infosPtr->addAnalyzerInfo("singlews", *singlewsInfo);

    //init simple analyzer info
    AnalyzerInfo simpleInfo;
    simpleInfo.setTokenizerConfig(TOKENIZER_TYPE, SIMPLE_ANALYZER);
    simpleInfo.setTokenizerConfig(DELIMITER, " ");
    infosPtr->addAnalyzerInfo("simple", simpleInfo);

    string basePath = ALIWSLIB_DATA_PATH;
    string aliwsConfig = basePath + "/conf/";

    infosPtr->makeCompatibleWithOldConfig();
    TokenizerManagerPtr tokenizerManagerPtr(
            new TokenizerManager(ResourceReaderPtr(new ResourceReader(aliwsConfig))));
    tokenizerManagerPtr->init(infosPtr->getTokenizerConfig());
    AnalyzerFactory *analyzerFactory = new AnalyzerFactory();
    analyzerFactory->init(infosPtr, tokenizerManagerPtr);

    AnalyzerFactoryPtr ptr(analyzerFactory);
    return ptr;
}

IndexInfos* RequestValidateProcessorTest::createIndexInfos(
        const vector<string> &indexNames, const vector<string> &analyzerNames) {
    IndexInfos* indexInfos = new IndexInfos();
    assert(indexNames.size() == analyzerNames.size());
    for (size_t i = 0; i < indexNames.size(); ++i) {
        string indexName = indexNames[i];
        IndexInfo* indexInfo = new IndexInfo();
        if (indexName.size() >= 4 && indexName.substr(0, 4) == "pk64") {
            indexInfo->setIndexType(it_primarykey64);
            indexName = indexName.substr(4);
        } else if (indexName.size() >= 5 && indexName.substr(0, 5) == "pk128") {
            indexInfo->setIndexType(it_primarykey128);
            indexName = indexName.substr(5);
        }
        indexInfo->addField(string("field_" + indexName).c_str(), 0);
        indexInfo->setIndexName(indexName.c_str());
        indexInfo->setAnalyzerName(analyzerNames[i].c_str());
        indexInfos->addIndexInfo(indexInfo);
    }
    return indexInfos;
}

AttributeInfos* RequestValidateProcessorTest::createAttributeInfos(
        const vector<string> &attrNames, const vector<bool> &multiFlags,
        const vector<FieldType> &fieldTypes) {
    AttributeInfos* attrInfos = new AttributeInfos();
    assert(attrNames.size() == multiFlags.size());
    assert(attrNames.size() == fieldTypes.size());
    for (size_t i = 0; i < attrNames.size(); ++i) {
        AttributeInfo* attrInfo = new AttributeInfo();
        attrInfo->setAttrName(attrNames[i]);
        attrInfo->setMultiValueFlag(multiFlags[i]);
        FieldInfo fieldInfo;
        fieldInfo.fieldType = fieldTypes[i];
        attrInfo->setFieldInfo(fieldInfo);
        attrInfos->addAttributeInfo(attrInfo);
    }
    return attrInfos;
}

bool RequestValidateProcessorTest::validateFetchSummaryCluster(ConfigClause* configClause,
        const ClusterTableInfoMapPtr &clusterTableInfoMapPtr)
{
    MultiErrorResultPtr& errorResult = _result->getMultiErrorResult();

    _validateProcessorPtr->setClusterTableInfoMapPtr(clusterTableInfoMapPtr);

    return _validateProcessorPtr->validateFetchSummaryCluster(configClause, errorResult);
}

bool RequestValidateProcessorTest::validateMultiCluster(ConfigClause* configClause,
        LevelClause *levelClause, const ClusterTableInfoMapPtr &clusterTableInfoMapPtr)
{
    MultiErrorResultPtr& errorResult = _result->getMultiErrorResult();
    _validateProcessorPtr->setClusterTableInfoMapPtr(clusterTableInfoMapPtr);

    return _validateProcessorPtr->validateMultiCluster(configClause, levelClause, errorResult);
}

ClusterTableInfoMapPtr RequestValidateProcessorTest::createClusterTableInfoMapPtr(
        const string &indexStr, const string &clusterStr)
{
    vector<string> clusters = StringUtil::split(clusterStr, ",");
    vector<string> index = StringUtil::split(indexStr, ",");
    ClusterTableInfoMapPtr clusterTableInfoMapPtr(new ClusterTableInfoMap);
    for (size_t i = 0; i < index.size(); i++) {
        TableInfoPtr tableInfoPtr(new TableInfo);
        string tableName = "table" + StringUtil::toString(i);
        tableInfoPtr->setTableName(tableName);
        IndexInfos* indexInfos = createIndexInfos(
                StringUtil::split(index[i] + ",dummy_index", ","),
                StringUtil::split("analyzer1,analyzer2", ","));
        tableInfoPtr->setIndexInfos(indexInfos);
        (*clusterTableInfoMapPtr)[clusters[i]] = tableInfoPtr;
    }
    return clusterTableInfoMapPtr;
}

RequestPtr RequestValidateProcessorTest::prepareRequest(
        const string &word1, const string& word2)
{
    RequestPtr requestPtr(new Request());

    ConfigClause *configClause = new ConfigClause();
    configClause->addClusterName("cluster1");
    requestPtr->setConfigClause(configClause);
    AndQuery * andQuery = new AndQuery("");
    RequiredFields requiredField;
    QueryPtr queryPtr1(new TermQuery(word1.c_str(), "default", requiredField, ""));
    QueryPtr queryPtr2(new TermQuery(word2.c_str(), "default", requiredField, ""));
    andQuery->addQuery(queryPtr1);
    andQuery->addQuery(queryPtr2);
    QueryClause *queryClause = new QueryClause(andQuery);
    requestPtr->setQueryClause(queryClause);

    return requestPtr;
}

END_HA3_NAMESPACE(qrs);
