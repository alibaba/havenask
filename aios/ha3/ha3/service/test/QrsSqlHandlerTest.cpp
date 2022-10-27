#include <unittest/unittest.h>
#include <ha3/service/QrsSqlHandler.h>
#include <autil/legacy/jsonizable.h>
#include <navi/engine/GraphOutputPorts.h>
#include <ha3/service/test/NaviMock.h>
#include <mutex>
#include <iquan_jni/api/IquanEnv.h>

using namespace std;
using namespace navi;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace iquan;

USE_HA3_NAMESPACE(turing);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(monitor);
USE_HA3_NAMESPACE(sql);
USE_HA3_NAMESPACE(config);

BEGIN_HA3_NAMESPACE(service);

static std::once_flag iquanFlag;

class QrsSqlHandlerTest : public TESTBASE {
public:
    QrsSqlHandlerTest() {}

    ~QrsSqlHandlerTest() {}

public:
    void setUp();
    void tearDown();

    void buildDocument(std::string &planOpStr,
                       std::shared_ptr<autil::legacy::RapidDocument> docPtr) {
        planOpStr = planOpStr + std::string(128, '\0');
        autil_rapidjson::ParseResult ok = docPtr->Parse(planOpStr.c_str());
        ASSERT_TRUE(ok);
    }

    void buildPlans(std::string &planStr,
                    SqlPlan &plan,
                    std::shared_ptr<autil::legacy::RapidDocument> docPtr) {
        ASSERT_TRUE(docPtr != nullptr);

        buildDocument(planStr, docPtr);

        try {
            FromRapidValue(plan, *docPtr);
        } catch (const autil::legacy::ExceptionBase &e) {
            ASSERT_TRUE(false);
        } catch (const IquanException &e) {
            ASSERT_TRUE(false);
        }
    }

    void FakeRunGraphFailed(const GraphOutputPortsPtr &outputs) {
        outputs->clear();
        outputs->finish();
    }
    void FakeRunGraphSuccess(const GraphOutputPortsPtr &outputs) {
        outputs->clear();
        TablePtr table(new Table(_poolPtr));
        GraphPort *graphPort1 = new GraphPort("", "");
        graphPort1->_data = table;
        outputs->addPort(graphPort1);
        NaviResultPtr result(new NaviResult());
        GraphPort *graphPort2 = new GraphPort("", "");
        graphPort2->_data = result;
        outputs->addPort(graphPort2);
        outputs->finish();
    }
protected:
    QrsSqlBizPtr _biz;
    TimeoutTerminator *_timeoutTerminator;
    int64_t _timeout;
    std::string _serviceName;
    QrsSqlHandlerPtr _handler;
    std::shared_ptr<autil::mem_pool::Pool> _poolPtr;
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(service, QrsSqlHandlerTest);

void QrsSqlHandlerTest::setUp() {
    std::call_once(iquanFlag, [&]() {
        Status status = IquanEnv::jvmSetup(jt_hdfs_jvm, {}, "");
        if (status.ok()) {
            HA3_LOG(ERROR, "can not start jvm");
        }
    });

    iquan::JniConfig jniConfig;
    iquan::ClientConfig clientConfig;

    HA3_LOG(DEBUG, "setUp!");
    _biz.reset(new QrsSqlBiz());
    _timeoutTerminator = new TimeoutTerminator(1000 * 1000, autil::TimeUtility::currentTime());
    _timeout = 1000;
    _serviceName = "sea";
    _handler.reset(new QrsSqlHandler(_biz, {}, _timeoutTerminator));
    _handler->_qrsSqlBiz->_sqlClientPtr.reset(new Iquan());
    iquan::Status status = _handler->_qrsSqlBiz->_sqlClientPtr->init(jniConfig, clientConfig);
    ASSERT_TRUE(status.ok());
    _poolPtr.reset(new autil::mem_pool::Pool());
}

void QrsSqlHandlerTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");
    _handler->_qrsSqlBiz->_sqlClientPtr = nullptr;
    delete _timeoutTerminator;
}

TEST_F(QrsSqlHandlerTest, testConstruct) {
    ASSERT_TRUE(_handler->_qrsSqlBiz != nullptr);
    ASSERT_TRUE(_timeoutTerminator == _handler->_timeoutTerminator);
    ASSERT_EQ(0, _handler->_finalOutputNum);
}


TEST_F(QrsSqlHandlerTest, testGetFinalOutputNum) {
    _handler->_finalOutputNum = 1;
    ASSERT_EQ(1, _handler->getFinalOutputNum());
}

TEST_F(QrsSqlHandlerTest, testParseIquanSqlParams) {
    _handler->_qrsSqlBiz->_sqlConfigPtr.reset(new SqlConfig());
    {
        sql::SqlQueryRequest sqlRequest;
        QrsSessionSqlRequest request(&sqlRequest);
        iquan::SqlQueryRequest response;
        request.sqlRequest->getKvPair() = {
            {"iquan.first", "first"},
            {"iquan.second", "second"},
            {"iquan.third", "third"},
        };
        ASSERT_TRUE(_handler->parseIquanSqlParams(request, response));
        ASSERT_EQ(5, response.sqlParams.size());
        EXPECT_EQ("first", AnyCast<std::string>(response.sqlParams["iquan.first"]));
        EXPECT_EQ("second", AnyCast<std::string>(response.sqlParams["iquan.second"]));
        EXPECT_EQ("third", AnyCast<std::string>(response.sqlParams["iquan.third"]));
        EXPECT_EQ("jni.post.optimize", AnyCast<std::string>(response.sqlParams["iquan.plan.prepare.level"]));
        EXPECT_EQ("false", AnyCast<std::string>(response.sqlParams["iquan.plan.cache.enable"]));
        ASSERT_EQ(2, response.execParams.size());
        EXPECT_FALSE(response.execParams[SQL_IQUAN_EXEC_PARAMS_SOURCE_ID].empty());
        EXPECT_EQ("empty", response.execParams[SQL_IQUAN_EXEC_PARAMS_SOURCE_SPEC]);
    }
    {
        sql::SqlQueryRequest sqlRequest;
        QrsSessionSqlRequest request(&sqlRequest);
        iquan::SqlQueryRequest response;
        request.sqlRequest->getKvPair() = {
            {"skip.iquan.first", "first"},
            {"iquan.second", "second"},
            {"exec.source.id", "123"},
            {"exec.source.spec", "a123"},
        };
        ASSERT_TRUE(_handler->parseIquanSqlParams(request, response));
        ASSERT_EQ(3, response.sqlParams.size());
        EXPECT_EQ("second", AnyCast<std::string>(response.sqlParams["iquan.second"]));
        ASSERT_EQ(2, response.execParams.size());
        EXPECT_EQ("jni.post.optimize", AnyCast<std::string>(response.sqlParams["iquan.plan.prepare.level"]));
        EXPECT_EQ("false", AnyCast<std::string>(response.sqlParams["iquan.plan.cache.enable"]));
        EXPECT_EQ("123", response.execParams[SQL_IQUAN_EXEC_PARAMS_SOURCE_ID]);
        EXPECT_EQ("a123", response.execParams[SQL_IQUAN_EXEC_PARAMS_SOURCE_SPEC]);
    }
}

TEST_F(QrsSqlHandlerTest, testParseSqlParams) {
    _handler->_qrsSqlBiz->_sqlConfigPtr.reset(new SqlConfig());
    _handler->_qrsSqlBiz->_sqlConfigPtr->iquanPlanCacheEnable = true;
    _handler->_qrsSqlBiz->_sqlConfigPtr->iquanPlanPrepareLevel = "test";
    {
        sql::SqlQueryRequest sqlRequest;
        QrsSessionSqlRequest request(&sqlRequest);
        iquan::SqlQueryRequest response;
        request.sqlRequest->getKvPair() = {
            {"iquan.first", "first"},
            {"iquan.second", "second"},
            {"iquan.third", "third"},
            {"iquan.source.id", "123"},
            {"exec.source.id", "exec.source.id"},
            {"exec.source.id2", "exec.source.id2"},
        };
        ASSERT_TRUE(_handler->parseSqlParams(request, response));
        ASSERT_EQ(8, response.sqlParams.size());
        EXPECT_EQ("first", AnyCast<std::string>(response.sqlParams["iquan.first"]));
        EXPECT_EQ("second", AnyCast<std::string>(response.sqlParams["iquan.second"]));
        EXPECT_EQ("third", AnyCast<std::string>(response.sqlParams["iquan.third"]));
        EXPECT_EQ("123", AnyCast<std::string>(response.sqlParams["iquan.source.id"]));
        EXPECT_EQ("plan_version_0.0.1", AnyCast<std::string>(response.sqlParams["iquan.plan.format.version"]));
        EXPECT_EQ("_summary_", AnyCast<std::string>(response.sqlParams["iquan.optimizer.table.summary.suffix"]));
        EXPECT_EQ("test", AnyCast<std::string>(response.sqlParams["iquan.plan.prepare.level"]));
        EXPECT_EQ("true", AnyCast<std::string>(response.sqlParams["iquan.plan.cache.enable"]));

        EXPECT_EQ("exec.source.id", response.execParams["exec.source.id"]);
        EXPECT_EQ("exec.source.id2", response.execParams["exec.source.id2"]);
    }
    {
        sql::SqlQueryRequest sqlRequest;
        QrsSessionSqlRequest request(&sqlRequest);
        iquan::SqlQueryRequest response;
        request.sqlRequest->getKvPair() = {
            {"dynamic_params", "[[100,"},
        };
        ASSERT_FALSE(_handler->parseSqlParams(request, response));
    }
}

TEST_F(QrsSqlHandlerTest, testParseDynamicParams) {
    {
        sql::SqlQueryRequest sqlRequest;
        QrsSessionSqlRequest request(&sqlRequest);
        vector<vector<autil::legacy::Any>> dynamicParams;
        request.sqlRequest->getKvPair() = {
            {"dynamic_params", "[[100, \"abc\"]]"},
        };
        ASSERT_TRUE(_handler->getSqlDynamicParams(request, dynamicParams));
        ASSERT_EQ(1u, dynamicParams.size());
        ASSERT_EQ(2U, dynamicParams[0].size());
        EXPECT_EQ(100, JsonNumberCast<int>(dynamicParams[0][0]));
        EXPECT_EQ("abc", AnyCast<string>(dynamicParams[0][1]));
    }
    {
        sql::SqlQueryRequest sqlRequest;
        QrsSessionSqlRequest request(&sqlRequest);
        vector<vector<autil::legacy::Any>> dynamicParams;
        request.sqlRequest->getKvPair() = {
            {"dynamic_params", "%5b%5b100%2c+%22abc%22%5d%5d"},
            {"urlencode_data", "true"},
        };
        ASSERT_TRUE(_handler->getSqlDynamicParams(request, dynamicParams));
        ASSERT_EQ(1u, dynamicParams.size());
        ASSERT_EQ(2U, dynamicParams[0].size());
        EXPECT_EQ(100, JsonNumberCast<int>(dynamicParams[0][0]));
        EXPECT_EQ("abc", AnyCast<string>(dynamicParams[0][1]));
    }
    {
        sql::SqlQueryRequest sqlRequest;
        QrsSessionSqlRequest request(&sqlRequest);
        vector<vector<autil::legacy::Any>> dynamicParams;
        request.sqlRequest->getKvPair() = {
            {"dynamic_params", "[[100"},
        };
        ASSERT_FALSE(_handler->getSqlDynamicParams(request, dynamicParams));
    }
}

TEST_F(QrsSqlHandlerTest, testRunGraph) {
    {
        // navi is null
        _handler->_qrsSqlBiz->_naviPtr.reset();
        QrsSessionSqlRequest sessionRequest(nullptr);
        QrsSessionSqlResult sessionResult;
        vector<string> outputPort;
        vector<string> outputNode;
        string desc;
        ASSERT_FALSE(_handler->runGraph(nullptr, sessionRequest, sessionResult,
                        outputPort, outputNode, 0, "", desc));
    }
    {
        // output error
        auto naviMock = new NaviMock();
        _handler->_qrsSqlBiz->_naviPtr.reset(naviMock);
        EXPECT_CALL(*naviMock, runGraph(_, _, _, _, _)).
            WillOnce(Invoke(std::bind(&QrsSqlHandlerTest::FakeRunGraphFailed,
                                    this, placeholders::_3)));
        navi::GraphDef graphDef;
        QrsSessionSqlRequest sessionRequest(nullptr);
        sessionRequest.pool = _poolPtr.get();
        QrsSessionSqlResult sessionResult;
        _handler->_qrsSqlBiz->_sqlConfigPtr.reset(new SqlConfig());
        _handler->_qrsSqlBiz->_sessionResource =
            _handler->_qrsSqlBiz->createSessionResource(1);
        _handler->_qrsSqlBiz->_sessionResource->indexApplication.reset(
                new IE_NAMESPACE(partition)::IndexApplication());
        vector<string> outputPort;
        vector<string> outputNode;
        string desc;
        ASSERT_FALSE(_handler->runGraph(&graphDef, sessionRequest, sessionResult,
                        outputPort, outputNode, 1000, "", desc));
    }
    {
        // success
        auto naviMock = new NaviMock();
        _handler->_qrsSqlBiz->_naviPtr.reset(naviMock);
        EXPECT_CALL(*naviMock, runGraph(_, _, _, _, _)).
            WillOnce(Invoke(std::bind(&QrsSqlHandlerTest::FakeRunGraphSuccess,
                                    this, placeholders::_3)));
        navi::GraphDef graphDef;
        QrsSessionSqlRequest sessionRequest(nullptr);
        sessionRequest.pool = _poolPtr.get();
        QrsSessionSqlResult sessionResult;
        _handler->_qrsSqlBiz->_sqlConfigPtr.reset(new SqlConfig());
        _handler->_qrsSqlBiz->_sessionResource =
            _handler->_qrsSqlBiz->createSessionResource(1);
        _handler->_qrsSqlBiz->_sessionResource->indexApplication.reset(
                new IE_NAMESPACE(partition)::IndexApplication());
        vector<string> outputPort;
        vector<string> outputNode;
        string desc;
        ASSERT_TRUE(_handler->runGraph(&graphDef, sessionRequest, sessionResult,
                        outputPort, outputNode, 1000, "", desc));
    }
}

TEST_F(QrsSqlHandlerTest, testGetGraphDef) {
    {
        // success graph
        _handler->_qrsSqlBiz->_sqlConfigPtr.reset(new SqlConfig());
        _handler->_qrsSqlBiz->_naviPtr.reset(new navi::Navi());
        SqlQueryResponse response;

            std::string planStr = R"json(
{
    "rel_plan_version" : "fake version",
    "rel_plan" :
    [
        {
            "id" : 1,
            "op_name" : "ValuesKernel",
            "inputs" : {
            },
            "attrs" : {
            }
        },
        {
            "id" : 0,
            "op_name" : "UnionKernel",
            "inputs" : {
                "input0" : [
                    1
                ]
            },
            "attrs" : {
                "type" : ["INTEGER"],
                "output_fields" : ["$attr1"]
            }
        }
    ]
}
)json";
        iquan::SqlPlan plan;
        std::shared_ptr<autil::legacy::RapidDocument> docPtr(new RapidDocument);
        buildPlans(planStr, response.result, docPtr);

        SessionMetricsCollectorPtr collector(new SessionMetricsCollector());
        iquan::SqlQueryRequest sqlQueryRequest;
        sql::SqlQueryRequest sqlRequest;
        QrsSessionSqlRequest request(&sqlRequest, nullptr, collector);
        ErrorResult errorResult;
        vector<string> outputPort;
        vector<string> outputNode;
        int64_t leftTime = 1000;
        auto graph = _handler->getGraphDef(response.result, sqlQueryRequest, request,
                leftTime, "", errorResult, outputPort, outputNode);
        ASSERT_TRUE(graph);
        ASSERT_EQ(1, outputPort.size());
        ASSERT_EQ(1, outputNode.size());
        ASSERT_EQ("output0", outputPort[0]);
        ASSERT_EQ("0", outputNode[0]);
        ASSERT_EQ(2, graph->nodes_size());
        ASSERT_EQ("1", graph->nodes(0).name());
        ASSERT_EQ("ValuesKernel", graph->nodes(0).kernel_name());
        ASSERT_EQ("0", graph->nodes(1).name());
        ASSERT_EQ("UnionKernel", graph->nodes(1).kernel_name());
        ASSERT_EQ(1, graph->edges_size());
        ASSERT_EQ("1", graph->edges(0).input().node_name());
        ASSERT_EQ("0", graph->edges(0).output().node_name());
        delete graph;
    }
    {
        // get graph failed
        SqlQueryResponse response;
        SessionMetricsCollectorPtr collector(new SessionMetricsCollector());
        sql::SqlQueryRequest sqlRequest;
        QrsSessionSqlRequest request(&sqlRequest, nullptr, collector);
        ErrorResult errorResult;
        vector<string> outputPort;
        vector<string> outputNode;
        int64_t leftTime = 1000;
        iquan::SqlQueryRequest sqlQueryRequest;
        auto graph = _handler->getGraphDef(response.result, sqlQueryRequest, request,
                leftTime, "", errorResult, outputPort, outputNode);
        ASSERT_TRUE(graph == nullptr);
        EXPECT_EQ(ERROR_SQL_TRANSFER_GRAPH, errorResult.getErrorCode());
        std::string expect("failed to call iquan transform, error code is -30, error message is iquan transform graph fail, sql plan: {\n\"exec_params\":\n  {\n  },\n\"rel_plan\":\n  [\n  ],\n\"rel_plan_version\":\n  \"\"\n}");
        ASSERT_EQ(expect, errorResult.getErrorMsg());
    }
    {
        // timeout
        sleep(2);
        SqlQueryResponse response;
        QrsSessionSqlRequest request(nullptr);
        ErrorResult errorResult;
        vector<string> outputPort;
        vector<string> outputNode;
        iquan::SqlQueryRequest sqlQueryRequest;
        auto graph = _handler->getGraphDef(response.result, sqlQueryRequest, request,
                0, "", errorResult, outputPort, outputNode);
        ASSERT_TRUE(graph == nullptr);
        ASSERT_EQ(ERROR_SQL_PLAN_SERVICE_TIMEOUT, errorResult.getErrorCode());
    }
}

TEST_F(QrsSqlHandlerTest, testGetTraceLevel) {
    {
        sql::SqlQueryRequest sqlRequest;
        sqlRequest.init("query=1&&kvpair=trace:trace3");
        QrsSessionSqlRequest request(&sqlRequest);
        ASSERT_EQ("TRACE3", QrsSqlHandler::getTraceLevel(request));
    }
    {
        sql::SqlQueryRequest sqlRequest;
        sqlRequest.init("query=1&&kvpair=trace:trAce3");
        QrsSessionSqlRequest request(&sqlRequest);
        ASSERT_EQ("TRACE3", QrsSqlHandler::getTraceLevel(request));
    }
    {
        sql::SqlQueryRequest sqlRequest;
        sqlRequest.init("query=1&&kvpair=trace:tracer3");
        QrsSessionSqlRequest request(&sqlRequest);
        ASSERT_EQ("TRACER3", QrsSqlHandler::getTraceLevel(request));
    }
    {
        sql::SqlQueryRequest sqlRequest;
        sqlRequest.init("query=1&&kvpair=trace:debug");
        QrsSessionSqlRequest request(&sqlRequest);
        ASSERT_EQ("DEBUG", QrsSqlHandler::getTraceLevel(request));
    }
    {
        sql::SqlQueryRequest sqlRequest;
        sqlRequest.init("query=1&&kvpair=trace:INFO");
        QrsSessionSqlRequest request(&sqlRequest);
        ASSERT_EQ("INFO", QrsSqlHandler::getTraceLevel(request));
    }
}

TEST_F(QrsSqlHandlerTest, testTraceErrorMsg) {
    {
        // test trace level < debug
        sql::SqlQueryRequest sqlRequest;
        sqlRequest._sqlStr = "123";
        QrsSessionSqlRequest sessionRequest(&sqlRequest);
        SqlQueryResponse sqlQueryResponse;
        QrsSessionSqlResult sessionResult;
        iquan::SqlQueryRequest sqlQueryRequest;
        QrsSqlHandler::traceErrorMsg("INFO",
                                     sqlQueryRequest,
                                     sqlQueryResponse,
                                     sessionRequest,
                                     sessionResult);
        ASSERT_EQ(0, sessionResult.sqlTrace.size());
        ASSERT_TRUE(sessionResult.sqlQuery.empty());
    }
    {
        sql::SqlQueryRequest sqlRequest;
        sqlRequest._sqlStr = "123";
        QrsSessionSqlRequest sessionRequest(&sqlRequest);
        SqlQueryResponse sqlQueryResponse;
        sqlQueryResponse.errorCode = 0;
        QrsSessionSqlResult sessionResult;
        iquan::SqlQueryRequest sqlQueryRequest;
        QrsSqlHandler::traceErrorMsg("debug",
                                     sqlQueryRequest,
                                     sqlQueryResponse,
                                     sessionRequest,
                                     sessionResult);
        ASSERT_EQ(0, sessionResult.sqlTrace.size());
        ASSERT_EQ(sqlRequest._sqlStr, sessionResult.sqlQuery);
    }
    {
        // navi result
        sql::SqlQueryRequest sqlRequest;
        sqlRequest._sqlStr = "123";
        QrsSessionSqlRequest sessionRequest(&sqlRequest);
        iquan::SqlQueryRequest sqlQueryRequest;
        SqlQueryResponse sqlQueryResponse;
        QrsSessionSqlResult sessionResult;
        sessionResult.naviResult.reset(new navi::NaviResult());
        sessionResult.naviResult->_ec = navi::EC_ABORT;
        sessionResult.naviResult->_msg = "456";
        QrsSqlHandler::traceErrorMsg("TRACE3",
                                     sqlQueryRequest,
                                     sqlQueryResponse,
                                     sessionRequest,
                                     sessionResult);
        ASSERT_EQ(sqlRequest._sqlStr, sessionResult.sqlQuery);
        ASSERT_EQ(1, sessionResult.sqlTrace.size());
        ASSERT_EQ("456", sessionResult.sqlTrace.front());
    }
}

TEST_F(QrsSqlHandlerTest, testCheckTimeOutWithError) {
    common::ErrorResult errorResult;
    ASSERT_TRUE(_handler->checkTimeOutWithError(errorResult, ERROR_SQL_PLAN_SERVICE_TIMEOUT));
    ASSERT_EQ(ERROR_NONE, errorResult.getErrorCode());
    sleep(2);
    ASSERT_FALSE(_handler->checkTimeOutWithError(errorResult, ERROR_SQL_PLAN_SERVICE_TIMEOUT));
    ASSERT_EQ(ERROR_SQL_PLAN_SERVICE_TIMEOUT , errorResult.getErrorCode());
    ASSERT_EQ("call sql plan service timeout.", errorResult.getErrorDescription());
    ASSERT_FALSE(_handler->checkTimeOutWithError(errorResult, ERROR_SQL_RUN_GRAPH_TIMEOUT));
    ASSERT_EQ(ERROR_SQL_RUN_GRAPH_TIMEOUT, errorResult.getErrorCode());
    ASSERT_EQ("run sql graph timeout failed.", errorResult.getErrorDescription());
}

TEST_F(QrsSqlHandlerTest, testFillSessionSqlResult) {
    {
        // success
        navi::GraphOutputPortsPtr outputs(new navi::GraphOutputPorts());
        navi::GraphPort *port0 = new navi::GraphPort("", "");
        TablePtr table(new Table(_poolPtr));
        port0->setData(table);
        navi::GraphPort *port1 = new navi::GraphPort("", "");
        navi::NaviResultPtr result(new navi::NaviResult());
        port1->setData(result);
        outputs->addPort(port0);
        outputs->addPort(port1);
        QrsSessionSqlResult sessionResult;
        string desc;
        ASSERT_TRUE(QrsSqlHandler::fillSessionSqlResult(outputs, sessionResult, desc));
        ASSERT_EQ(table.get(), sessionResult.table.get());
        ASSERT_EQ(result.get(), sessionResult.naviResult.get());
    }
    {
        // navi result failed
        navi::GraphOutputPortsPtr outputs(new navi::GraphOutputPorts());
        navi::GraphPort *port0 = new navi::GraphPort("", "");
        TablePtr table(new Table(_poolPtr));
        port0->setData(table);
        navi::GraphPort *port1 = new navi::GraphPort("", "");
        TablePtr table2(new Table(_poolPtr));
        port1->setData(table2);
        outputs->addPort(port0);
        outputs->addPort(port1);
        QrsSessionSqlResult sessionResult;
        string desc;
        ASSERT_FALSE(QrsSqlHandler::fillSessionSqlResult(outputs, sessionResult, desc));
    }
    {
        // navi result has error
        navi::GraphOutputPortsPtr outputs(new navi::GraphOutputPorts());
        navi::GraphPort *port0 = new navi::GraphPort("", "");
        TablePtr table(new Table(_poolPtr));
        port0->setData(table);
        navi::GraphPort *port1 = new navi::GraphPort("", "");
        navi::NaviResultPtr result(new navi::NaviResult());
        result->_ec = navi::EC_ABORT;
        port1->setData(result);
        outputs->addPort(port0);
        outputs->addPort(port1);
        QrsSessionSqlResult sessionResult;
        string desc;
        ASSERT_FALSE(QrsSqlHandler::fillSessionSqlResult(outputs, sessionResult, desc));
    }
    {
        // table is null
        navi::GraphOutputPortsPtr outputs(new navi::GraphOutputPorts());
        navi::GraphPort *port0 = new navi::GraphPort("", "");
        navi::NaviResultPtr result0(new navi::NaviResult());
        port0->setData(result0);
        navi::GraphPort *port1 = new navi::GraphPort("", "");
        navi::NaviResultPtr result(new navi::NaviResult());
        port1->setData(result);
        outputs->addPort(port0);
        outputs->addPort(port1);
        QrsSessionSqlResult sessionResult;
        string desc;
        ASSERT_FALSE(QrsSqlHandler::fillSessionSqlResult(outputs, sessionResult, desc));
    }
}

TEST_F(QrsSqlHandlerTest, testGetParallelNum) {
    {
        sql::SqlQueryRequest request;
        request.init("query=1&&kvpair=parallel:5");
        SqlConfigPtr sqlConfig(new SqlConfig());
        ASSERT_EQ(5, QrsSqlHandler::getParallelNum(&request, sqlConfig));
    }
    {
        sql::SqlQueryRequest request;
        request.init("query=1&&kvpair=parallel:5");
        SqlConfigPtr sqlConfig(new SqlConfig());
        sqlConfig->parallelNum = 6;
        ASSERT_EQ(5, QrsSqlHandler::getParallelNum(&request, sqlConfig));
    }
    {
        sql::SqlQueryRequest request;
        request.init("query=1");
        SqlConfigPtr sqlConfig(new SqlConfig());
        sqlConfig->parallelNum = 6;
        ASSERT_EQ(6, QrsSqlHandler::getParallelNum(&request, sqlConfig));
    }
    {
        sql::SqlQueryRequest request;
        request.init("query=1&&kvpair=parallel:-1");
        SqlConfigPtr sqlConfig(new SqlConfig());
        ASSERT_EQ(1, QrsSqlHandler::getParallelNum(&request, sqlConfig));
    }
    {
        sql::SqlQueryRequest request;
        request.init("query=1&&kvpair=parallel:100");
        SqlConfigPtr sqlConfig(new SqlConfig());
        ASSERT_EQ(16, QrsSqlHandler::getParallelNum(&request, sqlConfig));
    }
}

END_HA3_NAMESPACE(service);
