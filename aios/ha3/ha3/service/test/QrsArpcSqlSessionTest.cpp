#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include <ha3/config/SqlConfig.h>
#include <ha3/service/QrsArpcSqlSession.h>
#include <ha3/service/SessionPool.h>
#include <ha3/service/QrsSessionSqlResult.h>
#include <ha3/sql/framework/SqlQueryRequest.h>
#include <ha3/common/SqlAccessLog.h>
#include <multi_call/rpc/GigClosure.h>

using namespace std;
using namespace testing;
using namespace suez::turing;

USE_HA3_NAMESPACE(proto);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(turing);
USE_HA3_NAMESPACE(sql);

BEGIN_HA3_NAMESPACE(service);

class MockGigClosure : public multi_call::GigClosure {
public:
    MOCK_METHOD0(Run, void());
    MOCK_METHOD0(getProtocolType, multi_call::ProtocolType());
};

class QrsArpcSqlSessionTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
private:
    SessionPoolImpl<QrsArpcSqlSession> *_sessionPool;
    QrsArpcSqlSession *_session;
    QrsResponse *_response;
    suez::turing::PoolCachePtr _poolCache;
private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(service, QrsArpcSqlSessionTest);

void QrsArpcSqlSessionTest::setUp() {
    _poolCache.reset(new suez::turing::PoolCache());
    _sessionPool = new SessionPoolImpl<QrsArpcSqlSession>;
    _session = _sessionPool->get();
    _session->setPoolCache(_poolCache);
    _response = new QrsResponse();
    _session->_qrsResponse = _response;
}

void QrsArpcSqlSessionTest::tearDown() {
    DELETE_AND_SET_NULL(_sessionPool);
    DELETE_AND_SET_NULL(_response);
}

TEST_F(QrsArpcSqlSessionTest, testConstruct) {
    auto sessionPool = new SessionPoolImpl<QrsArpcSqlSession>;
    QrsArpcSqlSession *session = sessionPool->get();
    ASSERT_TRUE(session->_qrsResponse == nullptr);
    ASSERT_TRUE(session->_done == nullptr);
    ASSERT_TRUE(session->_timeout == -1);
    ASSERT_TRUE(session->_timeoutTerminator == nullptr);
    ASSERT_TRUE(session->_sqlQueryRequest == nullptr);
    ASSERT_TRUE(session->_accessLog == nullptr);
    ASSERT_TRUE(session->_sessionPool != nullptr);
    ASSERT_TRUE(session->_startTime == 0);
    ASSERT_TRUE(session->_pool == nullptr);
    delete sessionPool;
}

TEST_F(QrsArpcSqlSessionTest, testSetRequest) {
    proto::QrsRequest request;
    request.set_assemblyquery("xxx");
    request.set_timeout(10);
    auto closure = new MockGigClosure();
    _session->setRequest(&request, nullptr, closure,
                         turing::QrsServiceSnapshotPtr());
    ASSERT_EQ(10000, _session->_timeout);
    ASSERT_EQ("xxx", _session->_queryStr);
    delete closure;
}

TEST_F(QrsArpcSqlSessionTest, testSimple) {
    ASSERT_FALSE(_session->beginSession());
    ErrorResult errorResult = _session->_errorResult;
    ASSERT_EQ(ERROR_SQL_INIT_REQUEST, errorResult.getErrorCode());
    QrsSessionSqlResult result;
    result.errorResult = errorResult;
    _session->endQrsSession(result);
    ASSERT_EQ(multi_call::MULTI_CALL_ERROR_RPC_FAILED, result.multicallEc);
    ASSERT_EQ(QrsSessionSqlResult::SqlResultFormatType::SQL_RF_STRING,
              result.formatType);
    std::string::size_type pos = result.resultStr.find("ERROR_NONE");
    ASSERT_EQ(pos, std::string::npos);
}

TEST_F(QrsArpcSqlSessionTest, testBeginSession1) {
    _session->_snapshot.reset(new turing::QrsServiceSnapshot(true));
    QrsSqlBizPtr biz(new QrsSqlBiz());
    _session->_snapshot->_bizMap["." + DEFAULT_SQL_BIZ_NAME] = biz;
    _session->_queryStr = "kvpair=x";
    ASSERT_FALSE(_session->beginSession());
    ErrorResult errorResult = _session->_errorResult;
    ASSERT_EQ(ERROR_SQL_INIT_REQUEST, errorResult.getErrorCode());
}

TEST_F(QrsArpcSqlSessionTest, testBeginSession2) {
    _session->_snapshot.reset(new turing::QrsServiceSnapshot(true));
    QrsSqlBizPtr biz(new QrsSqlBiz());
    _session->_snapshot->_bizMap["." + DEFAULT_SQL_BIZ_NAME] = biz;
    _session->_queryStr = "query=xxx";
    EXPECT_TRUE(_session->beginSession());
    auto sqlQueryRequest = _session->_sqlQueryRequest;
    ASSERT_EQ("xxx", (sqlQueryRequest->getSqlQuery()));
}

TEST_F(QrsArpcSqlSessionTest, testBeginSession3) {
    _session->_snapshot.reset(new turing::QrsServiceSnapshot(true));
    QrsSqlBizPtr biz(new QrsSqlBiz());
    _session->_snapshot->_bizMap["." + DEFAULT_SQL_BIZ_NAME] = biz;
    _session->_queryStr = "query=xxx&&kvpair=bizName:x";
    ASSERT_FALSE(_session->beginSession());
    ErrorResult errorResult = _session->_errorResult;
    ASSERT_EQ(ERROR_BIZ_NOT_FOUND, errorResult.getErrorCode());
}

TEST_F(QrsArpcSqlSessionTest, testEndQrsSession) {
    QrsSessionSqlResult result;
    _session->endQrsSession(result);
    string str = "\n------------------------------- TRACE INFO ---------------------------\n";
    EXPECT_EQ(str, result.resultStr);
}

TEST_F(QrsArpcSqlSessionTest, testEndQrsSessionDestruct) {
    QrsSessionSqlResult result;
    result.errorResult = ErrorResult(ERROR_SQL_PLAN_SERVICE, "parse sql result failed.");
    _session->beginSession();
    _session->setClientAddress("xxx");
    _session->_queryStr = "yyy";
    _session->endQrsSession(result);
    string str = "parse sql result failed.";
    EXPECT_TRUE(result.resultStr.find(str) != string::npos);
    ASSERT_TRUE(_session->_pool == nullptr);
    ASSERT_TRUE(_session->_snapshot == nullptr);
    ASSERT_TRUE(_session->_qrsResponse == nullptr);
    ASSERT_TRUE(_session->_done == nullptr);
    ASSERT_TRUE(_session->_poolCache == nullptr);
    ASSERT_TRUE(_session->_sqlQueryRequest == nullptr);
    ASSERT_TRUE(_session->_qrsSqlBiz == nullptr);
    ASSERT_TRUE(_session->_accessLog == nullptr);
}

TEST_F(QrsArpcSqlSessionTest, testGetFormatType) {
    sql::SqlQueryRequest *sqlQueryRequest(new sql::SqlQueryRequest());
    {
        auto ret = QrsArpcSqlSession::getFormatType(nullptr, nullptr);
        ASSERT_EQ(QrsSessionSqlResult::SQL_RF_STRING, ret);
    }
    {
        auto ret = QrsArpcSqlSession::getFormatType(sqlQueryRequest, QrsSqlBizPtr());
        ASSERT_EQ(QrsSessionSqlResult::SQL_RF_STRING, ret);
    }
    {
        QrsSqlBizPtr qrsSqlBiz(new QrsSqlBiz());
        qrsSqlBiz->_sqlConfigPtr.reset(new SqlConfig());
        auto ret = QrsArpcSqlSession::getFormatType(sqlQueryRequest, qrsSqlBiz);
        ASSERT_EQ(QrsSessionSqlResult::SQL_RF_STRING, ret);
    }
    {
        QrsSqlBizPtr qrsSqlBiz(new QrsSqlBiz());
        qrsSqlBiz->_sqlConfigPtr.reset(new SqlConfig());
        qrsSqlBiz->_sqlConfigPtr->outputFormat = "string";
        auto ret = QrsArpcSqlSession::getFormatType(sqlQueryRequest, qrsSqlBiz);
        ASSERT_EQ(QrsSessionSqlResult::SQL_RF_STRING, ret);
    }
    {
        QrsSqlBizPtr qrsSqlBiz(new QrsSqlBiz());
        qrsSqlBiz->_sqlConfigPtr.reset(new SqlConfig());
        qrsSqlBiz->_sqlConfigPtr->outputFormat = "xxx";
        auto ret = QrsArpcSqlSession::getFormatType(sqlQueryRequest, qrsSqlBiz);
        ASSERT_EQ(QrsSessionSqlResult::SQL_RF_STRING, ret);
    }
    {
        QrsSqlBizPtr qrsSqlBiz(new QrsSqlBiz());
        qrsSqlBiz->_sqlConfigPtr.reset(new SqlConfig());
        qrsSqlBiz->_sqlConfigPtr->outputFormat = "json";
        auto ret = QrsArpcSqlSession::getFormatType(sqlQueryRequest, qrsSqlBiz);
        ASSERT_EQ(QrsSessionSqlResult::SQL_RF_JSON, ret);
    }
    delete sqlQueryRequest;
}

TEST_F(QrsArpcSqlSessionTest, testInitTimeoutTerminator) {
    HA3_LOG(DEBUG, "Begin Test!");
    _session->_pool = _poolCache->get();
    {
        _session->_timeout = 10;
        _session->setStartTime(0);

        ASSERT_TRUE(_session->initTimeoutTerminator(-1));
        ASSERT_TRUE(_session->initTimeoutTerminator(0));
        ASSERT_TRUE(_session->initTimeoutTerminator(1));
        ASSERT_TRUE(_session->initTimeoutTerminator(9));
        ASSERT_FALSE(_session->initTimeoutTerminator(10));
        ASSERT_FALSE(_session->initTimeoutTerminator(11));
    }
    {
        _session->_timeout = -1;
        _session->setStartTime(0);

        ASSERT_TRUE(_session->initTimeoutTerminator(-1));
        ASSERT_TRUE(_session->initTimeoutTerminator(0));
        ASSERT_TRUE(_session->initTimeoutTerminator(1));
    }
}

END_HA3_NAMESPACE();
