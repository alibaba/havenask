#include <unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/service/QrsArpcSession.h>
#include <ha3/service/SessionPool.h>
#include <ha3/proto/test/ARPCMessageCreator.h>
#include <ha3/util/HaCompressUtil.h>
#include <ha3/common/XMLResultFormatter.h>

using namespace std;
using namespace anet;
using namespace arpc;
using namespace autil;

USE_HA3_NAMESPACE(util);
USE_HA3_NAMESPACE(proto);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(config);

BEGIN_HA3_NAMESPACE(service);

class QrsArpcSessionTest : public TESTBASE {
public:
    QrsArpcSessionTest() {}
    ~QrsArpcSessionTest() {}
public:
    void setUp() {}
    void tearDown() {}
private:
    SessionPoolImpl<QrsArpcSession>* _sessionPool;
    QrsArpcSession* _session;
    QrsResponse *_response;
    suez::turing::PoolCachePtr _poolCache;
    turing::QrsServiceSnapshotPtr _snapshot;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(service, QrsArpcSessionTest);

TEST_F(QrsArpcSessionTest, testDropSession) {
    {
        SessionPoolImpl<QrsArpcSession>* sessionPool = new SessionPoolImpl<QrsArpcSession>();
        QrsArpcSession *session = sessionPool->get();
        session->dropRequest();
        delete sessionPool;
    }
    {
        SessionPoolImpl<QrsArpcSession>* sessionPool = new SessionPoolImpl<QrsArpcSession>();
        QrsArpcSession *session = sessionPool->get();
        session->_poolCache.reset(new suez::turing::PoolCache());
        session->dropRequest();
        delete sessionPool;
    }
}

/*
QrsArpcSessionTest::QrsArpcSessionTest() {
    _sessionPool = new SessionPoolImpl<QrsArpcSession>;
    _poolCache.reset(new suez::turing::PoolCache());
    _session = _sessionPool->get();
    _snapshot.reset(new turing::QrsServiceSnapshot(false));
    _snapshot->_bizMap[".default"].reset(new turing::QrsBiz());
}

QrsArpcSessionTest::~QrsArpcSessionTest() {
    DELETE_AND_SET_NULL(_sessionPool);
}

void QrsArpcSessionTest::setUp() {
    HA3_LOG(DEBUG, "setUp!");
    _sessionPool = new SessionPoolImpl<QrsArpcSession>;
    _session = _sessionPool->get();
    _session->setPoolCache(_poolCache);
    _session->_snapshot = _snapshot;
    _session->_bizName = "default";
    _response = new QrsResponse();
    _session->_qrsResponse = _response;
    ASSERT_TRUE(_session->beginSession());
}

void QrsArpcSessionTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");
    if (_session) {
        QrsSessionSearchResult result =
            QrsSessionSearchResult(XMLResultFormatter::xmlFormatErrorResult(
                            ErrorResult(ERROR_UNKNOWN, "unknown")));
        _session->endQrsSession(result);
    }
    DELETE_AND_SET_NULL(_response);
}

TEST_F(QrsArpcSessionTest, testQrsSessionSearchResult) {
    {
      QrsSessionSearchResult result;
      ASSERT_EQ(multi_call::MULTI_CALL_ERROR_RPC_FAILED, result.multicallEc);
      ASSERT_EQ("", result.resultStr);
      ASSERT_EQ("", result.errorStr);
      ASSERT_EQ(NO_COMPRESS, result.resultCompressType);
      ASSERT_EQ(RF_XML, result.formatType);
    }
    {
      QrsSessionSearchResult result("resultStr", Z_SPEED_COMPRESS, RF_PROTOBUF);
      ASSERT_EQ(multi_call::MULTI_CALL_ERROR_RPC_FAILED, result.multicallEc);
      ASSERT_EQ("resultStr", result.resultStr);
      ASSERT_EQ(Z_SPEED_COMPRESS, result.resultCompressType);
      ASSERT_EQ(RF_PROTOBUF, result.formatType);

      auto result1 = result;
      ASSERT_EQ(multi_call::MULTI_CALL_ERROR_RPC_FAILED, result1.multicallEc);
    }
}

TEST_F(QrsArpcSessionTest, testFillResponse) {
    HA3_LOG(DEBUG, "Begin Test!");
    QrsResponse response;
    string result = "aaaaaaaaaaaaaaaaaaaaaaaaaresultstring";
    //xml
    _session->fillResponse(result, Z_SPEED_COMPRESS, RF_XML, &response);
    ASSERT_EQ(FT_XML, response.formattype());
    ASSERT_EQ(CT_Z_SPEED_COMPRESS, response.compresstype());
    string compressedResult;
    util::HaCompressUtil::compress(result, Z_SPEED_COMPRESS, compressedResult);
    ASSERT_EQ(compressedResult, response.assemblyresult());

    //protobuf
    _session->fillResponse(result, Z_SPEED_COMPRESS, RF_PROTOBUF, &response);
    ASSERT_EQ(FT_PROTOBUF, response.formattype());
    ASSERT_EQ(CT_Z_SPEED_COMPRESS, response.compresstype());
    ASSERT_EQ(compressedResult, response.assemblyresult());

    multi_call::GigAgent gigAgent;
    //no_compress
    _session->fillResponse(result, NO_COMPRESS, RF_PROTOBUF, &response);
    _session->_gigQuerySession.reset();
    _session->_done = NULL;
    ASSERT_EQ(FT_PROTOBUF, response.formattype());
    ASSERT_EQ(CT_NO_COMPRESS, response.compresstype());
    ASSERT_EQ(result, response.assemblyresult());
    EXPECT_TRUE(response.gigresponseinfo().size() > 0);
}

TEST_F(QrsArpcSessionTest, testConvertFormatType) {
    HA3_LOG(DEBUG, "Begin Test!");
    QrsArpcSession session;
    ASSERT_EQ(FT_XML, session.convertFormatType(RF_XML));
    ASSERT_EQ(FT_PROTOBUF, session.convertFormatType(RF_PROTOBUF));
}

TEST_F(QrsArpcSessionTest, testInitTimeoutTerminator) {
    HA3_LOG(DEBUG, "Begin Test!");
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
*/
END_HA3_NAMESPACE(service);
