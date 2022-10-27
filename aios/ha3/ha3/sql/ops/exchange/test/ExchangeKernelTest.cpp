#include <ha3/sql/ops/test/OpTestBase.h>
#include <ha3/sql/ops/exchange/ExchangeKernel.h>
#include <ha3/sql/ops/exchange/Ha3QrsSqlRequestGenerator.h>
#include <ha3/sql/common/common.h>
#include <ha3/turing/variant/SqlTableVariant.h>
#include <ha3/turing/variant/SqlResultVariant.h>
#include <ha3/turing/variant/Ha3ResultVariant.h>
#include <multi_call/interface/Reply.h>

using namespace std;
using namespace matchdoc;
using namespace suez::turing;
using namespace navi;
using namespace tensorflow;
using namespace autil::legacy;
using namespace autil::legacy::json;

USE_HA3_NAMESPACE(service);
USE_HA3_NAMESPACE(turing);
USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(sql);

class MockReply: public multi_call::Reply {
public:
    MOCK_METHOD1(getResponses, std::vector<multi_call::ResponsePtr>(size_t &));
};

class ExchangeKernelTest : public OpTestBase {
public:
    ExchangeKernelTest();
    ~ExchangeKernelTest();
public:
    void setUp() override {
    }
    void tearDown() override {
        _table.reset();
    }
private:
    void prepareAttributeMap() {
        _attributeMap.clear();
        _attributeMap["db_name"] = string("Foo");
        _attributeMap[EXCHANGE_INPUT_PORT_ATTR] = string("output0");
        _attributeMap[EXCHANGE_INPUT_NODE_ATTR] = string("scan");
        _attributeMap[SUB_GRAPH_RUN_PARAM_TIMEOUT] = Any(1);
        _attributeMap[SUB_GRAPH_RUN_PARAM_TRACE_LEVEL] = string("");
        _attributeMap[SUB_GRAPH_RUN_PARAM_THREAD_LIMIT] = Any(1);
        _attributeMap[EXCHANGE_LACK_RESULT_ENABLE] = false;

    }

    void prepareTable() {
        vector<MatchDoc> docs = getMatchDocs(_allocator, 4);
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, docs, "id", {1, 2, 3, 4}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int64_t>(_allocator, docs, "a", {5, 6, 7, 8}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator(_allocator, docs, "b", {"b1", "b2", "b3", "b4"}));
        ASSERT_NO_FATAL_FAILURE(extendMultiValueMatchDocAllocator<char>(_allocator, docs, "c", {{'c', '1'}, {'2'}, {'3'}, {'4'}}));
        _table.reset(new Table(docs, _allocator));
    }

    void setResource(KernelTesterBuilder &testerBuilder) {
        for (auto &resource : _resourceMap) {
            testerBuilder.resource(resource.first, resource.second);
        }
    }

    KernelTesterPtr buildTester(KernelTesterBuilder &builder) {
        setResource(builder);
        builder.kernel("ExchangeKernel");
        builder.output("output0");
        builder.attrs(autil::legacy::ToJsonString(_attributeMap));
        builder.binaryAttrsFromMap({{EXCHANGE_GRAPH_ATTR, "foo"}});
        return KernelTesterPtr(builder.build());
    }

public:
    MatchDocAllocatorPtr _allocator;
    TablePtr _table;
};

ExchangeKernelTest::ExchangeKernelTest() {
}

ExchangeKernelTest::~ExchangeKernelTest() {
}

TEST_F(ExchangeKernelTest, testInit) {
    prepareAttributeMap();
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    auto kernel = dynamic_cast<ExchangeKernel *>(tester.getKernel());
    ASSERT_TRUE(nullptr != kernel);
    ASSERT_EQ("Foo.default_sql", kernel->_bizName);
    ASSERT_TRUE(nullptr != kernel->_gigQuerySession);
    ASSERT_TRUE(nullptr != kernel->_pool);
}

TEST_F(ExchangeKernelTest, testInitInvalidQueryResource) {
    prepareAttributeMap();
    _resourceMap.erase(_sqlQueryResource->getResourceName());
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    ASSERT_TRUE(tester.hasError());
    ASSERT_EQ("get sql query resource failed", tester.getErrorMessage());
}

TEST_F(ExchangeKernelTest, testInitInvalidPool) {
    prepareAttributeMap();
    _sqlQueryResource->_queryResource->setPool(nullptr);
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    ASSERT_TRUE(tester.hasError());
    ASSERT_EQ("get pool failed", tester.getErrorMessage());
}

TEST_F(ExchangeKernelTest, testGetSqlGraphResponse) {
    ExchangeKernel kernel;
    SqlGraphResponse sqlResponse;
    {
        navi::NaviLoggerProvider provider("WARN");
        // invalid qrs response
        ASSERT_FALSE(kernel.getSqlGraphResponse(multi_call::ResponsePtr(), sqlResponse));
        CHECK_TRACE_COUNT(1, "invalid qrs response", provider.getTrace(""));
    }
    {
        navi::NaviLoggerProvider provider("WARN");
        // get qrs response message failed
        std::shared_ptr<google::protobuf::Arena> arena(
            new google::protobuf::Arena());
        multi_call::ResponsePtr response(new QrsResponse(arena));
        ASSERT_FALSE(kernel.getSqlGraphResponse(response, sqlResponse));
        CHECK_TRACE_COUNT(1, "get qrs response message failed",
                provider.getTrace(""));
    }
    {
        navi::NaviLoggerProvider provider("WARN");
        // get qrs response message failed
        std::shared_ptr<google::protobuf::Arena> arena(
            new google::protobuf::Arena());
        QrsResponse *qrsResponse = new QrsResponse(arena);
        GraphResponse *graphResponse = new GraphResponse();
        suez::turing::ErrorInfo *errInfo = graphResponse->mutable_errorinfo();
        errInfo->set_errorcode(RS_ERROR_SERVICE_NOT_READY);
        errInfo->set_errormsg("service not ready");
        qrsResponse->init(graphResponse);
        multi_call::ResponsePtr response(qrsResponse);
        ASSERT_FALSE(kernel.getSqlGraphResponse(response, sqlResponse));
        CHECK_TRACE_COUNT(1, "graph response has error: service not ready",
                provider.getTrace(""));
    }
    {
        navi::NaviLoggerProvider provider("WARN");
        // get qrs response message failed
        std::shared_ptr<google::protobuf::Arena> arena(
            new google::protobuf::Arena());
        QrsResponse *qrsResponse = new QrsResponse(arena);
        NamedTensorProto *fakeGraphResponse = new NamedTensorProto();
        qrsResponse->init(fakeGraphResponse);
        multi_call::ResponsePtr response(qrsResponse);
        ASSERT_FALSE(kernel.getSqlGraphResponse(response, sqlResponse));
        CHECK_TRACE_COUNT(1, "get qrs response message failed",
                provider.getTrace(""));
    }
    {
        navi::NaviLoggerProvider provider("WARN");
        // expect message output size 1, actually 0
        std::shared_ptr<google::protobuf::Arena> arena(
            new google::protobuf::Arena());
        QrsResponse *qrsResponse = new QrsResponse(arena);
        GraphResponse *graphResponse = new GraphResponse();
        qrsResponse->init(graphResponse);
        multi_call::ResponsePtr response(qrsResponse);
        ASSERT_FALSE(kernel.getSqlGraphResponse(response, sqlResponse));
        CHECK_TRACE_COUNT(1, "expect message output size 1, actually 0",
                provider.getTrace(""));
    }
    {
        navi::NaviLoggerProvider provider("WARN");
        // invalid sql response tensor type
        std::shared_ptr<google::protobuf::Arena> arena(
            new google::protobuf::Arena());
        QrsResponse *qrsResponse = new QrsResponse(arena);
        GraphResponse *graphResponse = new GraphResponse();
        suez::turing::NamedTensorProto *tensorProto = graphResponse->add_outputs();
        ASSERT_TRUE(tensorProto);
        qrsResponse->init(graphResponse);
        multi_call::ResponsePtr response(qrsResponse);
        ASSERT_FALSE(kernel.getSqlGraphResponse(response, sqlResponse));
        CHECK_TRACE_COUNT(1, "invalid sql response tensor type",
                provider.getTrace(""));
    }
    {
        navi::NaviLoggerProvider provider("WARN");
        // invalid sql response tensor type
        std::shared_ptr<google::protobuf::Arena> arena(
            new google::protobuf::Arena());
        QrsResponse *qrsResponse = new QrsResponse(arena);
        GraphResponse *graphResponse = new GraphResponse();
        suez::turing::NamedTensorProto *namedTensorProto = graphResponse->add_outputs();
        ASSERT_TRUE(namedTensorProto);
        namedTensorProto->mutable_tensor();
        qrsResponse->init(graphResponse);
        multi_call::ResponsePtr response(qrsResponse);
        ASSERT_FALSE(kernel.getSqlGraphResponse(response, sqlResponse));
        CHECK_TRACE_COUNT(1, "invalid sql response tensor type",
                provider.getTrace(""));
    }
    {
        navi::NaviLoggerProvider provider("WARN");
        // parse sql response failed
        std::shared_ptr<google::protobuf::Arena> arena(
            new google::protobuf::Arena());
        QrsResponse *qrsResponse = new QrsResponse(arena);
        GraphResponse *graphResponse = new GraphResponse();
        suez::turing::NamedTensorProto *namedTensorProto = graphResponse->add_outputs();
        ASSERT_TRUE(namedTensorProto);
        TensorProto *tensorProto = namedTensorProto->mutable_tensor();
        Tensor t(DT_STRING, {1});
        t.scalar<string>()() = "foo";
        t.AsProtoField(tensorProto);
        qrsResponse->init(graphResponse);
        multi_call::ResponsePtr response(qrsResponse);
        ASSERT_FALSE(kernel.getSqlGraphResponse(response, sqlResponse));
        CHECK_TRACE_COUNT(1, "parse sql response failed",
                provider.getTrace(""));
    }
    {
        std::shared_ptr<google::protobuf::Arena> arena(
            new google::protobuf::Arena());
        QrsResponse *qrsResponse = new QrsResponse(arena);
        GraphResponse *graphResponse = new GraphResponse();
        suez::turing::NamedTensorProto *namedTensorProto = graphResponse->add_outputs();
        ASSERT_TRUE(namedTensorProto);
        TensorProto *tensorProto = namedTensorProto->mutable_tensor();
        Tensor t(DT_STRING, {1});
        t.scalar<string>()() = "";
        t.AsProtoField(tensorProto);
        qrsResponse->init(graphResponse);
        multi_call::ResponsePtr response(qrsResponse);
        ASSERT_TRUE(kernel.getSqlGraphResponse(response, sqlResponse));
    }
}

TEST_F(ExchangeKernelTest, testGetTableFromTensorProto) {
    ExchangeKernel kernel;
    TablePtr table;
    kernel._memoryPoolResource = &_memPoolResource;
    {
        // sql response has no data
        navi::NaviLoggerProvider provider("WARN");
        HA3_NS(sql)::NamedTensorProto tensorProto;
        ASSERT_FALSE(kernel.getTableFromTensorProto(tensorProto, table));
        CHECK_TRACE_COUNT(1, "sql response has no data",
                provider.getTrace(""));
    }
    {
        // invalid tensor type
        navi::NaviLoggerProvider provider("WARN");
        HA3_NS(sql)::NamedTensorProto tensorProto;
        tensorProto.mutable_data();
        ASSERT_FALSE(kernel.getTableFromTensorProto(tensorProto, table));
        CHECK_TRACE_COUNT(1, "invalid tensor type",
                provider.getTrace(""));
    }
    {
        // invalid tensor type
        navi::NaviLoggerProvider provider("WARN");
        HA3_NS(sql)::NamedTensorProto tensorProto;
        auto data = tensorProto.mutable_data();
        Tensor t(DT_STRING, {1});
        t.scalar<string>()() = "";
        t.AsProtoField(data);
        ASSERT_FALSE(kernel.getTableFromTensorProto(tensorProto, table));
        CHECK_TRACE_COUNT(1, "invalid tensor type", provider.getTrace(""));
    }
    {
        // invalid sql response
        navi::NaviLoggerProvider provider("WARN");
        HA3_NS(sql)::NamedTensorProto tensorProto;
        auto data = tensorProto.mutable_data();
        Tensor t(DT_VARIANT, {1});
        t.AsProtoField(data);
        ASSERT_FALSE(kernel.getTableFromTensorProto(tensorProto, table));
        CHECK_TRACE_COUNT(1, "invalid sql response", provider.getTrace(""));
    }
    {
        // invalid sql table variant
        navi::NaviLoggerProvider provider("WARN");
        HA3_NS(sql)::NamedTensorProto tensorProto;
        auto data = tensorProto.mutable_data();
        Tensor t(DT_VARIANT, {1});
        ResultPtr result(new Result());
        t.scalar<Variant>()() = Ha3ResultVariant(result, &_pool);
        t.AsProtoField(data);
        ASSERT_FALSE(kernel.getTableFromTensorProto(tensorProto, table));
        CHECK_TRACE_COUNT(1, "invalid sql table variant",
                provider.getTrace(""));
    }
    {
        kernel._pool = _poolPtr.get();
        kernel._tablePool = _memPoolResource.getPool();
        HA3_NS(sql)::NamedTensorProto tensorProto;
        auto data = tensorProto.mutable_data();
        Tensor t(DT_VARIANT, {1});
        t.scalar<Variant>()() = SqlTableVariant(_poolPtr);
        t.AsProtoField(data);
        ASSERT_TRUE(kernel.getTableFromTensorProto(tensorProto, table));
    }
}

TEST_F(ExchangeKernelTest, testGetResultFromTensorProto) {
    ExchangeKernel kernel;
    navi::NaviResultPtr result;
    {
        // sql response has no data
        navi::NaviLoggerProvider provider("WARN");
        HA3_NS(sql)::NamedTensorProto tensorProto;
        ASSERT_FALSE(kernel.getResultFromTensorProto(tensorProto, result));
        CHECK_TRACE_COUNT(1, "sql response has no data",
                provider.getTrace(""));
    }
    {
        // invalid tensor type
        navi::NaviLoggerProvider provider("WARN");
        HA3_NS(sql)::NamedTensorProto tensorProto;
        tensorProto.mutable_data();
        ASSERT_FALSE(kernel.getResultFromTensorProto(tensorProto, result));
        CHECK_TRACE_COUNT(1, "invalid tensor type",
                provider.getTrace(""));
    }
    {
        // invalid tensor type
        navi::NaviLoggerProvider provider("WARN");
        HA3_NS(sql)::NamedTensorProto tensorProto;
        auto data = tensorProto.mutable_data();
        Tensor t(DT_STRING, {1});
        t.scalar<string>()() = "";
        t.AsProtoField(data);
        ASSERT_FALSE(kernel.getResultFromTensorProto(tensorProto, result));
        CHECK_TRACE_COUNT(1, "invalid tensor type", provider.getTrace(""));
    }
    {
        // invalid sql response
        navi::NaviLoggerProvider provider("WARN");
        HA3_NS(sql)::NamedTensorProto tensorProto;
        auto data = tensorProto.mutable_data();
        Tensor t(DT_VARIANT, {1});
        t.AsProtoField(data);
        ASSERT_FALSE(kernel.getResultFromTensorProto(tensorProto, result));
        CHECK_TRACE_COUNT(1, "invalid sql response", provider.getTrace(""));
    }
    {
        // invalid sql result variant
        navi::NaviLoggerProvider provider("WARN");
        HA3_NS(sql)::NamedTensorProto tensorProto;
        auto data = tensorProto.mutable_data();
        Tensor t(DT_VARIANT, {1});
        ResultPtr ha3Result(new Result());
        t.scalar<Variant>()() = Ha3ResultVariant(ha3Result, &_pool);
        t.AsProtoField(data);
        ASSERT_FALSE(kernel.getResultFromTensorProto(tensorProto, result));
        CHECK_TRACE_COUNT(1, "invalid sql result variant",
                provider.getTrace(""));
    }
    {
        // construct sql result variant failed
        navi::NaviLoggerProvider provider("WARN");
        HA3_NS(sql)::NamedTensorProto tensorProto;
        auto data = tensorProto.mutable_data();
        Tensor t(DT_VARIANT, {1});
        navi::NaviResultPtr result(new navi::NaviResult());
        t.scalar<Variant>()() = SqlResultVariant(result, &_pool);
        t.AsProtoField(data);
        ASSERT_FALSE(kernel.getResultFromTensorProto(tensorProto, result));
        CHECK_TRACE_COUNT(1, "construct sql result variant failed",
                provider.getTrace(""));
    }
    kernel._pool = &_pool;
    kernel._tablePool = _memPoolResource.getPool();
    {
        HA3_NS(sql)::NamedTensorProto tensorProto;
        auto data = tensorProto.mutable_data();
        Tensor t(DT_VARIANT, {1});
        navi::NaviResultPtr result(new navi::NaviResult());
        t.scalar<Variant>()() = SqlResultVariant(result, &_pool);
        t.AsProtoField(data);
        ASSERT_TRUE(kernel.getResultFromTensorProto(tensorProto, result));
    }
}

TEST_F(ExchangeKernelTest, testGetDataFromResponse) {
    ExchangeKernel kernel;
    SqlGraphResponse sqlResponse;
    kernel._memoryPoolResource = &_memPoolResource;
    {
        navi::NaviLoggerProvider provider("WARN");
        std::shared_ptr<google::protobuf::Arena> arena(
            new google::protobuf::Arena());
        QrsResponse *qrsResponse = new QrsResponse(arena);
        multi_call::ResponsePtr response(qrsResponse);
        multi_call::ReplyPtr reply(new multi_call::Reply());
        ASSERT_FALSE(kernel.getDataFromResponse(response, reply));
        CHECK_TRACE_COUNT(1, "multi call has error, error code=32, "
                "errorMsg=MULTI_CALL_ERROR_NO_RESPONSE", provider.getTrace(""));
    }
    {
        navi::NaviLoggerProvider provider("WARN");
        std::shared_ptr<google::protobuf::Arena> arena(
            new google::protobuf::Arena());
        QrsResponse *qrsResponse = new QrsResponse(arena);
        qrsResponse->_stat.ec =
            multi_call::MultiCallErrorCode::MULTI_CALL_ERROR_NONE;
        multi_call::ResponsePtr response(qrsResponse);
        multi_call::ReplyPtr reply(new multi_call::Reply());
        ASSERT_FALSE(kernel.getDataFromResponse(response, reply));
        CHECK_TRACE_COUNT(1, "get qrs response message failed",
                provider.getTrace(""));
    }
    {
        navi::NaviLoggerProvider provider("WARN");
        std::shared_ptr<google::protobuf::Arena> arena(
            new google::protobuf::Arena());
        QrsResponse *qrsResponse = new QrsResponse(arena);
        GraphResponse *graphResponse = new GraphResponse();
        suez::turing::NamedTensorProto *namedTensorProto = graphResponse->add_outputs();
        ASSERT_TRUE(namedTensorProto);
        TensorProto *tensorProto = namedTensorProto->mutable_tensor();
        Tensor t(DT_STRING, {1});
        t.scalar<string>()() = "";
        t.AsProtoField(tensorProto);
        qrsResponse->init(graphResponse);
        qrsResponse->_stat.ec =
            multi_call::MultiCallErrorCode::MULTI_CALL_ERROR_NONE;
        multi_call::ResponsePtr response(qrsResponse);
        multi_call::ReplyPtr reply(new multi_call::Reply());
        ASSERT_FALSE(kernel.getDataFromResponse(response, reply));
        CHECK_TRACE_COUNT(1, "expect sql response output size 3, actually 0",
                provider.getTrace(""));
    }
    {
        navi::NaviLoggerProvider provider("WARN");
        std::shared_ptr<google::protobuf::Arena> arena(
            new google::protobuf::Arena());
        QrsResponse *qrsResponse = new QrsResponse(arena);
        GraphResponse *graphResponse = new GraphResponse();
        suez::turing::NamedTensorProto *namedTensorProto = graphResponse->add_outputs();
        ASSERT_TRUE(namedTensorProto);
        TensorProto *tensorProto = namedTensorProto->mutable_tensor();
        string responseStr;
        {
            // sql response has no data
            SqlGraphResponse sqlResponse;
            sqlResponse.add_outputs();
            sqlResponse.add_outputs();
            sqlResponse.add_outputs();
            sqlResponse.SerializeToString(&responseStr);
        }
        Tensor t(DT_STRING, {1});
        t.scalar<string>()() = responseStr;
        t.AsProtoField(tensorProto);
        qrsResponse->init(graphResponse);
        qrsResponse->_stat.ec =
            multi_call::MultiCallErrorCode::MULTI_CALL_ERROR_NONE;
        multi_call::ResponsePtr response(qrsResponse);
        multi_call::ReplyPtr reply(new multi_call::Reply());
        ASSERT_FALSE(kernel.getDataFromResponse(response, reply));
        CHECK_TRACE_COUNT(1, "sql response has no data",
                provider.getTrace(""));
    }
    kernel._pool = &_pool;
    kernel._tablePool = _memPoolResource.getPool();
    {
        // result error
        navi::NaviLoggerProvider provider("WARN");
        std::shared_ptr<google::protobuf::Arena> arena(
            new google::protobuf::Arena());
        QrsResponse *qrsResponse = new QrsResponse(arena);
        GraphResponse *graphResponse = new GraphResponse();
        suez::turing::NamedTensorProto *namedTensorProto = graphResponse->add_outputs();
        ASSERT_TRUE(namedTensorProto);
        TensorProto *tensorProto = namedTensorProto->mutable_tensor();
        string responseStr;
        {
            SqlGraphResponse sqlResponse;
            sqlResponse.add_outputs();
            {
                auto responseTensorProto = sqlResponse.add_outputs();
                auto data = responseTensorProto->mutable_data();
                Tensor t(DT_VARIANT, {1});
                navi::NaviResultPtr result(new navi::NaviResult());
                result->_ec = navi::EC_ABORT;
                result->_msg = "gg";
                t.scalar<Variant>()() = SqlResultVariant(result, &_pool);
                t.AsProtoField(data);
            }
            sqlResponse.add_outputs();
            sqlResponse.SerializeToString(&responseStr);
        }
        Tensor t(DT_STRING, {1});
        t.scalar<string>()() = responseStr;
        t.AsProtoField(tensorProto);
        qrsResponse->init(graphResponse);
        qrsResponse->_stat.ec =
            multi_call::MultiCallErrorCode::MULTI_CALL_ERROR_NONE;
        multi_call::ResponsePtr response(qrsResponse);
        multi_call::ReplyPtr reply(new multi_call::Reply());
        ASSERT_FALSE(kernel.getDataFromResponse(response, reply));
        CHECK_TRACE_COUNT(1, "gg", provider.getTrace(""));
    }
    {
        // invalid sql table variant
        navi::NaviLoggerProvider provider("WARN");
        std::shared_ptr<google::protobuf::Arena> arena(
            new google::protobuf::Arena());
        QrsResponse *qrsResponse = new QrsResponse(arena);
        GraphResponse *graphResponse = new GraphResponse();
        suez::turing::NamedTensorProto *namedTensorProto = graphResponse->add_outputs();
        ASSERT_TRUE(namedTensorProto);
        TensorProto *tensorProto = namedTensorProto->mutable_tensor();
        string responseStr;
        {
            SqlGraphResponse sqlResponse;
            {
                auto responseTensorProto = sqlResponse.add_outputs();
                auto data = responseTensorProto->mutable_data();
                Tensor t(DT_VARIANT, {1});
                ResultPtr result(new Result());
                t.scalar<Variant>()() = Ha3ResultVariant(result, &_pool);
                t.AsProtoField(data);
            }
            {
                auto responseTensorProto = sqlResponse.add_outputs();
                auto data = responseTensorProto->mutable_data();
                Tensor t(DT_VARIANT, {1});
                navi::NaviResultPtr result(new navi::NaviResult());
                t.scalar<Variant>()() = SqlResultVariant(result, &_pool);
                t.AsProtoField(data);
            }
            sqlResponse.add_outputs();
            sqlResponse.SerializeToString(&responseStr);
        }
        Tensor t(DT_STRING, {1});
        t.scalar<string>()() = responseStr;
        t.AsProtoField(tensorProto);
        qrsResponse->init(graphResponse);
        qrsResponse->_stat.ec =
            multi_call::MultiCallErrorCode::MULTI_CALL_ERROR_NONE;
        multi_call::ResponsePtr response(qrsResponse);
        multi_call::ReplyPtr reply(new multi_call::Reply());
        ASSERT_FALSE(kernel.getDataFromResponse(response, reply));
        CHECK_TRACE_COUNT(1, "invalid sql table variant",
                provider.getTrace(""));
    }
    prepareTable();
    {
        std::shared_ptr<google::protobuf::Arena> arena(
            new google::protobuf::Arena());
        QrsResponse *qrsResponse = new QrsResponse(arena);
        GraphResponse *graphResponse = new GraphResponse();
        suez::turing::NamedTensorProto *namedTensorProto = graphResponse->add_outputs();
        ASSERT_TRUE(namedTensorProto);
        TensorProto *tensorProto = namedTensorProto->mutable_tensor();
        string responseStr;
        {
            SqlGraphResponse sqlResponse;
            {
                auto responseTensorProto = sqlResponse.add_outputs();
                auto data = responseTensorProto->mutable_data();
                Tensor t(DT_VARIANT, {1});
                t.scalar<Variant>()() = SqlTableVariant(_table, &_pool);
                t.AsProtoField(data);
            }
            {
                auto responseTensorProto = sqlResponse.add_outputs();
                auto data = responseTensorProto->mutable_data();
                Tensor t(DT_VARIANT, {1});
                navi::NaviResultPtr result(new navi::NaviResult());
                t.scalar<Variant>()() = SqlResultVariant(result, &_pool);
                t.AsProtoField(data);
            }
            {
                SqlSearchInfo searchInfo;
                string searchInfoStr;
                searchInfo.SerializeToString(&searchInfoStr);
                NamedTensorProto *outputTensor = sqlResponse.add_outputs();
                outputTensor->set_name("sql_search_info");
                outputTensor->set_port("0");
                TensorProto *tensorProto = outputTensor->mutable_data();
                Tensor t(DT_STRING, {1});
                t.scalar<string>()() = searchInfoStr;
                t.AsProtoField(tensorProto);
            }
            sqlResponse.SerializeToString(&responseStr);
        }
        Tensor t(DT_STRING, {1});
        t.scalar<string>()() = responseStr;
        t.AsProtoField(tensorProto);
        qrsResponse->init(graphResponse);
        qrsResponse->_stat.ec =
            multi_call::MultiCallErrorCode::MULTI_CALL_ERROR_NONE;
        multi_call::ResponsePtr response(qrsResponse);
        multi_call::ReplyPtr reply(new multi_call::Reply());
        ASSERT_TRUE(kernel.getDataFromResponse(response, reply));
        ASSERT_EQ(4, kernel._table->getRowCount());
        ASSERT_TRUE(kernel.getDataFromResponse(response, reply));
        ASSERT_EQ(8, kernel._table->getRowCount());
    }
}

TEST_F(ExchangeKernelTest, testFillResultsWithSupportLackOff) {
    using testing::_;
    ExchangeKernel kernel;
    SqlGraphResponse sqlResponse;
    kernel._memoryPoolResource = &_memPoolResource;
    kernel._bizName = "123";
    {
        navi::NaviLoggerProvider provider("ERROR");
        multi_call::ReplyPtr reply;
        ASSERT_FALSE(kernel.fillResults(reply, nullptr));
        CHECK_TRACE_COUNT(1, "run search graph [123] failed", provider.getTrace(""));
    }
    {
        navi::NaviLoggerProvider provider("ERROR");
        MockReply* mockReply = new MockReply();
        multi_call::ReplyPtr reply(mockReply);
        EXPECT_CALL(*mockReply, getResponses(_))
            .WillOnce(Invoke([] (size_t &lackCount)
                            {
                                lackCount = 8;
                                std::vector<multi_call::ResponsePtr> vec;
                                return vec;
                            }));
        ASSERT_FALSE(kernel.fillResults(reply, nullptr));
        CHECK_TRACE_COUNT(1, "lack count is 8", provider.getTrace(""));
    }
    {
        navi::NaviLoggerProvider provider("ERROR");
        MockReply* mockReply = new MockReply();
        multi_call::ReplyPtr reply(mockReply);
        EXPECT_CALL(*mockReply, getResponses(_))
            .WillOnce(Invoke([] (size_t &lackCount)
                            {
                                lackCount = 0;
                                std::shared_ptr<google::protobuf::Arena> arena(
                                        new google::protobuf::Arena());
                                QrsResponse *qrsResponse = new QrsResponse(arena);
                                std::vector<multi_call::ResponsePtr> vec;
                                vec.emplace_back(qrsResponse);
                                return vec;
                            }));
        ASSERT_FALSE(kernel.fillResults(reply, nullptr));
        CHECK_TRACE_COUNT(1, "get data from response failed, biz name [123]", provider.getTrace(""));
    }
}

TEST_F(ExchangeKernelTest, testFillResultsWithSupportLackOn) {
    using testing::_;
    ExchangeKernel kernel;
    SqlGraphResponse sqlResponse;
    kernel._memoryPoolResource = &_memPoolResource;
    kernel._bizName = "123";
    kernel._lackResultEnable = true;
    {
        navi::NaviLoggerProvider provider("ERROR");
        multi_call::ReplyPtr reply;
        ASSERT_FALSE(kernel.fillResults(reply, nullptr));
        CHECK_TRACE_COUNT(1, "run search graph [123] failed", provider.getTrace(""));
    }
    {
        navi::NaviLoggerProvider provider("WARN");
        MockReply* mockReply = new MockReply();
        multi_call::ReplyPtr reply(mockReply);
        EXPECT_CALL(*mockReply, getResponses(_))
            .WillOnce(Invoke([] (size_t &lackCount)
                            {
                                lackCount = 8;
                                std::vector<multi_call::ResponsePtr> vec;
                                return vec;
                            }));
        ASSERT_TRUE(kernel.fillResults(reply, nullptr));
        CHECK_TRACE_COUNT(1, "lack count is 8, ignored", provider.getTrace(""));
    }
    {
        navi::NaviLoggerProvider provider("WARN");
        MockReply* mockReply = new MockReply();
        multi_call::ReplyPtr reply(mockReply);
        EXPECT_CALL(*mockReply, getResponses(_))
            .WillOnce(Invoke([] (size_t &lackCount)
                            {
                                lackCount = 0;
                                std::shared_ptr<google::protobuf::Arena> arena(
                                        new google::protobuf::Arena());
                                QrsResponse *qrsResponse = new QrsResponse(arena);
                                qrsResponse->setErrorCode(
                                        multi_call::MULTI_CALL_ERROR_NO_RESPONSE);
                                std::vector<multi_call::ResponsePtr> vec;
                                vec.emplace_back(qrsResponse);
                                return vec;
                            }));
        RpcInfo rpcInfo;
        ASSERT_TRUE(kernel.fillResults(reply, &rpcInfo));
        ASSERT_EQ(1, rpcInfo.lackcount());
    }

    {
        navi::NaviLoggerProvider provider("WARN");
        MockReply* mockReply = new MockReply();
        multi_call::ReplyPtr reply(mockReply);
        EXPECT_CALL(*mockReply, getResponses(_))
            .WillOnce(Invoke([] (size_t &lackCount)
                            {
                                lackCount = 0;
                                std::shared_ptr<google::protobuf::Arena> arena(
                                        new google::protobuf::Arena());
                                QrsResponse *qrsResponse = new QrsResponse(arena);
                                std::vector<multi_call::ResponsePtr> vec;
                                vec.emplace_back(qrsResponse);
                                return vec;
                            }));
        ASSERT_TRUE(kernel.fillResults(reply, nullptr));
        CHECK_TRACE_COUNT(1, "get data from response failed, biz name [123], ignored", provider.getTrace(""));
    }
}


END_HA3_NAMESPACE(sql);
