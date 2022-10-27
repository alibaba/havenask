#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include <ha3/turing/qrs/SearchTuringClosure.h>
#include <ha3/common/AccessLog.h>
#include <ha3/service/test/FakeClosure.h>
#include <tensorflow/core/framework/tensor.h>
#include <multi_call/common/ErrorCode.h>
#include <autil/TimeUtility.h>

using namespace std;
using namespace testing;
using namespace tensorflow;
using namespace multi_call;
using namespace autil;
using namespace suez::turing;

BEGIN_HA3_NAMESPACE(turing);

class SearchTuringClosureTest : public TESTBASE {
public:
    void setUp() override {
        _pAccessLog = new common::AccessLog();
        _pGraphRequest = new GraphRequest();
        _pGraphResponse = new GraphResponse();
        _pQrsResponse.reset(new proto::QrsResponse());
        _pDone.reset(new service::FakeClosure());
        _pClosure = new SearchTuringClosure(
                _pAccessLog,
                _pGraphRequest,
                _pGraphResponse,
                _pQrsResponse.get(),
                _pDone.get(),
                TimeUtility::currentTime());
    }

    void tearDown() override {
        _pQrsResponse.reset();
        _pDone.reset();
    }
protected:
    common::AccessLog* _pAccessLog{nullptr};
    GraphRequest* _pGraphRequest{nullptr};
    GraphResponse* _pGraphResponse{nullptr};
    unique_ptr<proto::QrsResponse> _pQrsResponse;
    unique_ptr<service::FakeClosure> _pDone;
    SearchTuringClosure* _pClosure{nullptr};
};

TEST_F(SearchTuringClosureTest, testRunSuccess) {
    Tensor tensor(DT_STRING, TensorShape({}));
    tensor.scalar<string>()() = "testdata";
    auto pNamedTensorProto = _pGraphResponse->add_outputs();
    tensor.AsProtoField(pNamedTensorProto->mutable_tensor());
    _pGraphResponse->set_multicall_ec(MULTI_CALL_ERROR_NONE);
    _pGraphResponse->set_gigresponseinfo("gigdata");
    _pClosure->Run();
    _pDone->wait();
    EXPECT_EQ("testdata", _pQrsResponse->assemblyresult());
    EXPECT_EQ("gigdata", _pQrsResponse->gigresponseinfo());
    EXPECT_EQ(MULTI_CALL_ERROR_NONE, _pQrsResponse->multicall_ec());
    EXPECT_EQ(proto::FT_PROTOBUF, _pQrsResponse->formattype());
}

TEST_F(SearchTuringClosureTest, testBizNotExist) {
    _pGraphResponse->set_multicall_ec(MULTI_CALL_REPLY_ERROR_NO_PROVIDER_FOUND);
    _pGraphResponse->set_gigresponseinfo("error");
    auto pErrorInfo = _pGraphResponse->mutable_errorinfo();
    pErrorInfo->set_errorcode(RS_ERROR_BIZ_NOT_EXIST);
    pErrorInfo->set_errormsg("biz not exist");
    _pClosure->Run();
    _pDone->wait();
    EXPECT_TRUE(_pQrsResponse->has_assemblyresult());
    EXPECT_EQ("", _pQrsResponse->assemblyresult());
    EXPECT_EQ("error", _pQrsResponse->gigresponseinfo());
    EXPECT_EQ(MULTI_CALL_REPLY_ERROR_NO_PROVIDER_FOUND, _pQrsResponse->multicall_ec());
    EXPECT_EQ(proto::FT_PROTOBUF, _pQrsResponse->formattype());
}

TEST_F(SearchTuringClosureTest, testEmptyOutput) {
    _pGraphResponse->set_multicall_ec(MULTI_CALL_ERROR_NONE);
    _pGraphResponse->set_gigresponseinfo("gigdata");
    _pClosure->Run();
    _pDone->wait();
    EXPECT_TRUE(_pQrsResponse->has_assemblyresult());
    EXPECT_EQ("", _pQrsResponse->assemblyresult());
    EXPECT_EQ("gigdata", _pQrsResponse->gigresponseinfo());
    EXPECT_EQ(MULTI_CALL_ERROR_RPC_FAILED, _pQrsResponse->multicall_ec());
    EXPECT_EQ(proto::FT_PROTOBUF, _pQrsResponse->formattype());
}

TEST_F(SearchTuringClosureTest, testNotStringOutput) {
    Tensor tensor(DT_INT32, TensorShape({}));
    tensor.scalar<int32_t>()() = 1;
    auto pNamedTensorProto = _pGraphResponse->add_outputs();
    tensor.AsProtoField(pNamedTensorProto->mutable_tensor());
    _pGraphResponse->set_multicall_ec(MULTI_CALL_ERROR_NONE);
    _pGraphResponse->set_gigresponseinfo("gigdata");
    _pClosure->Run();
    _pDone->wait();
    EXPECT_TRUE(_pQrsResponse->has_assemblyresult());
    EXPECT_EQ("", _pQrsResponse->assemblyresult());
    EXPECT_EQ("gigdata", _pQrsResponse->gigresponseinfo());
    EXPECT_EQ(MULTI_CALL_ERROR_RPC_FAILED, _pQrsResponse->multicall_ec());
    EXPECT_EQ(proto::FT_PROTOBUF, _pQrsResponse->formattype());
}

END_HA3_NAMESPACE(turing);
