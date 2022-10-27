#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include <tensorflow/core/kernels/ops_testutil.h>
#include <tensorflow/core/framework/fake_input.h>
#include <tensorflow/core/framework/node_def_builder.h>
#include <ha3/common/Request.h>
#include <ha3/turing/ops/test/Ha3OpTestBase.h>
#include <ha3/common/TimeoutTerminator.h>
#include <ha3/turing/ops/Ha3RequestInitOp.h>

using namespace std;
using namespace tensorflow;
using namespace suez::turing;
using namespace ::testing;
USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(turing);

class Ha3RequestInitOpTest : public Ha3OpTestBase {
private:
    void prepareOp() {
        TF_ASSERT_OK(
                NodeDefBuilder("myop", "Ha3RequestInitOp")
                .Input(FakeInput(DT_VARIANT))
                .Finalize(node_def()));
        TF_ASSERT_OK(InitOp());
        _requestInitOp = dynamic_cast<Ha3RequestInitOp *>(kernel_.get());
    }

    void prepareSimpleInput() {
        _request.reset(new common::Request(&_pool));
        _request->setConfigClause(new ConfigClause());
        _configClause = _request->getConfigClause();
        ASSERT_NE(nullptr, _configClause);
    }

    void fakeTimeout(uint32_t timeout, uint32_t seekTimeout) {
        _configClause->setRpcTimeOut(timeout);
        _configClause->setSeekTimeOut(seekTimeout);
    }

private:
    common::RequestPtr _request;
    ConfigClause* _configClause;
    Ha3RequestInitOp* _requestInitOp;

private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(ops, Ha3RequestInitOpTest);

TEST_F(Ha3RequestInitOpTest, testTimeout) {
    ASSERT_NO_FATAL_FAILURE(prepareOp());
    ASSERT_NO_FATAL_FAILURE(prepareSimpleInput());

    uint32_t timeout = 100;
    uint32_t seekTimeout = 50;
    fakeTimeout(timeout, seekTimeout);

    int64_t startTime = 0;
    int64 currentTime = 10;
    TimeoutTerminatorPtr timeoutTerminator = _requestInitOp->createTimeoutTerminator(_request,startTime, currentTime, false);

    int64_t targetTime = (startTime + timeout) * Ha3RequestInitOp::MS_TO_US_FACTOR;
    ASSERT_EQ(targetTime, timeoutTerminator->getExpireTime());
}

TEST_F(Ha3RequestInitOpTest, testSeekTimeout) {
    ASSERT_NO_FATAL_FAILURE(prepareOp());
    ASSERT_NO_FATAL_FAILURE(prepareSimpleInput());

    uint32_t timeout = 100;
    uint32_t seekTimeout = 50;
    fakeTimeout(timeout, seekTimeout);

    int64_t startTime = 0;
    int64 currentTime = 10;
    TimeoutTerminatorPtr timeoutTerminator = _requestInitOp->createTimeoutTerminator(_request,startTime, currentTime, true);

    int64_t targetTime = (startTime + seekTimeout) * Ha3RequestInitOp::MS_TO_US_FACTOR;
    ASSERT_EQ(targetTime, timeoutTerminator->getExpireTime());
}

TEST_F(Ha3RequestInitOpTest, testDefaultSeekTimeout) {
    ASSERT_NO_FATAL_FAILURE(prepareOp());
    ASSERT_NO_FATAL_FAILURE(prepareSimpleInput());

    uint32_t timeout = 100;
    uint32_t seekTimeout = 0;
    fakeTimeout(timeout, seekTimeout);

    int64_t startTime = 0;
    int64 currentTime = 10;
    TimeoutTerminatorPtr timeoutTerminator = _requestInitOp->createTimeoutTerminator(_request,startTime, currentTime, true);

    int64_t targetTime = (startTime + int64_t(timeout * SEEK_TIMEOUT_PERCENTAGE)) * Ha3RequestInitOp::MS_TO_US_FACTOR;
    ASSERT_EQ(targetTime, timeoutTerminator->getExpireTime());
}

TEST_F(Ha3RequestInitOpTest, testTimeoutWithoutSeek) {
    ASSERT_NO_FATAL_FAILURE(prepareOp());
    ASSERT_NO_FATAL_FAILURE(prepareSimpleInput());

    uint32_t timeout = 100;
    uint32_t seekTimeout = 0;
    fakeTimeout(timeout, seekTimeout);

    int64_t startTime = 0;
    int64 currentTime = 10;
    TimeoutTerminatorPtr timeoutTerminator = _requestInitOp->createTimeoutTerminator(_request,startTime, currentTime, false);

    int64_t targetTime = (startTime + timeout) * Ha3RequestInitOp::MS_TO_US_FACTOR;
    ASSERT_EQ(targetTime, timeoutTerminator->getExpireTime());
}

TEST_F(Ha3RequestInitOpTest, testRpcTimeout) {
    ASSERT_NO_FATAL_FAILURE(prepareOp());
    ASSERT_NO_FATAL_FAILURE(prepareSimpleInput());

    uint32_t timeout = 5;
    uint32_t seekTimeout = 0;
    fakeTimeout(timeout, seekTimeout);

    int64_t startTime = 0;
    int64 currentTime = 10 * Ha3RequestInitOp::MS_TO_US_FACTOR;
    TimeoutTerminatorPtr timeoutTerminator = _requestInitOp->createTimeoutTerminator(_request,startTime, currentTime, true);

    ASSERT_EQ(nullptr, timeoutTerminator);
}

END_HA3_NAMESPACE();
