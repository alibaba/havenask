#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include <ha3/turing/qrs/QrsServiceImpl.h>
#include <arpc/ANetRPCController.h>
#include <http_arpc/HTTPRPCControllerBase.h>

using namespace std;
using namespace testing;

BEGIN_HA3_NAMESPACE(turing);

class FakeHttpController : public http_arpc::HTTPRPCControllerBase {
public:
    FakeHttpController() {
    }
public:
    void Reset() override {
    }
    bool Failed() const override {
        return false;
    }
    std::string ErrorText() const override {
        return "";
    }
    void StartCancel() override {
    }
    void SetFailed(const std::string &reason) override {
    }
    bool IsCanceled() const override {
        return false;
    }
    void NotifyOnCancel(google::protobuf::Closure* callback) override {
    }
    void SetErrorCode(int32_t errorCode) {
    }
    int32_t GetErrorCode() {
        return 0;
    }
    void SetQueueTime(int64_t beginTime) {
    }
    int64_t GetQueueTime() const {
        return 0;
    }
    void SetAddr(const std::string &addr) {
        _addr = addr;
    }
    const std::string &GetAddr() const {
        return _addr;
    }
private:
    std::string _addr;
};

class QrsServiceImplTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(qrs, QrsServiceImplTest);

void QrsServiceImplTest::setUp() {
}

void QrsServiceImplTest::tearDown() {
}

TEST_F(QrsServiceImplTest, testGetClientIp) {
    {
        // null controller
        EXPECT_EQ("", QrsServiceImpl::getClientIp(nullptr));
    }
    {
        // no rpc controller
        multi_call::GigRpcController controller;
        EXPECT_EQ("", QrsServiceImpl::getClientIp(&controller));
    }
    {
        // arpc
        multi_call::GigRpcController controller;
        arpc::ANetRPCController anetController;
        anetController.SetClientAddress("10.101.101.101:10086");
        controller.setRpcController(&anetController);
        EXPECT_EQ("arpc 10.101.101.101:10086",
            QrsServiceImpl::getClientIp(&controller));
    }
    {
        // http
        multi_call::GigRpcController controller;
        FakeHttpController httpController;
        httpController.SetAddr("10.101.101.102:10087");
        controller.setRpcController(&httpController);
        EXPECT_EQ("http 10.101.101.102:10087",
            QrsServiceImpl::getClientIp(&controller));
    }
}

END_HA3_NAMESPACE();

