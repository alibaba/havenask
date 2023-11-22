#include "navi/tester/NaviResourceHelper.h"

#include "navi/engine/test/MockAsyncPipe.h"
#include "navi/resource/GraphMemoryPoolR.h"
#include "navi/example/TestResource.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;

namespace navi {

class FakeHelperKernelRes : public Resource {
public:
    static const std::string RESOURCE_ID;

public:
    void def(ResourceDefBuilder &builder) const override { builder.name(RESOURCE_ID, navi::RS_KERNEL); }
    bool config(navi::ResourceConfigContext &ctx) override {
        NAVI_JSONIZE(ctx, "test_config_key", _testConfigValue);
        NAVI_JSONIZE(ctx, "create_async_pipe", _createAsyncPipe, _createAsyncPipe);
        return true;
    }
    ErrorCode init(ResourceInitContext &ctx) override {
        if (_createAsyncPipe) {
            if (_asyncPipe = ctx.createRequireKernelAsyncPipe(); !_asyncPipe) {
                NAVI_KERNEL_LOG(ERROR, "create async pipe failed");
                return navi::EC_ABORT;
            }
        }
        return EC_NONE;
    }

private:
    std::string _testConfigValue;
    bool _createAsyncPipe = false;
    std::shared_ptr<AsyncPipe> _asyncPipe;
};

const std::string FakeHelperKernelRes::RESOURCE_ID = "fake_helper_kernel_r";
REGISTER_RESOURCE(FakeHelperKernelRes);

class NaviResourceHelperTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
};

void NaviResourceHelperTest::setUp() {}

void NaviResourceHelperTest::tearDown() {}

TEST_F(NaviResourceHelperTest, testSimple) {
    // NaviLoggerProvider provider("DEBUG");
    NaviResourceHelper naviRes;
    auto res1 = naviRes.getOrCreateResPtr<GraphMemoryPoolR>();
    ASSERT_NE(nullptr, res1);
    auto res2 = naviRes.getOrCreateResPtr<GraphMemoryPoolR>();
    ASSERT_EQ(res1, res2);
}

TEST_F(NaviResourceHelperTest, testSnapshotResourceCreate) {
    // NaviLoggerProvider provider("DEBUG");
    NaviResourceHelper naviRes;
    naviRes.snapshotConfig(TestSnapshotR::RESOURCE_ID, "{}");
    auto res1 = naviRes.getOrCreateResPtr<TestSnapshotR>();
    ASSERT_NE(nullptr, res1);
    auto res2 = naviRes.getOrCreateResPtr<TestSnapshotR>();
    ASSERT_EQ(res1, res2);
}

TEST_F(NaviResourceHelperTest, testKernelResource_Create) {
    NaviResourceHelper naviRes;
    naviRes.kernelConfig(R"json({"test_config_key" : "test_config_value"})json");
    auto *res1 = naviRes.getOrCreateRes<FakeHelperKernelRes>();
    ASSERT_TRUE(res1);
    ASSERT_EQ("test_config_value", res1->_testConfigValue);
    auto *res2 = naviRes.getOrCreateRes<FakeHelperKernelRes>();
    ASSERT_EQ(res1, res2);
}

TEST_F(NaviResourceHelperTest, testKernelResource_AsyncPipe) {
    {
        NaviResourceHelper naviRes;
        naviRes.kernelConfig(R"json({
    "test_config_key" : "test_config_value",
    "create_async_pipe": false
})json");
        auto *res1 = naviRes.getOrCreateRes<FakeHelperKernelRes>();
        ASSERT_TRUE(res1);
        ASSERT_FALSE(res1->_asyncPipe);
    }
    {
        NaviResourceHelper naviRes;
        naviRes.kernelConfig(R"json({
    "test_config_key" : "test_config_value",
    "create_async_pipe": true
})json");
        auto *res1 = naviRes.getOrCreateRes<FakeHelperKernelRes>();
        ASSERT_TRUE(res1);
        ASSERT_TRUE(res1->_asyncPipe);
        ASSERT_TRUE(dynamic_cast<MockAsyncPipe *>(res1->_asyncPipe.get()));
    }
}

} // namespace navi
