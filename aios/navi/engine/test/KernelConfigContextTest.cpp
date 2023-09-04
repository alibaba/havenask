#include "unittest/unittest.h"
#include "navi/engine/KernelConfigContext.h"
#include "navi/tester/KernelTesterBuilder.h"

using namespace std;
using namespace testing;

namespace navi {

class KernelConfigContextTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
};

void KernelConfigContextTest::setUp() {
}

void KernelConfigContextTest::tearDown() {
}

TEST_F(KernelConfigContextTest, testAttr) {
    {
        KernelTesterBuilder testerBuilder;
        auto ctx = testerBuilder.buildConfigContext();
        ASSERT_TRUE(ctx);
        try {
            int value = 0;
            ctx->Jsonize("key", value);
            ASSERT_TRUE(false) << "should throw exception";
        } catch (const autil::legacy::ExceptionBase &e) {
            ASSERT_EQ("[key] not found when try to parse from Json", e.GetMessage());
        }
    }
    {
        KernelTesterBuilder testerBuilder;
        testerBuilder.attrs(R"json({"a" : "3"})json");
        auto ctx = testerBuilder.buildConfigContext();
        ASSERT_TRUE(ctx);
        try {
            int value = 0;
            ctx->Jsonize("key", value);
            ASSERT_TRUE(false) << "should throw exception";
        } catch (const autil::legacy::ExceptionBase &e) {
            ASSERT_EQ("[key] not found when try to parse from Json", e.GetMessage());
        }
    }
    {
        KernelTesterBuilder testerBuilder;
        testerBuilder.attrs(R"json({"key" : "3"})json");
        auto ctx = testerBuilder.buildConfigContext();
        ASSERT_TRUE(ctx);
        try {
            int value = 0;
            ctx->Jsonize("key", value);
            ASSERT_TRUE(false) << "should throw exception";
        } catch (const autil::legacy::ExceptionBase &e) {
            ASSERT_EQ("type not match, expect int64 but get: \"3\"", e.GetMessage());
        }
    }
    {
        KernelTesterBuilder testerBuilder;
        testerBuilder.attrs(R"json({"key" : 3})json");
        auto ctx = testerBuilder.buildConfigContext();
        ASSERT_TRUE(ctx);
        try {
            int value = 0;
            ctx->Jsonize("key", value);
            ASSERT_EQ(3, value);
        } catch (const autil::legacy::ExceptionBase &e) {
            ASSERT_TRUE(false) << "should not throw exception";
        }
    }
}

TEST_F(KernelConfigContextTest, testBinary) {
    {
        KernelTesterBuilder testerBuilder;
        testerBuilder.binaryAttrsFromMap({{"a", "3"}});
        auto ctx = testerBuilder.buildConfigContext();
        ASSERT_TRUE(ctx);
        try {
            int value = 0;
            ctx->Jsonize("key", value);
            ASSERT_TRUE(false) << "should throw exception";
        } catch (const autil::legacy::ExceptionBase &e) {
            ASSERT_EQ("[key] not found when try to parse from Json", e.GetMessage());
        }
    }
    {
        KernelTesterBuilder testerBuilder;
        testerBuilder.binaryAttrsFromMap({{"key", "3"}});
        auto ctx = testerBuilder.buildConfigContext();
        ASSERT_TRUE(ctx);
        try {
            int value = 0;
            ctx->Jsonize("key", value);
            ASSERT_TRUE(false) << "should throw exception";
        } catch (const autil::legacy::ExceptionBase &e) {
            ASSERT_EQ("invalid Jsonize type for key [key] in binary attr, must "
                      "be string",
                      e.GetMessage());
        }
    }
    {
        KernelTesterBuilder testerBuilder;
        testerBuilder.binaryAttrsFromMap({{"key", "3"}});
        auto ctx = testerBuilder.buildConfigContext();
        ASSERT_TRUE(ctx);
        try {
            std::string value;
            ctx->Jsonize("key", value);
            ASSERT_EQ("3", value);
        } catch (const autil::legacy::ExceptionBase &e) {
            ASSERT_TRUE(false) << "should not throw exception";
        }
    }
}

TEST_F(KernelConfigContextTest, testConfig) {
    {
        KernelTesterBuilder testerBuilder;
        testerBuilder.kernelConfig(R"json({"a" : "3"})json");
        auto ctx = testerBuilder.buildConfigContext();
        ASSERT_TRUE(ctx);
        try {
            int value = 0;
            ctx->Jsonize("key", value);
            ASSERT_TRUE(false) << "should throw exception";
        } catch (const autil::legacy::ExceptionBase &e) {
            ASSERT_EQ("[key] not found when try to parse from Json", e.GetMessage());
        }
    }
    {
        KernelTesterBuilder testerBuilder;
        testerBuilder.kernelConfig(R"json({"key" : "3"})json");
        auto ctx = testerBuilder.buildConfigContext();
        ASSERT_TRUE(ctx);
        try {
            int value = 0;
            ctx->Jsonize("key", value);
            ASSERT_TRUE(false) << "should throw exception";
        } catch (const autil::legacy::ExceptionBase &e) {
            ASSERT_EQ("type not match, expect int64 but get: \"3\"", e.GetMessage());
        }
    }
    {
        KernelTesterBuilder testerBuilder;
        testerBuilder.kernelConfig(R"json({"key" : 3})json");
        auto ctx = testerBuilder.buildConfigContext();
        ASSERT_TRUE(ctx);
        try {
            int value = 0;
            ctx->Jsonize("key", value);
            ASSERT_EQ(3, value);
        } catch (const autil::legacy::ExceptionBase &e) {
            ASSERT_TRUE(false) << "should not throw exception";
        }
    }
}

TEST_F(KernelConfigContextTest, testArray) {
    KernelTesterBuilder testerBuilder;
    testerBuilder.kernelConfig(R"json({"key" : {"types" : [{"value": "4"}]}})json");
    auto ctx = testerBuilder.buildConfigContext();
    ASSERT_TRUE(ctx);
    try {
        auto keyCtx = ctx->enter("key");
        ASSERT_TRUE(keyCtx.hasKey("types"));
        auto typeCtx = keyCtx.enter("types");
        ASSERT_TRUE(typeCtx.isArray());
        ASSERT_EQ(1, typeCtx.size());
        auto type0 = typeCtx.enter(0);
        string value;
        type0.Jsonize("value", value);
        ASSERT_EQ("4", value);
    } catch (const autil::legacy::ExceptionBase &e) {
        ASSERT_EQ("type not match, expect int64 but get: \"3\"", e.GetMessage());
    }
}

}
