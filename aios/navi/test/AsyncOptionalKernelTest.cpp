#include "autil/Thread.h"
#include "navi/builder/GraphBuilder.h"
#include "navi/engine/Node.h"
#include "navi/engine/NaviSnapshot.h"
#include "navi/example/TestData.h"
#include "navi/example/TestKernel.h"
#include "navi/example/TestResource.h"
#include "navi/log/NaviLoggerProvider.h"
#include "navi/test_cluster/NaviGraphRunner.h"
#include "navi/tester/KernelTesterBuilder.h"
#include "navi/tester/TesterDef.h"
#include "navi/util/NaviClosure.h"
#include "unittest/unittest.h"
#include "unittest/TestEnvScope.h"

using namespace std;
using namespace testing;
using namespace autil;

namespace navi {

class AsyncOptionalKernelTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
};

void AsyncOptionalKernelTest::setUp() {
}

void AsyncOptionalKernelTest::tearDown() {
}

TEST_F(AsyncOptionalKernelTest, testPipeInInitKernel) {
    KernelTesterBuilder testerBuilder;
    testerBuilder.module(GET_PRIVATE_TEST_DATA_PATH() + "config/modules/libtest_plugin.so");
    testerBuilder.kernel("AsyncOptionalMixKernel");
    testerBuilder.input("input1");
    testerBuilder.output("output1");
    testerBuilder.snapshotResourceConfig(GraphMemoryPoolR::RESOURCE_ID, R"json({"test" : true})json");
    testerBuilder.snapshotResourceConfig(MemoryPoolR::RESOURCE_ID, R"json({"use_asan_pool" : true})json");


    KernelTesterPtr testerPtr(testerBuilder.build());
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;

    auto *kernel = dynamic_cast<AsyncOptionalMixKernel *>(tester.getKernel());
    ASSERT_NE(nullptr, kernel);

    auto pipe = kernel->getAsyncPipe();
    ASSERT_NE(nullptr, pipe);

    { // only async pipe input
        ASSERT_FALSE(tester.compute());
        auto inputData = make_shared<HelloData>("aaaaa");
        pipe->setData(inputData);
        ASSERT_TRUE(tester.compute());

        DataPtr odata;
        bool eof = true;
        ASSERT_TRUE(tester.getOutput("output1", odata, eof));
        ASSERT_FALSE(eof);
        ASSERT_EQ(inputData.get(), odata.get());

        ASSERT_FALSE(tester.compute());
    }

    { // only traditional input
        ASSERT_FALSE(tester.compute());
        auto inputData = make_shared<HelloData>("bbbbb");
        tester.setInput("input1", inputData);
        ASSERT_TRUE(tester.compute());

        DataPtr odata;
        bool eof = true;
        ASSERT_TRUE(tester.getOutput("output1", odata, eof));
        ASSERT_FALSE(eof);
        ASSERT_EQ(inputData.get(), odata.get());

        ASSERT_FALSE(tester.compute());
    }

    {
        // mix pipe and traditional input
        ASSERT_FALSE(tester.compute());
        auto inputData1 = make_shared<HelloData>("ccccc");
        tester.setInput("input1", inputData1);
        auto inputData2 = make_shared<HelloData>("ddddd");
        pipe->setData(inputData2);
        pipe->setEof();

        ASSERT_TRUE(tester.compute());
        DataPtr odata;
        bool eof = true;
        ASSERT_TRUE(tester.getOutput("output1", odata, eof));
        ASSERT_TRUE(eof);
        ASSERT_EQ(inputData2.get(), odata.get());

        ASSERT_FALSE(tester.compute());
    }
}

} // namespace navi
