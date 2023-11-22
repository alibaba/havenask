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

class AsyncSourceKernelTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
};

void AsyncSourceKernelTest::setUp() {
}

void AsyncSourceKernelTest::tearDown() {
}

TEST_F(AsyncSourceKernelTest, testPipeInInitKernel) {
    KernelTesterBuilder testerBuilder;
    testerBuilder.module(NAVI_TEST_DATA_PATH + "config/modules/libtest_plugin.so");
    testerBuilder.kernel("AsyncInInitSourceKernel");
    testerBuilder.output("output1");
    testerBuilder.snapshotResourceConfig(GraphMemoryPoolR::RESOURCE_ID, R"json({"test" : true})json");
    testerBuilder.snapshotResourceConfig(MemoryPoolR::RESOURCE_ID, R"json({"use_asan_pool" : true})json");

    KernelTesterPtr testerPtr(testerBuilder.build());
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;

    auto *kernel = dynamic_cast<AsyncInInitSourceKernel *>(tester.getKernel());
    ASSERT_NE(nullptr, kernel);

    auto pipe = kernel->getAsyncPipe();
    ASSERT_NE(nullptr, pipe);
    {
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
    {
        ASSERT_FALSE(tester.compute());
        auto inputData = make_shared<HelloData>("bbbbb");
        pipe->setData(inputData);
        ASSERT_TRUE(tester.compute());

        DataPtr odata;
        bool eof = true;
        ASSERT_TRUE(tester.getOutput("output1", odata, eof));
        ASSERT_FALSE(eof);
        ASSERT_EQ(inputData.get(), odata.get());

        ASSERT_FALSE(tester.compute());
    }
    {
        ASSERT_FALSE(tester.compute());
        auto inputData1 = make_shared<HelloData>("ccccc");
        pipe->setData(inputData1);
        auto inputData2 = make_shared<HelloData>("ddddd");
        pipe->setData(inputData2);
        pipe->setEof();

        ASSERT_TRUE(tester.compute());
        DataPtr odata;
        bool eof = true;
        ASSERT_TRUE(tester.getOutput("output1", odata, eof));
        ASSERT_FALSE(eof);
        ASSERT_EQ(inputData1.get(), odata.get());

        ASSERT_TRUE(tester.compute());
        ASSERT_TRUE(tester.getOutput("output1", odata, eof));
        ASSERT_TRUE(eof);
        ASSERT_EQ(inputData2.get(), odata.get());

        ASSERT_FALSE(tester.compute());
    }
}

TEST_F(AsyncSourceKernelTest, testPipeInInitKernel_Abort) {
    KernelTesterBuilder testerBuilder;
    testerBuilder.module(NAVI_TEST_DATA_PATH + "config/modules/libtest_plugin.so");
    testerBuilder.kernel("AsyncInInitSourceKernel");
    testerBuilder.output("output1");
    testerBuilder.snapshotResourceConfig(GraphMemoryPoolR::RESOURCE_ID, R"json({"test" : true})json");
    testerBuilder.snapshotResourceConfig(MemoryPoolR::RESOURCE_ID, R"json({"use_asan_pool" : true})json");

    KernelTesterPtr testerPtr(testerBuilder.build());
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;

    auto *kernel = dynamic_cast<AsyncInInitSourceKernel *>(tester.getKernel());
    ASSERT_NE(nullptr, kernel);

    auto pipe = kernel->getAsyncPipe();
    ASSERT_NE(nullptr, pipe);
    {
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
    {
        ASSERT_FALSE(tester.compute());
        auto inputData = make_shared<ErrorCommandData>();
        pipe->setData(inputData);
        ASSERT_TRUE(tester.compute());

        ASSERT_EQ(EC_ABORT, tester.getErrorCode());
        ASSERT_EQ("error command got, will abort", tester.getErrorMessage());
    }
}

TEST_F(AsyncSourceKernelTest, testPipeInInitKernel_Destruct) {
    KernelTesterBuilder testerBuilder;
    testerBuilder.module(NAVI_TEST_DATA_PATH + "config/modules/libtest_plugin.so");
    testerBuilder.kernel("AsyncInInitSourceKernel");
    testerBuilder.output("output1");
    testerBuilder.snapshotResourceConfig(GraphMemoryPoolR::RESOURCE_ID, R"json({"test" : true})json");
    testerBuilder.snapshotResourceConfig(MemoryPoolR::RESOURCE_ID, R"json({"use_asan_pool" : true})json");

    KernelTesterPtr testerPtr(testerBuilder.build());
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    auto *kernel = dynamic_cast<AsyncInInitSourceKernel *>(tester.getKernel());
    ASSERT_NE(nullptr, kernel);
    auto pipe = kernel->getAsyncPipe();
    ASSERT_NE(nullptr, pipe);
    testerPtr.reset();
    pipe.reset();
}

TEST_F(AsyncSourceKernelTest, testPipeInInitKernel_Concurrent) {
    NaviGraphRunner naviGraphRunner;
    ASSERT_TRUE(naviGraphRunner.init());

    auto graphDef = std::make_unique<GraphDef>();
    GraphBuilder builder(graphDef.get());
    builder.newSubGraph(naviGraphRunner.getBizName());
    builder.node("node1").kernel("AsyncInInitSourceKernel").out("output1").asGraphOutput("o");
    ASSERT_TRUE(builder.ok());

    AsyncInInitSourceKernel::lastPipe.reset();
    auto naviUserResult = naviGraphRunner.runLocalGraph(graphDef.release(), {});
    double timeout = 10;
    while (timeout > 0) {
        if (AsyncInInitSourceKernel::lastPipe) {
            break;
        }
        usleep(0.1 * 1000 * 1000);
        timeout -= 0.1;
    }
    ASSERT_LT(0, timeout) << "get pipe timeout";

    constexpr size_t THREAD_NUM = 10, INPUT_NUM = 1000;
    std::atomic<size_t> left(THREAD_NUM);
    WaitClosure waitClosure;
    std::vector<ThreadPtr> requestThreadList;
    for (size_t i = 0; i < THREAD_NUM; ++i) {
        requestThreadList.emplace_back(autil::Thread::createThread([&waitClosure, &left]() {
            waitClosure.wait();
            auto inputData = make_shared<HelloData>("aaaaa");
            for (size_t i = 0; i < INPUT_NUM; ++i) {
                AsyncInInitSourceKernel::lastPipe->setData(inputData);
            }
            if (--left == 0) {
                AsyncInInitSourceKernel::lastPipe->setEof();
            }
        }));
    }
    waitClosure.notify();

    size_t dataCount = 0;
    while (true) {
        NaviUserData data;
        bool eof = false;
        if (naviUserResult->nextData(data, eof)) {
            dataCount += data.data ? 1 : 0;
        }
        if (eof) {
            break;
        }
    }
    for (const auto &t : requestThreadList) {
        t->join();
    }
    auto result = naviUserResult->getNaviResult();
    EXPECT_EQ(EC_NONE, result->getErrorCode());
    EXPECT_EQ("", result->getErrorMessage());
    ASSERT_EQ(THREAD_NUM * INPUT_NUM, dataCount);
}

TEST_F(AsyncSourceKernelTest, testPipeInInitKernel_Concurrent2) {
    KernelTesterBuilder testerBuilder;
    // testerBuilder.logLevel("SCHEDULE2");
    testerBuilder.module(NAVI_TEST_DATA_PATH + "config/modules/libtest_plugin.so");
    testerBuilder.kernel("AsyncInInitSourceKernel");
    testerBuilder.output("output1");
    testerBuilder.snapshotResourceConfig(GraphMemoryPoolR::RESOURCE_ID, R"json({"test" : true})json");
    testerBuilder.snapshotResourceConfig(MemoryPoolR::RESOURCE_ID, R"json({"use_asan_pool" : true})json");

    KernelTesterPtr testerPtr(testerBuilder.build());
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    auto *kernel = dynamic_cast<AsyncInInitSourceKernel *>(tester.getKernel());
    ASSERT_NE(nullptr, kernel);

    auto pipe = kernel->getAsyncPipe();
    ASSERT_NE(nullptr, pipe);

    constexpr size_t THREAD_NUM = 10;
    std::atomic<size_t> left = THREAD_NUM;
    WaitClosure waitClosure;
    auto threadFunc = [&pipe, &waitClosure, &left]() {
        waitClosure.wait();
        // NaviLoggerProvider provider("SCHEDULE2");
        auto inputData = make_shared<HelloData>("aaaaa");
        for (size_t i = 0; i < 1000; ++i) {
            pipe->setData(inputData);
        }
        if (--left == 0) {
            pipe->setEof();
        }
    };

    std::vector<ThreadPtr> requestThreadList;
    for (size_t i = 0; i < THREAD_NUM; ++i) {
        requestThreadList.emplace_back(autil::Thread::createThread(threadFunc));
    }

    waitClosure.notify();
    size_t dataCount = 0;
    while (true) {
        if (!tester.compute()) {
            ASSERT_TRUE(tester.getNode()->outputOk());
            continue;
        }
        DataPtr odata;
        bool eof = true;
        ASSERT_TRUE(tester.getOutput("output1", odata, eof));
        if (odata != nullptr) {
            ++dataCount;
        }
        if (eof) {
            break;
        }
    }
    ASSERT_EQ(THREAD_NUM * 1000, dataCount);
    for (const auto &t : requestThreadList) {
        t->join();
    }
}

TEST_F(AsyncSourceKernelTest, testPipeInComputeKernel) {
    KernelTesterBuilder testerBuilder;
    testerBuilder.module(NAVI_TEST_DATA_PATH + "config/modules/libtest_plugin.so");
    testerBuilder.kernel("AsyncInComputeSourceKernel");
    testerBuilder.output("output1");
    testerBuilder.snapshotResourceConfig(GraphMemoryPoolR::RESOURCE_ID, R"json({"test" : true})json");
    testerBuilder.snapshotResourceConfig(MemoryPoolR::RESOURCE_ID, R"json({"use_asan_pool" : true})json");

    KernelTesterPtr testerPtr(testerBuilder.build());
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;

    auto *kernel = dynamic_cast<AsyncInComputeSourceKernel *>(tester.getKernel());
    ASSERT_NE(nullptr, kernel);

    ASSERT_TRUE(tester.compute());
    auto pipe = kernel->getAsyncPipe();
    ASSERT_NE(nullptr, pipe);

    {
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

    {
        ASSERT_FALSE(tester.compute());
        auto inputData = make_shared<HelloData>("bbbbb");
        pipe->setData(inputData);
        ASSERT_TRUE(tester.compute());

        DataPtr odata;
        bool eof = true;
        ASSERT_TRUE(tester.getOutput("output1", odata, eof));
        ASSERT_FALSE(eof);
        ASSERT_EQ(inputData.get(), odata.get());

        ASSERT_FALSE(tester.compute());
    }

    {
        ASSERT_FALSE(tester.compute());
        auto inputData1 = make_shared<HelloData>("ccccc");
        pipe->setData(inputData1);
        auto inputData2 = make_shared<HelloData>("ddddd");
        pipe->setData(inputData2);
        pipe->setEof();

        ASSERT_TRUE(tester.compute());
        DataPtr odata;
        bool eof = true;
        ASSERT_TRUE(tester.getOutput("output1", odata, eof));
        ASSERT_FALSE(eof);
        ASSERT_EQ(inputData1.get(), odata.get());

        ASSERT_TRUE(tester.compute());
        ASSERT_TRUE(tester.getOutput("output1", odata, eof));
        ASSERT_TRUE(eof);
        ASSERT_EQ(inputData2.get(), odata.get());

        ASSERT_FALSE(tester.compute());
    }
}

TEST_F(AsyncSourceKernelTest, testPipeInComputeKernel_Abort) {
    KernelTesterBuilder testerBuilder;
    testerBuilder.module(NAVI_TEST_DATA_PATH + "config/modules/libtest_plugin.so");
    testerBuilder.kernel("AsyncInComputeSourceKernel");
    testerBuilder.output("output1");
    testerBuilder.snapshotResourceConfig(GraphMemoryPoolR::RESOURCE_ID, R"json({"test" : true})json");
    testerBuilder.snapshotResourceConfig(MemoryPoolR::RESOURCE_ID, R"json({"use_asan_pool" : true})json");

    KernelTesterPtr testerPtr(testerBuilder.build());
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;

    auto *kernel = dynamic_cast<AsyncInComputeSourceKernel *>(tester.getKernel());
    ASSERT_NE(nullptr, kernel);

    ASSERT_TRUE(tester.compute());
    auto pipe = kernel->getAsyncPipe();
    ASSERT_NE(nullptr, pipe);

    {
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

    {
        ASSERT_FALSE(tester.compute());
        auto inputData = make_shared<ErrorCommandData>();
        pipe->setData(inputData);
        ASSERT_TRUE(tester.compute());

        ASSERT_EQ(EC_ABORT, tester.getErrorCode());
        ASSERT_EQ("error command got, will abort", tester.getErrorMessage());
    }
}

TEST_F(AsyncSourceKernelTest, testPipeInComputeKernel_Concurrent) {
    NaviGraphRunner naviGraphRunner;
    ASSERT_TRUE(naviGraphRunner.init());

    auto graphDef = std::make_unique<GraphDef>();
    GraphBuilder builder(graphDef.get());
    builder.newSubGraph(naviGraphRunner.getBizName());
    builder.node("node1").kernel("AsyncInComputeSourceKernel").out("output1").asGraphOutput("o");
    ASSERT_TRUE(builder.ok());

    AsyncInComputeSourceKernel::lastPipe.reset();
    auto naviUserResult = naviGraphRunner.runLocalGraph(graphDef.release(), {});
    double timeout = 10;
    while (timeout > 0) {
        if (AsyncInComputeSourceKernel::lastPipe) {
            break;
        }
        usleep(0.1 * 1000 * 1000);
        timeout -= 0.1;
    }
    ASSERT_LT(0, timeout) << "get pipe timeout";

    constexpr size_t THREAD_NUM = 10, INPUT_NUM = 1000;
    std::atomic<size_t> left(THREAD_NUM);
    WaitClosure waitClosure;
    std::vector<ThreadPtr> requestThreadList;
    for (size_t i = 0; i < THREAD_NUM; ++i) {
        requestThreadList.emplace_back(autil::Thread::createThread([&waitClosure, &left]() {
            waitClosure.wait();
            auto inputData = make_shared<HelloData>("aaaaa");
            for (size_t i = 0; i < INPUT_NUM; ++i) {
                AsyncInComputeSourceKernel::lastPipe->setData(inputData);
            }
            if (--left == 0) {
                AsyncInComputeSourceKernel::lastPipe->setEof();
            }
        }));
    }
    waitClosure.notify();

    size_t dataCount = 0;
    while (true) {
        NaviUserData data;
        bool eof = false;
        if (naviUserResult->nextData(data, eof)) {
            dataCount += data.data ? 1 : 0;
        }
        if (eof) {
            break;
        }
    }
    for (const auto &t : requestThreadList) {
        t->join();
    }
    auto result = naviUserResult->getNaviResult();
    EXPECT_EQ(EC_NONE, result->getErrorCode());
    EXPECT_EQ("", result->getErrorMessage());
    ASSERT_EQ(THREAD_NUM * INPUT_NUM, dataCount);
}

TEST_F(AsyncSourceKernelTest, testPipeWithoutOutputKernel) {
    // NaviLoggerProvider logProvider("SCHEDULE2");
    KernelTesterBuilder testerBuilder;
    testerBuilder.module(NAVI_TEST_DATA_PATH + "config/modules/libtest_plugin.so");
    testerBuilder.kernel("AsyncWithoutOutputSourceKernel");
    testerBuilder.input("input1");

    testerBuilder.snapshotResourceConfig(GraphMemoryPoolR::RESOURCE_ID, R"json({"test" : true})json");
    testerBuilder.snapshotResourceConfig(MemoryPoolR::RESOURCE_ID, R"json({"use_asan_pool" : true})json");

    KernelTesterPtr testerPtr(testerBuilder.build());
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;

    auto *kernel = dynamic_cast<AsyncWithoutOutputSourceKernel *>(tester.getKernel());
    ASSERT_NE(nullptr, kernel);

    DataPtr idata0(new HelloData("test0"));
    ASSERT_TRUE(tester.setInput("input1", idata0));
    ASSERT_TRUE(tester.compute());
    ASSERT_EQ(nullptr, kernel->getAsyncPipe());

    ASSERT_TRUE(tester.setInput("input1", idata0));
    ASSERT_TRUE(tester.compute());
    ASSERT_EQ(nullptr, kernel->getAsyncPipe());

    ASSERT_TRUE(tester.setInputEof("input1"));
    ASSERT_TRUE(tester.compute());
    auto pipe = kernel->getAsyncPipe();
    ASSERT_NE(nullptr, pipe);

    {
        ASSERT_FALSE(tester.compute());
        auto inputData = make_shared<HelloData>("aaaaa");
        pipe->setData(inputData);
        ASSERT_TRUE(tester.compute());
    }

    {
        ASSERT_FALSE(tester.compute());
        auto inputData = make_shared<HelloData>("bbbbb");
        pipe->setData(inputData);
        ASSERT_TRUE(tester.compute());
    }

    {
        ASSERT_FALSE(tester.compute());
        // auto inputData1 = make_shared<HelloData>("ccccc");
        // pipe->setData(inputData1);
        auto inputData2 = make_shared<HelloData>("ddddd");
        pipe->setData(inputData2);
        pipe->setEof();
        // ASSERT_TRUE(tester.compute());
        ASSERT_TRUE(tester.compute());

        auto *node = tester.getNode();
        ASSERT_NE(nullptr, node);
        ASSERT_FALSE(node->getAsyncInfo());
        ASSERT_TRUE(node->isFinished());
    }
}
TEST_F(AsyncSourceKernelTest, testPipeInitSlowKernel) {
    NaviGraphRunner naviGraphRunner;
    ASSERT_TRUE(naviGraphRunner.init());

    auto graphDef = std::make_unique<GraphDef>();
    GraphBuilder builder(graphDef.get());
    builder.newSubGraph(naviGraphRunner.getBizName());
    builder.node("node1").kernel("AsyncInitSlowSourceKernel").out("output1").asGraphOutput("o");
    ASSERT_TRUE(builder.ok());

    AsyncInitSlowSourceKernel::lastPipe.reset();
    AsyncInitSlowSourceKernel::lastClosure = nullptr;
    auto naviUserResult = naviGraphRunner.runLocalGraph(graphDef.release(), {});
    double timeout = 10;
    while (timeout > 0) {
        if (AsyncInitSlowSourceKernel::lastPipe && AsyncInitSlowSourceKernel::lastClosure) {
            break;
        }
        usleep(0.1 * 1000 * 1000);
        timeout -= 0.1;
    }
    ASSERT_LT(0, timeout) << "get pipe timeout";

    constexpr size_t THREAD_NUM = 10, INPUT_NUM = 1000;
    std::atomic<size_t> left(THREAD_NUM);
    WaitClosure waitClosure;
    std::vector<ThreadPtr> requestThreadList;
    for (size_t i = 0; i < THREAD_NUM; ++i) {
        requestThreadList.emplace_back(autil::Thread::createThread([&waitClosure, &left]() {
            waitClosure.wait();
            auto inputData = make_shared<HelloData>("aaaaa");
            for (size_t i = 0; i < INPUT_NUM; ++i) {
                AsyncInitSlowSourceKernel::lastPipe->setData(inputData);
            }
            if (--left == 0) {
                AsyncInitSlowSourceKernel::lastPipe->setEof();
            }
        }));
    }
    waitClosure.notify();
    AsyncInitSlowSourceKernel::lastClosure->notify();

    size_t dataCount = 0;
    while (true) {
        NaviUserData data;
        bool eof = false;
        if (naviUserResult->nextData(data, eof)) {
            dataCount += data.data ? 1 : 0;
        }
        if (eof) {
            break;
        }
    }
    for (const auto &t : requestThreadList) {
        t->join();
    }
    auto result = naviUserResult->getNaviResult();
    EXPECT_EQ(EC_NONE, result->getErrorCode());
    EXPECT_EQ("", result->getErrorMessage());
    ASSERT_EQ(THREAD_NUM * INPUT_NUM, dataCount);
}

} // namespace navi
