#include "navi/example/TestData.h"
#include "navi/tester/KernelTesterBuilder.h"
#include "navi/resource/GraphMemoryPoolR.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;

namespace navi {

class KernelTesterTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
};

void KernelTesterTest::setUp() {
}

void KernelTesterTest::tearDown() {
}

TEST_F(KernelTesterTest, testSimple) {
    KernelTesterBuilder testerBuilder;
    testerBuilder.module(GET_PRIVATE_TEST_DATA_PATH() + "config/modules/libtest_plugin.so");
    testerBuilder.kernel("HelloKernelOld");
    testerBuilder.input("input1");
    testerBuilder.output("output1");
    testerBuilder.snapshotResourceConfig(GraphMemoryPoolR::RESOURCE_ID,
            R"json({"test" : true})json");
    testerBuilder.snapshotResourceConfig(MemoryPoolR::RESOURCE_ID,
            R"json({"use_asan_pool" : true})json");
    {
        KernelTesterPtr testerPtr(testerBuilder.build("test lack attribute"));
        ASSERT_TRUE(testerPtr.get());
        ASSERT_EQ(EC_INVALID_ATTRIBUTE, testerPtr->getErrorCode());
    }
    {
        testerBuilder.attrs(R"json({"times" : -1})json");
        KernelTesterPtr testerPtr(testerBuilder.build("test negative attribute"));
        ASSERT_TRUE(testerPtr.get());
        ASSERT_EQ(EC_INVALID_ATTRIBUTE, testerPtr->getErrorCode());
    }
    {
        testerBuilder.attrs(R"json({"times" : 1})json");
        KernelTesterPtr testerPtr(testerBuilder.build("test normal attribute"));
        ASSERT_TRUE(testerPtr.get());

        auto &tester = *testerPtr;
        DataPtr idata0(new HelloData("test0"));
        ASSERT_TRUE(tester.setInput("input1", idata0));
        ASSERT_TRUE(tester.compute());
        ASSERT_TRUE(tester.getKernel());
        DataPtr odata;
        bool eof = false;
        ASSERT_TRUE(tester.getOutput("output1", odata, eof));
        ASSERT_FALSE(eof);
        std::string expData0("0_hello:1_test0");
        odata->show();
        ASSERT_EQ(expData0, dynamic_pointer_cast<HelloData>(odata)->getData()[0]);

        DataPtr idata1(new HelloData("test1"));
        ASSERT_TRUE(tester.setInput("input1", idata1));
        ASSERT_TRUE(tester.compute());
        ASSERT_TRUE(tester.getOutput("output1", odata, eof));
        ASSERT_TRUE(eof);
        std::string expData1("1_hello:1_test1");
        odata->show();
        ASSERT_EQ(expData1, dynamic_pointer_cast<HelloData>(odata)->getData()[0]);

        DataPtr idata2(new HelloData("test2"));
        ASSERT_FALSE(tester.setInput("input1", idata2));
    }
}

}
