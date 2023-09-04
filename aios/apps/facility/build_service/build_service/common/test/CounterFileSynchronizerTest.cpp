#include "build_service/common/CounterFileSynchronizer.h"

#include "build_service/test/unittest.h"
#include "fslib/util/FileUtil.h"
#include "indexlib/util/counter/AccumulativeCounter.h"
#include "indexlib/util/counter/CounterMap.h"
#include "indexlib/util/counter/StateCounter.h"

using namespace std;
using namespace testing;

BS_NAMESPACE_USE(util);
using namespace indexlib::util;

namespace build_service { namespace common {

class CounterFileSynchronizerTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp();
    void tearDown();
};

void CounterFileSynchronizerTest::setUp() {}

void CounterFileSynchronizerTest::tearDown() {}

TEST_F(CounterFileSynchronizerTest, testSimple)
{
    string syncPath = fslib::util::FileUtil::joinFilePath(GET_TEMP_DATA_PATH(), "counterFile");
    bool fileExist;
    CounterMapPtr counterMap1 = CounterFileSynchronizer::loadCounterMap(syncPath, fileExist);
    ASSERT_TRUE(counterMap1);
    ASSERT_FALSE(fileExist);

    CounterFileSynchronizer syner1;
    ASSERT_TRUE(syner1.init(counterMap1, syncPath));

    auto test1 = counterMap1->GetAccCounter("bs.testcounter1");
    test1->Increase(size_t(10));
    auto test2 = counterMap1->GetStateCounter("bs.testcounter2");
    test2->Set(size_t(20));
    syner1.sync();

    CounterMapPtr counterMap2 = CounterFileSynchronizer::loadCounterMap(syncPath, fileExist);
    ASSERT_TRUE(counterMap2);
    ASSERT_TRUE(fileExist);

    CounterFileSynchronizer syner2;
    ASSERT_TRUE(syner2.init(counterMap2, syncPath));

    CounterMapPtr counterMap3 = syner2.getCounterMap();

    test1 = counterMap3->GetAccCounter("bs.testcounter1");
    test2 = counterMap3->GetStateCounter("bs.testcounter2");

    ASSERT_TRUE(test1);
    ASSERT_TRUE(test2);

    EXPECT_EQ(size_t(10), test1->Get());
    EXPECT_EQ(size_t(20), test2->Get());
}

TEST_F(CounterFileSynchronizerTest, testInitException)
{
    // TODO: test invalid init parameter
}

TEST_F(CounterFileSynchronizerTest, testInitFromCounterMap)
{
    CounterMapPtr counterMap(new CounterMap());
    counterMap->GetAccCounter(BS_COUNTER(test1))->Increase(11);
    counterMap->GetStateCounter(BS_COUNTER(test.test2))->Set(12);
    string counterFilePath = fslib::util::FileUtil::joinFilePath(GET_TEMP_DATA_PATH(), "counterFile");

    CounterFileSynchronizer counterFileSynchronizer;
    ASSERT_TRUE(counterFileSynchronizer.init(counterMap, counterFilePath));
    ASSERT_TRUE(counterFileSynchronizer.sync());

    CounterFileSynchronizerPtr syner1(new CounterFileSynchronizer());
    bool fileExist;
    CounterMapPtr newCounterMap = CounterFileSynchronizer::loadCounterMap(counterFilePath, fileExist);
    ASSERT_TRUE(fileExist);
    ASSERT_TRUE(syner1->init(newCounterMap, counterFilePath));
    ASSERT_EQ(counterMap->ToJsonString(), syner1->getCounterMap()->ToJsonString());
}

TEST_F(CounterFileSynchronizerTest, testBeginSync)
{
    CounterMapPtr counterMap(new CounterMap());
    counterMap->GetAccCounter(BS_COUNTER(test1))->Increase(11);
    counterMap->GetStateCounter(BS_COUNTER(test.test2))->Set(12);
    string counterFilePath = fslib::util::FileUtil::joinFilePath(GET_TEMP_DATA_PATH(), "counterFile");
    CounterFileSynchronizer counterFileSynchronizer;
    ASSERT_TRUE(counterFileSynchronizer.init(counterMap, counterFilePath));

    // In this case, sync will write counter to file,
    // but flush is not force, so maybe file has been cleaned and flush not done
    // then loadCounterMap read the (existed) file but content is empty.
    // Now we just use long duration sleep to reduce the flush latency's affect
    ASSERT_TRUE(counterFileSynchronizer.beginSync(3));
    sleep(5);
    {
        bool fileExist;
        CounterMapPtr newCounterMap = CounterFileSynchronizer::loadCounterMap(counterFilePath, fileExist);
        ASSERT_TRUE(fileExist);
        CounterFileSynchronizerPtr syner(new CounterFileSynchronizer());
        ASSERT_TRUE(syner->init(newCounterMap, counterFilePath));
        ASSERT_EQ(counterMap->ToJsonString(), syner->getCounterMap()->ToJsonString());
    }
    counterMap->GetAccCounter(BS_COUNTER(test1))->Increase(21);
    counterMap->GetStateCounter(BS_COUNTER(test.test2))->Set(22);
    sleep(5);
    {
        bool fileExist;
        CounterMapPtr newCounterMap = CounterFileSynchronizer::loadCounterMap(counterFilePath, fileExist);
        ASSERT_TRUE(fileExist);
        CounterFileSynchronizerPtr syner(new CounterFileSynchronizer());
        ASSERT_TRUE(syner->init(newCounterMap, counterFilePath));
        ASSERT_EQ(counterMap->ToJsonString(), syner->getCounterMap()->ToJsonString());
    }
}

}} // namespace build_service::common
