#include "build_service/test/unittest.h"
#include "build_service/common/CounterFileSynchronizer.h"
#include "build_service/util/FileUtil.h"
#include "indexlib/util/counter/accumulative_counter.h"
#include "indexlib/util/counter/state_counter.h"
#include "indexlib/util/counter/counter_map.h"

using namespace std;
using namespace testing;

BS_NAMESPACE_USE(util);
IE_NAMESPACE_USE(util);

namespace build_service {
namespace common {

class CounterFileSynchronizerTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
};

void CounterFileSynchronizerTest::setUp() {
}

void CounterFileSynchronizerTest::tearDown() {
}

TEST_F(CounterFileSynchronizerTest, testSimple) {
    string syncPath = FileUtil::joinFilePath(GET_TEST_DATA_PATH(), "counterFile");
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

TEST_F(CounterFileSynchronizerTest, testInitException) {
    // TODO: test invalid init parameter
}

TEST_F(CounterFileSynchronizerTest, testInitFromCounterMap) {
    CounterMapPtr counterMap(new CounterMap());
    counterMap->GetAccCounter(BS_COUNTER(test1))->Increase(11);
    counterMap->GetStateCounter(BS_COUNTER(test.test2))->Set(12);
    string counterFilePath = FileUtil::joinFilePath(GET_TEST_DATA_PATH(), "counterFile");
    
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

TEST_F(CounterFileSynchronizerTest, testBeginSync) {
    CounterMapPtr counterMap(new CounterMap());
    counterMap->GetAccCounter(BS_COUNTER(test1))->Increase(11);
    counterMap->GetStateCounter(BS_COUNTER(test.test2))->Set(12);
    string counterFilePath = FileUtil::joinFilePath(GET_TEST_DATA_PATH(), "counterFile");
    CounterFileSynchronizer counterFileSynchronizer;
    ASSERT_TRUE(counterFileSynchronizer.init(counterMap, counterFilePath));
    ASSERT_TRUE(counterFileSynchronizer.beginSync(2));
    sleep(3);
    {
        bool fileExist;
        CounterMapPtr newCounterMap = CounterFileSynchronizer::loadCounterMap(
                counterFilePath, fileExist);
        ASSERT_TRUE(fileExist);
        CounterFileSynchronizerPtr syner(new CounterFileSynchronizer());
        ASSERT_TRUE(syner->init(newCounterMap, counterFilePath));
        ASSERT_EQ(counterMap->ToJsonString(), syner->getCounterMap()->ToJsonString());        
    }
    counterMap->GetAccCounter(BS_COUNTER(test1))->Increase(21);
    counterMap->GetStateCounter(BS_COUNTER(test.test2))->Set(22);    
    sleep(3);
    {
        bool fileExist;
        CounterMapPtr newCounterMap = CounterFileSynchronizer::loadCounterMap(
                counterFilePath, fileExist); 
        ASSERT_TRUE(fileExist);
        CounterFileSynchronizerPtr syner(new CounterFileSynchronizer());
        ASSERT_TRUE(syner->init(newCounterMap, counterFilePath));
        ASSERT_EQ(counterMap->ToJsonString(), syner->getCounterMap()->ToJsonString()); 
    }    
}

}
}
