#include "indexlib/partition/test/flushed_locator_container_unittest.h"

#include "autil/StringUtil.h"

using namespace std;
using namespace autil;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, FlushedLocatorContainerTest);

FlushedLocatorContainerTest::FlushedLocatorContainerTest() {}

FlushedLocatorContainerTest::~FlushedLocatorContainerTest() {}

void FlushedLocatorContainerTest::CaseSetUp() {}

void FlushedLocatorContainerTest::CaseTearDown() {}

void FlushedLocatorContainerTest::TestSimpleProcess()
{
    FlushedLocatorContainer container(2);
    EXPECT_FALSE(container.HasFlushingLocator());

    std::promise<bool> testPromise0;
    auto testFuture0 = testPromise0.get_future().share();
    document::Locator locator0;
    locator0.SetLocator("1");
    container.Push(testFuture0, locator0);
    EXPECT_TRUE(container.HasFlushingLocator());

    std::promise<bool> testPromise1;
    auto testFuture1 = testPromise1.get_future().share();
    document::Locator locator1;
    locator1.SetLocator("2");
    container.Push(testFuture1, locator1);

    EXPECT_FALSE(container.GetLastFlushedLocator().IsValid());

    EXPECT_TRUE(container.HasFlushingLocator());
    testPromise0.set_value(true);
    EXPECT_TRUE(container.HasFlushingLocator());
    EXPECT_EQ(locator0, container.GetLastFlushedLocator());

    EXPECT_EQ(locator0, container.GetLastFlushedLocator());

    testPromise1.set_value(true);
    EXPECT_EQ(locator1, container.GetLastFlushedLocator());
    EXPECT_FALSE(container.HasFlushingLocator());

    // pop the first two locators out of container
    vector<std::promise<bool>> promises;
    for (int i = 0; i < 2; ++i) {
        std::promise<bool> testPromise;
        auto testFuture = testPromise.get_future().share();
        document::Locator locator;
        locator.SetLocator(string("x") + StringUtil::toString(i));
        container.Push(testFuture, locator);
        promises.push_back(std::move(testPromise));
    }
    // test when promise is set to false, the corresponding locator is considered as not flushed.
    EXPECT_EQ(locator1, container.GetLastFlushedLocator());
    promises[1].set_value(false);
    promises[0].set_value(true);
    promises.clear();
    EXPECT_FALSE(container.HasFlushingLocator());
    document::Locator locator("x0");
    EXPECT_EQ(locator, container.GetLastFlushedLocator());
}
}} // namespace indexlib::partition
