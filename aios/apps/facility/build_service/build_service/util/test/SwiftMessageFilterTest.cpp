#include "build_service/util/SwiftMessageFilter.h"

#include "build_service/test/unittest.h"
#include "indexlib/framework/Locator.h"

using namespace std;
using namespace testing;

namespace build_service { namespace util {

class SwiftMessageFilterTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp() {}
    void tearDown() {}

    std::vector<indexlibv2::base::Progress>
    makeLocatorProgress(const std::vector<std::tuple<uint32_t, uint32_t, int64_t, uint32_t>>& param)
    {
        std::vector<indexlibv2::base::Progress> progress;
        for (const auto& [from, to, ts, concurrentIdx] : param) {
            progress.push_back({from, to, {ts, concurrentIdx}});
        }
        return progress;
    }
};

TEST_F(SwiftMessageFilterTest, testSimple)
{
    {
        SwiftMessageFilter filter;
        for (size_t i = 0; i < 30; i++) {
            uint16_t payload = 0;
            int64_t ts = i;
            indexlibv2::framework::Locator locator(0, ts + 1);
            auto progress = locator.GetProgress();
            ASSERT_FALSE(filter.filterOrRewriteProgress(payload, {ts, 0}, &progress));
            ASSERT_EQ(locator.GetProgress(), progress);
        }
    }
    {
        SwiftMessageFilter filter;
        for (size_t i = 0; i < 30; i++) {
            uint16_t payload = 0;
            int64_t ts = i;
            indexlibv2::framework::Locator locator(0, {1, ts + 1});
            auto progress = locator.GetProgress();
            ASSERT_FALSE(filter.filterOrRewriteProgress(payload, {1, ts}, &progress));
            ASSERT_EQ(locator.GetProgress(), progress);
        }
    }
}

TEST_F(SwiftMessageFilterTest, testSeek)
{
    SwiftMessageFilter filter;
    auto seekProgress = makeLocatorProgress({{0, 100, -1, 0}, {101, 200, 110, 0}});
    filter.setSeekProgress(seekProgress);

    //  [payload, docTs]
    //  [50, 99] (need rewrite)
    //  [150, 100] (need filter)
    //  [50, 130] (not rewrite and not filter)
    {
        auto progress = makeLocatorProgress({{0, 200, 100, 0}});
        ASSERT_FALSE(filter.filterOrRewriteProgress(/*payload*/ 50, {/*timestamp*/ 99, 0}, &progress));
        indexlibv2::framework::Locator locator;
        locator.SetProgress(progress);
        ASSERT_EQ(makeLocatorProgress({{0, 100, 100, 0}, {101, 200, 110, 0}}), progress) << locator.DebugString();
    }
    {
        auto progress = makeLocatorProgress({{0, 200, 130, 0}});
        ASSERT_TRUE(filter.filterOrRewriteProgress(/*payload*/ 150, {/*timestamp*/ 100, 0}, &progress));
    }
    {
        auto progress = makeLocatorProgress({{0, 200, 150, 0}});
        ASSERT_FALSE(filter.filterOrRewriteProgress(/*payload*/ 50, {/*timestamp*/ 130, 0}, &progress));
        ASSERT_EQ(makeLocatorProgress({{0, 200, 150, 0}}), progress);
    }
}

TEST_F(SwiftMessageFilterTest, testSeekWithSubIdx)
{
    SwiftMessageFilter filter;
    auto seekProgress = makeLocatorProgress({{0, 100, 1, 0}, {101, 200, 1, 110}});
    filter.setSeekProgress(seekProgress);

    //  [payload, docTs]
    //  [50, 99] (need rewrite)
    //  [150, 100] (need filter)
    //  [50, 130] (not rewrite and not filter)
    {
        auto progress = makeLocatorProgress({{0, 200, 1, 100}});
        ASSERT_FALSE(
            filter.filterOrRewriteProgress(/*payload*/ 50, {/*timestamp*/ 1, /*concurrentIdx*/ 99}, &progress));
        indexlibv2::framework::Locator locator;
        locator.SetProgress(progress);
        ASSERT_EQ(makeLocatorProgress({{0, 100, 1, 100}, {101, 200, 1, 110}}), progress) << locator.DebugString();
    }
    {
        auto progress = makeLocatorProgress({{0, 200, 1, 130}});
        ASSERT_TRUE(
            filter.filterOrRewriteProgress(/*payload*/ 150, {/*timestamp*/ 1, /*concurrentIdx*/ 100}, &progress));
    }
    {
        auto progress = makeLocatorProgress({{0, 200, 1, 150}});
        ASSERT_FALSE(
            filter.filterOrRewriteProgress(/*payload*/ 50, {/*timestamp*/ 1, /*concurrentIdx*/ 130}, &progress));
        ASSERT_EQ(makeLocatorProgress({{0, 200, 1, 150}}), progress);
    }
}
}} // namespace build_service::util
