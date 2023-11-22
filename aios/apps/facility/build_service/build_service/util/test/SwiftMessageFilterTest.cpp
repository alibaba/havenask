#include "build_service/util/SwiftMessageFilter.h"

#include <cstddef>
#include <cstdint>
#include <tuple>
#include <vector>

#include "build_service/test/unittest.h"
#include "indexlib/base/Progress.h"
#include "indexlib/framework/Locator.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;

namespace build_service { namespace util {

class SwiftMessageFilterTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp() {}
    void tearDown() {}

    indexlibv2::base::ProgressVector
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
            auto progress = locator.GetMultiProgress();

            ASSERT_FALSE(filter.needSkip(indexlibv2::framework::Locator::DocInfo(payload, ts, 0, 0)));
            filter.rewriteProgress(&progress);
            ASSERT_EQ(locator.GetMultiProgress(), progress);
        }
    }
    {
        SwiftMessageFilter filter;
        for (size_t i = 0; i < 30; i++) {
            uint16_t payload = 0;
            int64_t ts = i;
            indexlibv2::framework::Locator locator(0, {1, ts + 1});
            auto progress = locator.GetMultiProgress();
            ASSERT_FALSE(filter.needSkip(indexlibv2::framework::Locator::DocInfo(payload, ts, 1, 0)));
            filter.rewriteProgress(&progress);
            ASSERT_EQ(locator.GetMultiProgress(), progress);
        }
    }
}

TEST_F(SwiftMessageFilterTest, testSeek)
{
    SwiftMessageFilter filter;
    indexlibv2::base::MultiProgress seekProgress;
    seekProgress.push_back(makeLocatorProgress({{0, 100, -1, 0}, {101, 200, 110, 0}}));
    filter.setSeekProgress({seekProgress});

    //  [payload, docTs]
    //  [50, 99] (need rewrite)
    //  [150, 100] (need filter)
    //  [50, 130] (not rewrite and not filter)
    {
        indexlibv2::base::MultiProgress progress;
        progress.push_back(makeLocatorProgress({{0, 200, 100, 0}}));
        indexlibv2::framework::Locator::DocInfo docInfo(/*payload*/ 50, /*timestamp*/ 99, 0, 0);
        ASSERT_FALSE(filter.needSkip(docInfo));
        filter.rewriteProgress(&progress);
        ASSERT_EQ(makeLocatorProgress({{0, 100, 100, 0}, {101, 200, 110, 0}}), progress[0]);
    }
    {
        indexlibv2::base::MultiProgress progress;
        progress.push_back(makeLocatorProgress({{0, 200, 130, 0}}));
        indexlibv2::framework::Locator::DocInfo docInfo(/*payload*/ 150, /*timestamp*/ 100, 0, 0);
        ASSERT_TRUE(filter.needSkip(docInfo));
    }
    {
        indexlibv2::base::MultiProgress progress;
        progress.push_back(makeLocatorProgress({{0, 200, 150, 0}}));
        indexlibv2::framework::Locator::DocInfo docInfo(/*payload*/ 50, /*timestamp*/ 130, 0, 0);
        ASSERT_FALSE(filter.needSkip(docInfo));
        filter.rewriteProgress(&progress);
        ASSERT_EQ(makeLocatorProgress({{0, 200, 150, 0}}), progress[0]);
    }
}

TEST_F(SwiftMessageFilterTest, testSeekEmpty)
{
    SwiftMessageFilter filter;
    indexlibv2::framework::Locator locator;
    filter.setSeekProgress(locator.GetMultiProgress());

    indexlibv2::base::MultiProgress progress;
    progress.push_back(makeLocatorProgress({{0, 200, 100, 0}}));
    auto expectProgress = progress;
    indexlibv2::framework::Locator::DocInfo docInfo(/*payload*/ 50, /*timestamp*/ 99, 0, 0);
    ASSERT_FALSE(filter.needSkip(docInfo));
    filter.rewriteProgress(&progress);
    ASSERT_EQ(expectProgress, progress);
}

TEST_F(SwiftMessageFilterTest, testSeekWithSubIdx)
{
    SwiftMessageFilter filter;
    indexlibv2::base::MultiProgress seekProgress;
    seekProgress.push_back(makeLocatorProgress({{0, 100, 1, 0}, {101, 200, 1, 110}}));
    filter.setSeekProgress({seekProgress});

    //  [payload, docTs]
    //  [50, 99] (need rewrite)
    //  [150, 100] (need filter)
    //  [50, 130] (not rewrite and not filter)
    {
        indexlibv2::base::MultiProgress progress;
        progress.push_back(makeLocatorProgress({{0, 200, 1, 100}}));
        indexlibv2::framework::Locator::DocInfo docInfo(/*payload*/ 50, /*timestamp*/ 1, /*concurrentIdx*/ 99, 0);
        ASSERT_FALSE(filter.needSkip(docInfo));
        filter.rewriteProgress(&progress);
        ASSERT_EQ(makeLocatorProgress({{0, 100, 1, 100}, {101, 200, 1, 110}}), progress[0]);
    }
    {
        indexlibv2::base::MultiProgress progress;
        progress.push_back(makeLocatorProgress({{0, 200, 1, 130}}));
        indexlibv2::framework::Locator::DocInfo docInfo(/*payload*/ 150, /*timestamp*/ 1, /*concurrentIdx*/ 100, 0);
        ASSERT_TRUE(filter.needSkip(docInfo));
    }
    {
        indexlibv2::base::MultiProgress progress;
        progress.push_back(makeLocatorProgress({{0, 200, 1, 150}}));
        indexlibv2::framework::Locator::DocInfo docInfo(/*payload*/ 50, /*timestamp*/ 1, /*concurrentIdx*/ 130, 0);
        ASSERT_FALSE(filter.needSkip(docInfo));
        filter.rewriteProgress(&progress);
        ASSERT_EQ(makeLocatorProgress({{0, 200, 1, 150}}), progress[0]);
    }
}

TEST_F(SwiftMessageFilterTest, testSimpleWithMultiTopic)
{
    SwiftMessageFilter filter;
    indexlibv2::base::MultiProgress seekProgress;
    seekProgress.push_back(makeLocatorProgress({{0, 65535, 100, 0}}));
    seekProgress.push_back(makeLocatorProgress({{0, 65535, 50, 0}}));
    filter.setSeekProgress(seekProgress);

    {
        indexlibv2::base::MultiProgress progress;
        progress.push_back(makeLocatorProgress({{0, 65535, 51, 0}}));
        progress.push_back(makeLocatorProgress({{0, 65535, 50, 0}}));
        indexlibv2::framework::Locator::DocInfo docInfo(/*hash*/ 50, /*timestamp*/ 50, /*concurrentIdx*/ 0,
                                                        /*sourceIdx*/ 0);
        ASSERT_TRUE(filter.needSkip(docInfo));
    }
    {
        indexlibv2::base::MultiProgress progress;
        progress.push_back(makeLocatorProgress({{0, 65535, 51, 0}}));
        progress.push_back(makeLocatorProgress({{0, 65535, 51, 0}}));
        indexlibv2::framework::Locator::DocInfo docInfo(/*hash*/ 50, /*timestamp*/ 50, /*concurrentIdx*/ 0,
                                                        /*sourceIdx*/ 1);
        ASSERT_FALSE(filter.needSkip(docInfo));
        filter.rewriteProgress(&progress);
        indexlibv2::base::MultiProgress expectProgress;
        expectProgress.push_back(makeLocatorProgress({{0, 65535, 100, 0}}));
        expectProgress.push_back(makeLocatorProgress({{0, 65535, 51, 0}}));
        ASSERT_EQ(expectProgress, progress);
    }
    {
        indexlibv2::base::MultiProgress progress;
        progress.push_back(makeLocatorProgress({{0, 65535, 80, 0}}));
        progress.push_back(makeLocatorProgress({{0, 65535, 51, 0}}));
        indexlibv2::framework::Locator::DocInfo docInfo(/*hash*/ 50, /*timestamp*/ 79, /*concurrentIdx*/ 0,
                                                        /*sourceIdx*/ 0);
        ASSERT_TRUE(filter.needSkip(docInfo));
    }
    {
        indexlibv2::base::MultiProgress progress;
        progress.push_back(makeLocatorProgress({{0, 65535, 80, 0}}));
        progress.push_back(makeLocatorProgress({{0, 65535, 80, 0}}));
        indexlibv2::framework::Locator::DocInfo docInfo(/*hash*/ 50, /*timestamp*/ 79, /*concurrentIdx*/ 0,
                                                        /*sourceIdx*/ 1);
        ASSERT_FALSE(filter.needSkip(docInfo));
        filter.rewriteProgress(&progress);
        indexlibv2::base::MultiProgress expectProgress;
        expectProgress.push_back(makeLocatorProgress({{0, 65535, 100, 0}}));
        expectProgress.push_back(makeLocatorProgress({{0, 65535, 80, 0}}));
        ASSERT_EQ(expectProgress, progress);
    }
    {
        indexlibv2::base::MultiProgress progress;
        progress.push_back(makeLocatorProgress({{0, 65535, 120, 0}}));
        progress.push_back(makeLocatorProgress({{0, 65535, 80, 0}}));
        indexlibv2::framework::Locator::DocInfo docInfo(/*hash*/ 50, /*timestamp*/ 119, /*concurrentIdx*/ 0,
                                                        /*sourceIdx*/ 0);
        ASSERT_FALSE(filter.needSkip(docInfo));
        filter.rewriteProgress(&progress);
        indexlibv2::base::MultiProgress expectProgress;
        expectProgress.push_back(makeLocatorProgress({{0, 65535, 120, 0}}));
        expectProgress.push_back(makeLocatorProgress({{0, 65535, 80, 0}}));
        ASSERT_EQ(expectProgress, progress);
    }
}

}} // namespace build_service::util
