#include "build_service/util/LocatorUtil.h"

#include <cstdint>
#include <limits>
#include <map>
#include <optional>
#include <ostream>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "build_service/common/Locator.h"
#include "build_service/test/unittest.h"
#include "indexlib/base/Progress.h"
#include "indexlib/framework/Locator.h"
#include "swift/protocol/SwiftMessage.pb.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;

namespace build_service { namespace util {

class LocatorUtilTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp() {}
    void tearDown() {}
};

typedef std::map<uint8_t, std::vector<std::tuple<uint32_t, uint32_t, int64_t, uint32_t>>> ProgressParamMap;
swift::protocol::ReaderProgress makeProgress(const ProgressParamMap& params)
{
    std::string topicName = "topic_name";
    swift::protocol::ReaderProgress progress;
    for (auto topicParam : params) {
        auto topicProgress = progress.add_topicprogress();
        topicProgress->set_topicname(topicName);
        topicProgress->set_uint8filtermask(0);
        topicProgress->set_uint8maskresult(topicParam.first);
        for (const auto& [from, to, ts, offsetInRaw] : topicParam.second) {
            auto partProgress = topicProgress->add_partprogress();
            partProgress->set_from(from);
            partProgress->set_to(to);
            partProgress->set_timestamp(ts);
            partProgress->set_offsetinrawmsg(offsetInRaw);
        }
    }
    return progress;
}

std::vector<indexlibv2::base::Progress>
makeLocatorProgress(const std::vector<std::tuple<uint32_t, uint32_t, int64_t, uint32_t>>& param)
{
    std::vector<indexlibv2::base::Progress> progress;
    for (const auto& [from, to, ts, concurrentIdx] : param) {
        progress.push_back({from, to, {ts, concurrentIdx}});
    }
    return progress;
}

TEST_F(LocatorUtilTest, TestConvertSwiftMultiProgress)
{
    {
        ProgressParamMap progressParam;
        // 两个progress
        progressParam[1] = {{0, 500, 2, 0}, {501, 1000, 5, 0}};
        progressParam[2] = {{0, 100, 1, 0}, {101, 900, 3, 0}, {901, 1000, 6, 0}};
        auto swiftProgress = makeProgress(progressParam);
        auto [ret, retProgress] = LocatorUtil::convertSwiftMultiProgress(swiftProgress);
        ASSERT_TRUE(ret);

        indexlibv2::framework::Locator locator1, locator2;
        indexlibv2::base::MultiProgress multiProgress;
        multiProgress.emplace_back(makeLocatorProgress({{0, 500, 2, 0}, {501, 1000, 5, 0}}));
        multiProgress.emplace_back(makeLocatorProgress({{0, 100, 1, 0}, {101, 900, 3, 0}, {901, 1000, 6, 0}}));
        locator1.SetMultiProgress(multiProgress);
        locator2.SetMultiProgress(retProgress);
        ASSERT_EQ(locator1.GetMultiProgress(), locator2.GetMultiProgress());
    }
    {
        // 三个progress
        ProgressParamMap progressParam;
        progressParam[1] = {{0, 500, 2, 0}, {501, 1000, 5, 0}};
        progressParam[2] = {{0, 100, 1, 0}, {101, 900, 3, 0}, {901, 1000, 6, 0}};
        progressParam[3] = {{0, 600, 0, 0}, {601, 1000, 4, 0}};
        auto swiftProgress = makeProgress(progressParam);
        auto [ret, retProgress] = LocatorUtil::convertSwiftMultiProgress(swiftProgress);
        ASSERT_TRUE(ret);

        indexlibv2::framework::Locator locator1, locator2;
        indexlibv2::base::MultiProgress multiProgress;
        multiProgress.emplace_back(makeLocatorProgress({{0, 500, 2, 0}, {501, 1000, 5, 0}}));
        multiProgress.emplace_back(makeLocatorProgress({{0, 100, 1, 0}, {101, 900, 3, 0}, {901, 1000, 6, 0}}));
        multiProgress.emplace_back(makeLocatorProgress({{0, 600, 0, 0}, {601, 1000, 4, 0}}));
        locator1.SetMultiProgress(multiProgress);
        locator2.SetMultiProgress(retProgress);
        ASSERT_EQ(locator1.GetMultiProgress(), locator2.GetMultiProgress());
    }
}

TEST_F(LocatorUtilTest, TestConvertSwiftProgress)
{
    {
        ProgressParamMap progressParam;
        // 两个progress
        progressParam[1] = {{0, 500, 2, 0}, {501, 1000, 5, 0}};
        progressParam[2] = {{0, 100, 1, 0}, {101, 900, 3, 0}, {901, 1000, 6, 0}};
        auto swiftProgress = makeProgress(progressParam);
        auto locatorProgress =
            makeLocatorProgress({{0, 100, 1, 0}, {101, 500, 2, 0}, {501, 900, 3, 0}, {901, 1000, 5, 0}});
        auto [ret, retProgress] = LocatorUtil::convertSwiftProgress(swiftProgress, true);
        ASSERT_TRUE(ret);

        indexlibv2::framework::Locator locator1, locator2;
        locator1.SetMultiProgress({locatorProgress});
        locator2.SetMultiProgress({retProgress});
        ASSERT_EQ(locator1.DebugString(), locator2.DebugString());
    }
    {
        // 三个progress
        ProgressParamMap progressParam;
        progressParam[1] = {{0, 500, 2, 0}, {501, 1000, 5, 0}};
        progressParam[2] = {{0, 100, 1, 0}, {101, 900, 3, 0}, {901, 1000, 6, 0}};
        progressParam[3] = {{0, 600, 0, 0}, {601, 1000, 4, 0}};
        auto swiftProgress = makeProgress(progressParam);
        auto locatorProgress = makeLocatorProgress({{0, 600, 0, 0}, {601, 900, 3, 0}, {901, 1000, 4, 0}});
        auto [ret, retProgress] = LocatorUtil::convertSwiftProgress(swiftProgress, true);
        ASSERT_TRUE(ret);

        indexlibv2::framework::Locator locator1, locator2;
        locator1.SetMultiProgress({locatorProgress});
        locator2.SetMultiProgress({retProgress});
        ASSERT_EQ(locator1.DebugString(), locator2.DebugString());
    }
    {
        ProgressParamMap progressParam;
        //不连续
        progressParam[1] = {{0, 400, 2, 0}, {601, 1000, 5, 0}};
        progressParam[2] = {{0, 100, 1, 0}, {501, 1000, 6, 0}};
        auto swiftProgress = makeProgress(progressParam);
        auto locatorProgress =
            makeLocatorProgress({{0, 100, 1, 0}, {101, 400, 2, 0}, {501, 600, 6, 0}, {601, 1000, 5, 0}});
        auto [ret, retProgress] = LocatorUtil::convertSwiftProgress(swiftProgress, true);
        ASSERT_TRUE(ret);

        indexlibv2::framework::Locator locator1, locator2;
        locator1.SetMultiProgress({locatorProgress});
        locator2.SetMultiProgress({retProgress});
        ASSERT_EQ(locator1.DebugString(), locator2.DebugString());
    }
    {
        ProgressParamMap progressParam;
        // 两个progress
        progressParam[1] = {{0, 500, 1, 2}, {501, 1000, 1, 5}};
        progressParam[2] = {{0, 100, 1, 1}, {101, 900, 1, 3}, {901, 1000, 1, 6}};
        auto swiftProgress = makeProgress(progressParam);
        auto locatorProgress =
            makeLocatorProgress({{0, 100, 1, 1}, {101, 500, 1, 2}, {501, 900, 1, 3}, {901, 1000, 1, 5}});
        auto [ret, retProgress] = LocatorUtil::convertSwiftProgress(swiftProgress, true);
        ASSERT_TRUE(ret);

        indexlibv2::framework::Locator locator1, locator2;
        locator1.SetMultiProgress({locatorProgress});
        locator2.SetMultiProgress({retProgress});
        ASSERT_EQ(locator1.DebugString(), locator2.DebugString());
    }
}

TEST_F(LocatorUtilTest, TestComputeMinProgress)
{
    {
        //不连续
        auto locatorProgress1 = makeLocatorProgress({{0, 400, 2, 0}, {601, 1000, 5, 0}});
        auto locatorProgress2 = makeLocatorProgress({{0, 100, 1, 0}, {501, 1000, 6, 0}, {1001, 2000, 5, 0}});
        auto locatorProgress3 =
            makeLocatorProgress({{0, 100, 1, 0}, {101, 400, 2, 0}, {501, 600, 6, 0}, {601, 2000, 5, 0}});

        indexlibv2::framework::Locator locator1, locator2;

        auto ret = LocatorUtil::computeProgress(locatorProgress1, locatorProgress2, LocatorUtil::minOffset);
        locator1.SetMultiProgress({ret});
        locator2.SetMultiProgress({locatorProgress3});

        ASSERT_EQ(locator2.DebugString(), locator1.DebugString());
    }
}

TEST_F(LocatorUtilTest, TestComputeMinMultiProgress)
{
    {
        auto progressA1 = makeLocatorProgress({{0, 400, 2, 0}, {601, 1000, 5, 0}});
        auto progressB1 = makeLocatorProgress({{0, 100, 1, 0}, {501, 1000, 6, 0}, {1001, 2000, 5, 0}});
        auto expectProgress1 =
            makeLocatorProgress({{0, 100, 1, 0}, {101, 400, 2, 0}, {501, 600, 6, 0}, {601, 2000, 5, 0}});

        auto progressA2 = makeLocatorProgress({{0, 300, 2, 0}, {601, 1000, 6, 0}});
        auto progressB2 = makeLocatorProgress({{0, 300, 1, 0}, {301, 600, 6, 0}, {601, 2000, 5, 0}});
        auto expectProgress2 = makeLocatorProgress({{0, 300, 1, 0}, {301, 600, 6, 0}, {601, 2000, 5, 0}});

        auto ret = LocatorUtil::computeMultiProgress({progressA1, progressA2}, {progressB1, progressB2},
                                                     LocatorUtil::minOffset);
        ASSERT_TRUE(ret.has_value());
        indexlibv2::framework::Locator locator, expectLocator;
        locator.SetMultiProgress(ret.value());
        expectLocator.SetMultiProgress({expectProgress1, expectProgress2});
        ASSERT_EQ(expectLocator.DebugString(), locator.DebugString());
    }
}

TEST_F(LocatorUtilTest, TestEncodeAndDecodeOffset)
{
    auto checkFunc = [](int8_t streamIdx, int64_t timestamp) {
        int64_t offset = 0;
        ASSERT_TRUE(LocatorUtil::encodeOffset(streamIdx, timestamp, &offset))
            << "encode " << streamIdx << " : " << timestamp << " failed" << endl;
        int8_t decodedStreamIdx = streamIdx + 1;
        int64_t decodedTimestamp = timestamp + 1;
        LocatorUtil::decodeOffset(offset, &decodedStreamIdx, &decodedTimestamp);
        ASSERT_EQ(streamIdx, decodedStreamIdx)
            << "stream idx differ " << streamIdx << " input: " << streamIdx << " " << timestamp << endl;
        ASSERT_EQ(timestamp, decodedTimestamp)
            << "timestamp  differ " << decodedTimestamp << " input: " << (int64_t)streamIdx << " " << timestamp << endl;
    };

    checkFunc(0, 1000);
    checkFunc(1, 1000);
    checkFunc(1, 0);
    checkFunc(-1, -1);
    checkFunc(0, 0);
    // at least it can work until 2500-1-1 0:0:0
    checkFunc(0, 16725196800000000L);
    checkFunc(0, LocatorUtil::MAX_TIMESTAMP - 1);

    // special case for invalid locator, unexpected to be called but we protect it
    int8_t streamIdx = 0;
    int64_t timestamp = 0;
    LocatorUtil::decodeOffset(-1, &streamIdx, &timestamp);
    ASSERT_EQ((int8_t)-1, streamIdx);
    ASSERT_EQ((int64_t)-1, timestamp);

    int64_t offset = 0;
    ASSERT_TRUE(LocatorUtil::encodeOffset(streamIdx, timestamp, &offset));
    ASSERT_EQ(-1, offset);
}

TEST_F(LocatorUtilTest, TestEncodeDecodeInvalidLocator)
{
    auto checkInvalidEncode = [](int8_t streamIdx, int64_t timestamp) {
        int64_t offset = 0;
        ASSERT_FALSE(LocatorUtil::encodeOffset(streamIdx, timestamp, &offset))
            << " unexpected sucessful encode :" << streamIdx << " " << timestamp << endl;
    };
    checkInvalidEncode(0, -1);
    checkInvalidEncode(0, -2);
    checkInvalidEncode(-1, 0);
    checkInvalidEncode(-2, 0);
    checkInvalidEncode(-1, 100);
    checkInvalidEncode(-2, 100);
    checkInvalidEncode(1, 1L << 60);
    checkInvalidEncode(1, LocatorUtil::MAX_TIMESTAMP);
}

TEST_F(LocatorUtilTest, TestDecodedOffsetValid)
{
    // if streamIdx and timestamp is valid, the decoded offset is supposed to be equal of greater than 0
    auto checkOffsetValid = [](int8_t streamIdx, int64_t timestamp) {
        int64_t offset = 0;
        ASSERT_TRUE(LocatorUtil::encodeOffset(streamIdx, timestamp, &offset))
            << " encode failed :" << streamIdx << " " << timestamp << endl;
        ASSERT_GE(offset, 0);
    };
    for (int32_t i = 0; i < (int32_t)std::numeric_limits<int8_t>::max(); ++i) {
        checkOffsetValid((int8_t)i, 0);
        checkOffsetValid((int8_t)i, 100);
    }
}

TEST_F(LocatorUtilTest, TestGetSwiftWatermark)
{
    {
        common::Locator locator;
        std::vector<indexlibv2::base::Progress> progress;
        progress.push_back({0, 100, {100, 0}});
        progress.push_back({0, 100, {200, 0}});
        locator.SetMultiProgress({progress});
        ASSERT_EQ(100, LocatorUtil::getSwiftWatermark(locator));
    }
    {
        common::Locator locator;
        std::vector<indexlibv2::base::Progress> progress;
        int64_t offset = 0;
        ASSERT_TRUE(LocatorUtil::encodeOffset(1, 100, &offset));
        progress.push_back({0, 100, {offset, 0}});
        ASSERT_TRUE(LocatorUtil::encodeOffset(1, 200, &offset));
        progress.push_back({0, 100, {offset, 0}});
        locator.SetMultiProgress({progress});
        ASSERT_EQ(100, LocatorUtil::getSwiftWatermark(locator));
    }
}
}} // namespace build_service::util
