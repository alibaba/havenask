#include "indexlib/framework/Locator.h"

//#include "indexlib/document/index_locator.h"
#include "autil/StringUtil.h"
#include "unittest/unittest.h"

namespace indexlibv2::framework {

class LocatorTest : public TESTBASE
{
    void SetProgress(Locator& locator, std::vector<base::Progress> progress) { locator.SetMultiProgress({progress}); }
    std::vector<base::Progress> GetProgress(std::vector<std::tuple<uint32_t, uint32_t, int64_t>> params)
    {
        std::vector<base::Progress> progress;
        for (auto [from, to, ts] : params) {
            base::Progress::Offset offset = {ts, 0};
            progress.emplace_back(from, to, offset);
        }
        return progress;
    }
    void SetProgress(Locator& locator, std::vector<std::tuple<uint32_t, uint32_t, int64_t>> params)
    {
        locator.SetMultiProgress({GetProgress(params)});
    }
};

TEST_F(LocatorTest, ValidOrNot)
{
    Locator locator1(0, -1);
    ASSERT_FALSE(locator1.IsValid());

    Locator locator2(123, -1);
    ASSERT_FALSE(locator2.IsValid());

    Locator locator3(0, 0);
    ASSERT_TRUE(locator3.IsValid());

    Locator locator4(123, 0);
    ASSERT_TRUE(locator4.IsValid());

    Locator locator5(123, 456);
    ASSERT_TRUE(locator5.IsValid());

    using namespace indexlibv2::base;
    Locator otherLocator;
    otherLocator.SetMultiProgress({{Progress(0, 32767, {-1, 0}), Progress(32768, 65535, {-1, 0})}});
    Locator locator6;
    ASSERT_FALSE(locator6.IsValid());
    ASSERT_FALSE(otherLocator.IsValid());

    locator6.Update(otherLocator);
    ASSERT_FALSE(locator6.IsValid()) << locator6.DebugString();

    SetProgress(otherLocator, {Progress(0, 32767, {-1, 0}), Progress(32768, 65535, {100, 0})});
    locator6.Update(otherLocator);
    ASSERT_TRUE(locator6.IsValid()) << locator6.DebugString();
    ASSERT_TRUE(otherLocator.IsValid());

    SetProgress(otherLocator, {Progress(0, 16383, {-1, 0}), Progress(16384, 32767, {100, 0}),
                               Progress(32768, 49151, {-1, 0}), Progress(49152, 65535, {1000, 0})});
    locator6.Update(otherLocator);
    ASSERT_TRUE(locator6.IsValid());
    ASSERT_TRUE(otherLocator.IsValid());
}

TEST_F(LocatorTest, GetAndSet)
{
    Locator locator(1, 2);
    ASSERT_EQ(1, locator.GetSrc());
    ASSERT_EQ(2, locator.GetOffset().first);

    locator.SetSrc(3);
    locator.SetOffset({4, 0});
    ASSERT_EQ(3, locator.GetSrc());
    ASSERT_EQ(4, locator.GetOffset().first);
}

TEST_F(LocatorTest, SerializeAndDeserialize)
{
    std::string serialized;

    uint64_t src = 123;
    base::Progress::Offset offset = {456, 0};
    base::Progress singleProgress = base::Progress(12, 34, offset);
    {
        Locator locator(src, offset);
        std::vector<base::Progress> progress;
        progress.emplace_back(singleProgress);
        SetProgress(locator, progress);
        ASSERT_EQ(123, locator.GetSrc());
        ASSERT_EQ(offset, locator.GetOffset());
        ASSERT_EQ(40, locator.Size());
        progress = locator.GetMultiProgress()[0];
        ASSERT_EQ(progress.size(), 1);
        ASSERT_EQ(progress[0].from, 12);
        ASSERT_EQ(progress[0].to, 34);
        ASSERT_EQ(progress[0].offset, offset);
        serialized = locator.Serialize();
    }

    {
        Locator locator;
        ASSERT_EQ(40, serialized.size());
        ASSERT_TRUE(locator.Deserialize(serialized));
        ASSERT_EQ(123, locator.GetSrc());
        ASSERT_EQ(offset, locator.GetOffset());
        ASSERT_EQ(40, locator.Size());
        std::vector<base::Progress> progress = locator.GetMultiProgress()[0];
        ASSERT_EQ(progress.size(), 1);
        ASSERT_EQ(progress[0].from, 12);
        ASSERT_EQ(progress[0].to, 34);
        ASSERT_EQ(progress[0].offset, offset);
        const std::string expectedStr("123:456:[{12,34,456,0}]");
        ASSERT_EQ(expectedStr, locator.DebugString());
    }
}

TEST_F(LocatorTest, SerializeAndDeserializeWithUserData)
{
    std::string serializedStr;
    {
        Locator locator(1, 1024);
        std::vector<base::Progress> progress {{0, 65535, {1024, 0}}};
        SetProgress(locator, std::move(progress));
        locator.SetUserData("userdata1");
        serializedStr = locator.Serialize();
    }
    {
        Locator locator;
        ASSERT_TRUE(locator.Deserialize(serializedStr));
        ASSERT_EQ(1, locator.GetSrc());
        ASSERT_EQ(1024, locator.GetOffset().first);
        const auto& progress = locator.GetMultiProgress()[0];
        ASSERT_EQ(1, progress.size());
        ASSERT_EQ(base::Progress(0, 65535, {1024, 0}), progress[0]);
        ASSERT_EQ("userdata1", locator.GetUserData());
        ASSERT_EQ("1:1024:userdata1:[{0,65535,1024,0}]", locator.DebugString());
    }
}

TEST_F(LocatorTest, TestSimple)
{
    Locator locator1(/*src=*/123, /*offset=*/456);
    EXPECT_EQ(123, locator1.GetSrc());
    EXPECT_EQ(456, locator1.GetOffset().first);
    EXPECT_EQ(40, locator1.Size());
    EXPECT_EQ(1, locator1.GetMultiProgress()[0].size());
    EXPECT_EQ(0, locator1.GetMultiProgress()[0][0].from);
    EXPECT_EQ(65535, locator1.GetMultiProgress()[0][0].to);
    EXPECT_EQ(456, locator1.GetMultiProgress()[0][0].offset.first);

    Locator locator2;
    EXPECT_EQ(0, locator2.GetMultiProgress()[0].size());

    Locator locator3;
    locator3.SetOffset({100, 0});
    EXPECT_EQ(1, locator3.GetMultiProgress()[0].size());
    EXPECT_EQ(0, locator3.GetMultiProgress()[0][0].from);
    EXPECT_EQ(65535, locator3.GetMultiProgress()[0][0].to);
    EXPECT_EQ(100, locator3.GetMultiProgress()[0][0].offset.first);
}

TEST_F(LocatorTest, TestLegacyLocator)
{
    // test set & get
    {
        Locator locator(/*src*/ 0, /*offset*/ 10);
        ASSERT_FALSE(locator.IsLegacyLocator());
        locator.SetLegacyLocator();
        ASSERT_TRUE(locator.IsLegacyLocator());
    }
    // test IsSameSrc
    {
        Locator locator1(0, 0);
        Locator locator2(1, 0);
        ASSERT_FALSE(locator1.IsSameSrc(locator2, /*ignoreLegacyDiffSrc*/ false));
        ASSERT_FALSE(locator2.IsSameSrc(locator1, /*ignoreLegacyDiffSrc*/ false));
        // no legacy locator
        ASSERT_FALSE(locator1.IsSameSrc(locator2, /*ignoreLegacyDiffSrc*/ true));
        ASSERT_FALSE(locator2.IsSameSrc(locator1, /*ignoreLegacyDiffSrc*/ true));
        locator1.SetLegacyLocator();
        ASSERT_TRUE(locator1.IsSameSrc(locator2, /*ignoreLegacyDiffSrc*/ true));
        ASSERT_TRUE(locator2.IsSameSrc(locator1, /*ignoreLegacyDiffSrc*/ true));
    }
    // test IsFasterThan
    {
        Locator locator1(0, 0);
        SetProgress(locator1, {{0, 100, 20}});
        Locator locator2(1, 0);
        SetProgress(locator2, {{0, 100, 10}});

        ASSERT_EQ(Locator::LocatorCompareResult::LCR_INVALID, locator1.IsFasterThan(locator2, false));
        locator2.SetLegacyLocator();
        ASSERT_EQ(Locator::LocatorCompareResult::LCR_INVALID, locator1.IsFasterThan(locator2, false));
        ASSERT_EQ(Locator::LocatorCompareResult::LCR_FULLY_FASTER, locator1.IsFasterThan(locator2, true));
        ASSERT_EQ(Locator::LocatorCompareResult::LCR_SLOWER, locator2.IsFasterThan(locator1, true));
    }
    {
        Locator locator1(111, 0);
        SetProgress(locator1, {{0, 65535, 111}});
        locator1.SetLegacyLocator();

        Locator locator2(222, 0);
        SetProgress(locator2, {{49152, 65535, 112}});
        ASSERT_EQ(Locator::LocatorCompareResult::LCR_SLOWER, locator1.IsFasterThan(locator2, true));
        ASSERT_EQ(Locator::LocatorCompareResult::LCR_FULLY_FASTER, locator2.IsFasterThan(locator1, true));
    }
    // test equal
    {
        Locator locator1(/*src=*/0, /*offset=*/0);
        locator1.SetMultiProgress({{{/*from=*/0, /*to=*/100, /*offset=*/ {10, 0}}}});
        Locator locator2(/*src=*/0, /*offset=*/0);
        locator2.SetMultiProgress({{{/*from=*/0, /*to=*/100, /*offset=*/ {10, 0}}}});

        ASSERT_EQ(Locator::LocatorCompareResult::LCR_FULLY_FASTER,
                  locator1.IsFasterThan(locator2, /*ignoreLegacyDiffSrc=*/true));
        ASSERT_EQ(Locator::LocatorCompareResult::LCR_FULLY_FASTER,
                  locator2.IsFasterThan(locator1, /*ignoreLegacyDiffSrc=*/true));
        ASSERT_EQ(Locator::LocatorCompareResult::LCR_SLOWER, locator1.IsFasterThan(Locator::DocInfo(0, 10, 0, 0)));
    }
}
// TEST_F(LocatorTest, TestCompatable)
// {
//     indexlib::document::IndexLocator oldLocator(123, 456, "");
//     Locator newLocator;
//     ASSERT_TRUE(newLocator.Deserialize(oldLocator.toString()));
//     ASSERT_EQ(123, newLocator.GetSrc());
//     ASSERT_EQ(456, newLocator.GetOffset());
//     ASSERT_EQ(1, newLocator.GetProgress().size());
//     ASSERT_EQ(base::Progress(0, 65535, 456), newLocator.GetProgress()[0]);
// }

TEST_F(LocatorTest, TestCompareProgress)
{
    Locator locator;
    SetProgress(locator, {{100, 200, 50}, {201, 300, 60}});
    ASSERT_EQ(50, locator.GetOffset().first);

    {
        ASSERT_EQ(Locator::LocatorCompareResult::LCR_SLOWER, locator.IsFasterThan(Locator::DocInfo(0, 100, 0, 0)));
        ASSERT_EQ(Locator::LocatorCompareResult::LCR_SLOWER, locator.IsFasterThan(Locator::DocInfo(100, 100, 0, 0)));
        ASSERT_EQ(Locator::LocatorCompareResult::LCR_FULLY_FASTER,
                  locator.IsFasterThan(Locator::DocInfo(100, 30, 0, 0)));
        ASSERT_EQ(Locator::LocatorCompareResult::LCR_SLOWER, locator.IsFasterThan(Locator::DocInfo(500, 30, 0, 0)));
    }
    {
        Locator locator2;
        SetProgress(locator2, {{150, 200, 50}, {201, 300, 60}});
        ASSERT_EQ(Locator::LocatorCompareResult::LCR_FULLY_FASTER, locator.IsFasterThan(locator2, false));
    }

    {
        Locator locator2 = locator;
        ASSERT_EQ(Locator::LocatorCompareResult::LCR_FULLY_FASTER, locator.IsFasterThan(locator2, false));
        ASSERT_EQ(Locator::LocatorCompareResult::LCR_FULLY_FASTER, locator2.IsFasterThan(locator, false));
    }

    {
        Locator locator2;
        SetProgress(locator2, {{100, 200, 49}, {201, 300, 59}});
        ASSERT_EQ(Locator::LocatorCompareResult::LCR_FULLY_FASTER, locator.IsFasterThan(locator2, false));
        ASSERT_EQ(Locator::LocatorCompareResult::LCR_SLOWER, locator2.IsFasterThan(locator, false));

        Locator locator3;
        SetProgress(locator3, {{100, 200, 50}, {201, 300, 59}});
        ASSERT_EQ(Locator::LocatorCompareResult::LCR_FULLY_FASTER, locator.IsFasterThan(locator3, false));
        ASSERT_EQ(Locator::LocatorCompareResult::LCR_SLOWER, locator3.IsFasterThan(locator, false));

        Locator locator4;
        SetProgress(locator4, {{100, 150, 49}, {151, 200, 49}, {201, 300, 59}});
        ASSERT_EQ(Locator::LocatorCompareResult::LCR_FULLY_FASTER, locator.IsFasterThan(locator4, false));
        ASSERT_EQ(Locator::LocatorCompareResult::LCR_SLOWER, locator4.IsFasterThan(locator, false));

        Locator locator5;
        SetProgress(locator5, {{100, 300, 30}});
        ASSERT_EQ(Locator::LocatorCompareResult::LCR_FULLY_FASTER, locator.IsFasterThan(locator5, false));
        ASSERT_EQ(Locator::LocatorCompareResult::LCR_SLOWER, locator5.IsFasterThan(locator, false));
    }

    {
        Locator locator2;
        Locator locator3;
        SetProgress(locator2, {{0, 65535, 0}});
        SetProgress(locator3, {{0, 120, 11}, {121, 200, 11}});
        ASSERT_EQ(Locator::LocatorCompareResult::LCR_FULLY_FASTER, locator3.IsFasterThan(locator2, false));
    }

    {
        Locator locator2;
        SetProgress(locator2, {{100, 200, 59}, {201, 300, 59}});

        ASSERT_EQ(Locator::LocatorCompareResult::LCR_PARTIAL_FASTER, locator.IsFasterThan(locator2, false));
        ASSERT_EQ(Locator::LocatorCompareResult::LCR_PARTIAL_FASTER, locator2.IsFasterThan(locator, false));
    }

    {
        Locator locator2;
        SetProgress(locator2, {{100, 150, 59}, {141, 200, 59}, {201, 300, 59}});

        ASSERT_EQ(Locator::LocatorCompareResult::LCR_PARTIAL_FASTER, locator.IsFasterThan(locator2, false));
        ASSERT_EQ(Locator::LocatorCompareResult::LCR_PARTIAL_FASTER, locator2.IsFasterThan(locator, false));
    }
    {
        // test INVALID
        Locator locator2;
        Locator locator3;
        ASSERT_EQ(Locator::LocatorCompareResult::LCR_FULLY_FASTER, locator.IsFasterThan(locator2, false));
        // both INVALID
        ASSERT_EQ(Locator::LocatorCompareResult::LCR_FULLY_FASTER, locator3.IsFasterThan(locator2, false));
        // slower
        ASSERT_EQ(Locator::LocatorCompareResult::LCR_SLOWER, locator2.IsFasterThan(locator, false));
    }

    {
        Locator locator1, locator2;
        SetProgress(locator1, {{30, 50, 10}});
        SetProgress(locator2, {{0, 40, 12}});
        ASSERT_EQ(Locator::LocatorCompareResult::LCR_PARTIAL_FASTER, locator1.IsFasterThan(locator2, false));

        SetProgress(locator1, {{30, 50, 10}});
        SetProgress(locator2, {{30, 40, 12}});
        ASSERT_EQ(Locator::LocatorCompareResult::LCR_PARTIAL_FASTER, locator1.IsFasterThan(locator2, false));

        SetProgress(locator1, {{30, 50, 10}});
        SetProgress(locator2, {{30, 50, 10}});
        ASSERT_EQ(Locator::LocatorCompareResult::LCR_FULLY_FASTER, locator1.IsFasterThan(locator2, false));

        SetProgress(locator1, {{30, 50, 10}});
        SetProgress(locator2, {{30, 50, 9}});
        ASSERT_EQ(Locator::LocatorCompareResult::LCR_FULLY_FASTER, locator1.IsFasterThan(locator2, false));

        SetProgress(locator1, {{30, 50, 10}});
        SetProgress(locator2, {{30, 50, 11}});
        ASSERT_EQ(Locator::LocatorCompareResult::LCR_SLOWER, locator1.IsFasterThan(locator2, false));

        SetProgress(locator1, {{30, 50, 10}});
        SetProgress(locator2, {{30, 40, 8}});
        ASSERT_EQ(Locator::LocatorCompareResult::LCR_FULLY_FASTER, locator1.IsFasterThan(locator2, false));

        SetProgress(locator1, {{30, 50, 10}});
        SetProgress(locator2, {{0, 50, 12}});
        ASSERT_EQ(Locator::LocatorCompareResult::LCR_SLOWER, locator1.IsFasterThan(locator2, false));

        SetProgress(locator1, {{30, 50, 10}});
        SetProgress(locator2, {{0, 50, 8}});
        ASSERT_EQ(Locator::LocatorCompareResult::LCR_PARTIAL_FASTER, locator1.IsFasterThan(locator2, false));

        SetProgress(locator1, {{30, 50, 10}, {51, 60, 12}});
        SetProgress(locator2, {{40, 55, 11}});
        ASSERT_EQ(Locator::LocatorCompareResult::LCR_PARTIAL_FASTER, locator1.IsFasterThan(locator2, false));

        SetProgress(locator1, {{30, 50, 10}, {51, 60, 12}});
        SetProgress(locator2, {{40, 55, 10}});
        ASSERT_EQ(Locator::LocatorCompareResult::LCR_FULLY_FASTER, locator1.IsFasterThan(locator2, false));

        SetProgress(locator1, {{30, 50, 10}, {51, 60, 12}});
        SetProgress(locator2, {{30, 60, 20}});
        ASSERT_EQ(Locator::LocatorCompareResult::LCR_SLOWER, locator1.IsFasterThan(locator2, false));
    }
}

TEST_F(LocatorTest, testUpdateProgress)
{
    auto GetProgressDebugString = [](const std::vector<base::Progress>& progressVec) {
        std::string debugString;
        for (const auto& progress : progressVec) {
            debugString += "{" + std::to_string(progress.from) + "," + std::to_string(progress.to) + "," +
                           std::to_string(progress.offset.first) + "," + std::to_string(progress.offset.second) + "}";
        };
        return debugString;
    };
    auto CheckProgress = [&](const std::vector<base::Progress>& expected, const std::vector<base::Progress>& progress) {
        if (expected != progress) {
            std::cerr << "[DEBUG] expected [" << GetProgressDebugString(expected) << "], actual ["
                      << GetProgressDebugString(progress) << "]" << std::endl;
            return false;
        }
        return true;
    };

    Locator locator1, locator2;
    std::vector<base::Progress> expectedProgress;
    // invalid update
    SetProgress(locator1, {{0, 50, 10}, {51, 100, 20}});
    SetProgress(locator2, {{0, 60, 12}});
    ASSERT_FALSE(locator1.Update(locator2));

    SetProgress(locator1, {{0, 50, 10}});
    SetProgress(locator2, {{0, 50, 9}});
    ASSERT_FALSE(locator1.Update(locator2));

    SetProgress(locator1, {{0, 50, 10}, {51, 100, 20}});
    SetProgress(locator2, {{100, 101, 19}});
    ASSERT_FALSE(locator1.Update(locator2));

    // same range
    SetProgress(locator1, {{0, 50, 10}});
    locator1._userData = "111";
    SetProgress(locator2, {{0, 50, 11}});
    locator2._userData = "222";
    expectedProgress = GetProgress({{0, 50, 11}});
    ASSERT_TRUE(locator1.Update(locator2));
    ASSERT_TRUE(CheckProgress(expectedProgress, locator1.GetMultiProgress()[0]));
    ASSERT_EQ(11, locator1.GetOffset().first);
    ASSERT_EQ("222", locator1.GetUserData());

    // test range split
    SetProgress(locator1, {{0, 50, 10}});
    SetProgress(locator2, {{50, 100, 11}});
    expectedProgress = GetProgress({{0, 49, 10}, {50, 100, 11}});
    ASSERT_TRUE(locator1.Update(locator2));
    ASSERT_TRUE(CheckProgress(expectedProgress, locator1.GetMultiProgress()[0]));
    ASSERT_EQ(10, locator1.GetOffset().first);

    SetProgress(locator1, {{0, 50, 10}});
    SetProgress(locator2, {{20, 30, 11}});
    expectedProgress = GetProgress({{0, 19, 10}, {20, 30, 11}, {31, 50, 10}});
    ASSERT_TRUE(locator1.Update(locator2));
    ASSERT_TRUE(CheckProgress(expectedProgress, locator1.GetMultiProgress()[0]));
    ASSERT_EQ(10, locator1.GetOffset().first);

    SetProgress(locator1, {{0, 50, 10}, {100, 150, 11}});
    SetProgress(locator2, {{0, 200, 20}});
    expectedProgress = GetProgress({{0, 200, 20}});
    ASSERT_TRUE(locator1.Update(locator2));
    ASSERT_TRUE(CheckProgress(expectedProgress, locator1.GetMultiProgress()[0]));
    ASSERT_EQ(20, locator1.GetOffset().first);

    SetProgress(locator1, {{0, 50, 10}, {100, 150, 11}});
    SetProgress(locator2, {{30, 130, 20}});
    expectedProgress = GetProgress({{0, 29, 10}, {30, 130, 20}, {131, 150, 11}});
    ASSERT_TRUE(locator1.Update(locator2));
    ASSERT_TRUE(CheckProgress(expectedProgress, locator1.GetMultiProgress()[0]));
    ASSERT_EQ(10, locator1.GetOffset().first);

    SetProgress(locator1, {{0, 50, 10}, {100, 150, 11}});
    SetProgress(locator2, {{0, 50, 11}, {110, 130, 20}});
    expectedProgress = GetProgress({{0, 50, 11}, {100, 109, 11}, {110, 130, 20}, {131, 150, 11}});
    ASSERT_TRUE(locator1.Update(locator2));
    ASSERT_TRUE(CheckProgress(expectedProgress, locator1.GetMultiProgress()[0]));
    ASSERT_EQ(11, locator1.GetOffset().first);

    // test range merge
    SetProgress(locator1, {{0, 50, 10}, {100, 150, 11}});
    SetProgress(locator2, {{30, 120, 11}});
    expectedProgress = GetProgress({{0, 29, 10}, {30, 150, 11}});
    ASSERT_TRUE(locator1.Update(locator2));
    ASSERT_TRUE(CheckProgress(expectedProgress, locator1.GetMultiProgress()[0]));
    ASSERT_EQ(10, locator1.GetOffset().first);

    SetProgress(locator1, {{0, 50, 10}, {100, 150, 11}});
    SetProgress(locator2, {{51, 99, 11}});
    expectedProgress = GetProgress({{0, 50, 10}, {51, 150, 11}});
    ASSERT_TRUE(locator1.Update(locator2));
    ASSERT_TRUE(CheckProgress(expectedProgress, locator1.GetMultiProgress()[0]));
    ASSERT_EQ(10, locator1.GetOffset().first);

    SetProgress(locator1, {{0, 50, 10}, {100, 150, 10}});
    SetProgress(locator2, {{51, 99, 10}, {120, 200, 10}});
    expectedProgress = GetProgress({{0, 200, 10}});
    ASSERT_TRUE(locator1.Update(locator2));
    ASSERT_TRUE(CheckProgress(expectedProgress, locator1.GetMultiProgress()[0]));
    ASSERT_EQ(10, locator1.GetOffset().first);

    // test not merge empty range
    SetProgress(locator1, {{0, 65535, 0}});
    SetProgress(locator2, {{0, 120, 11}, {121, 200, 11}});
    expectedProgress = GetProgress({{0, 200, 11}, {201, 65535, 0}});
    ASSERT_TRUE(locator1.Update(locator2));
    ASSERT_TRUE(CheckProgress(expectedProgress, locator1.GetMultiProgress()[0]));
    ASSERT_EQ(0, locator1.GetOffset().first);

    SetProgress(locator1, {{0, 65535, 0}});
    SetProgress(locator2, {{0, 120, 11}, {121, 130, 0}, {150, 200, 12}});
    expectedProgress = GetProgress({{0, 120, 11}, {121, 149, 0}, {150, 200, 12}, {201, 65535, 0}});
    ASSERT_TRUE(locator1.Update(locator2));
    ASSERT_TRUE(CheckProgress(expectedProgress, locator1.GetMultiProgress()[0]));
    ASSERT_EQ(0, locator1.GetOffset().first);

    // from version synchronizerTest
    Locator baseLocator, updateLocator;
    SetProgress(baseLocator, {{0, 32767, 10}, {32768, 65535, 12}});
    SetProgress(updateLocator, {{0, 32767, 15}});
    expectedProgress = GetProgress({{0, 32767, 15}, {32768, 65535, 12}});
    ASSERT_TRUE(baseLocator.Update(updateLocator));
    ASSERT_TRUE(CheckProgress(expectedProgress, baseLocator.GetMultiProgress()[0]));
    ASSERT_EQ(12, baseLocator.GetOffset().first);

    SetProgress(baseLocator, {{0, 32767, 10}, {32768, 65535, 12}});
    SetProgress(updateLocator, {{32768, 65535, 15}});
    expectedProgress = GetProgress({{0, 32767, 10}, {32768, 65535, 15}});
    ASSERT_TRUE(baseLocator.Update(updateLocator));
    ASSERT_TRUE(CheckProgress(expectedProgress, baseLocator.GetMultiProgress()[0]));
    ASSERT_EQ(10, baseLocator.GetOffset().first);

    SetProgress(baseLocator, {{0, 32767, 10}, {32768, 65535, 12}});
    SetProgress(updateLocator, {{0, 32767, 15}, {32768, 65535, 15}});
    expectedProgress = GetProgress({{0, 65535, 15}});
    ASSERT_TRUE(baseLocator.Update(updateLocator));
    ASSERT_TRUE(CheckProgress(expectedProgress, baseLocator.GetMultiProgress()[0]));
    ASSERT_EQ(15, baseLocator.GetOffset().first);

    SetProgress(baseLocator, {{0, 32767, 10}, {32768, 65535, 12}});
    SetProgress(updateLocator, {{21845, 43960, 14}});
    expectedProgress = GetProgress({{0, 21844, 10}, {21845, 43960, 14}, {43961, 65535, 12}});
    ASSERT_TRUE(baseLocator.Update(updateLocator));
    ASSERT_TRUE(CheckProgress(expectedProgress, baseLocator.GetMultiProgress()[0]));
    ASSERT_EQ(10, baseLocator.GetOffset().first);

    SetProgress(baseLocator, {{0, 21844, 10}, {21845, 43960, 11}, {43961, 65535, 12}});
    SetProgress(updateLocator, {{32768, 65535, 13}});
    expectedProgress = GetProgress({{0, 21844, 10}, {21845, 32767, 11}, {32768, 65535, 13}});
    ASSERT_TRUE(baseLocator.Update(updateLocator));
    ASSERT_TRUE(CheckProgress(expectedProgress, baseLocator.GetMultiProgress()[0]));
    ASSERT_EQ(10, baseLocator.GetOffset().first);

    SetProgress(baseLocator, {{0, 21844, 10}, {21845, 43960, 11}, {43961, 65535, 12}});
    SetProgress(updateLocator, {{0, 32767, 14}});
    expectedProgress = GetProgress({{0, 32767, 14}, {32768, 43960, 11}, {43961, 65535, 12}});
    ASSERT_TRUE(baseLocator.Update(updateLocator));
    ASSERT_TRUE(CheckProgress(expectedProgress, baseLocator.GetMultiProgress()[0]));
    ASSERT_EQ(11, baseLocator.GetOffset().first);

    SetProgress(baseLocator, {{0, 10000, 10},
                              {10001, 20000, 11},
                              {20001, 30000, 12},
                              {30001, 40000, 13},
                              {40001, 50000, 14},
                              {50001, 65535, 15}});
    SetProgress(updateLocator, {{32768, 65535, 20}});
    expectedProgress =
        GetProgress({{0, 10000, 10}, {10001, 20000, 11}, {20001, 30000, 12}, {30001, 32767, 13}, {32768, 65535, 20}});
    ASSERT_TRUE(baseLocator.Update(updateLocator));
    ASSERT_TRUE(CheckProgress(expectedProgress, baseLocator.GetMultiProgress()[0]));
    ASSERT_EQ(10, baseLocator.GetOffset().first);

    SetProgress(baseLocator, {{0, 10000, 10},
                              {10001, 20000, 11},
                              {20001, 30000, 12},
                              {30001, 40000, 13},
                              {40001, 50000, 14},
                              {50001, 65535, 15}});
    SetProgress(updateLocator, {{0, 32767, 20}});
    expectedProgress = GetProgress({{0, 32767, 20}, {32768, 40000, 13}, {40001, 50000, 14}, {50001, 65535, 15}});
    ASSERT_TRUE(baseLocator.Update(updateLocator));
    ASSERT_TRUE(CheckProgress(expectedProgress, baseLocator.GetMultiProgress()[0]));
    ASSERT_EQ(13, baseLocator.GetOffset().first);

    SetProgress(baseLocator, {{0, 32767, 10}});
    SetProgress(updateLocator, {{0, 50, 15}, {65530, 65535, 17}});
    expectedProgress = GetProgress({{0, 50, 15}, {51, 32767, 10}, {65530, 65535, 17}});
    ASSERT_TRUE(baseLocator.Update(updateLocator));
    ASSERT_TRUE(CheckProgress(expectedProgress, baseLocator.GetMultiProgress()[0]));
    ASSERT_EQ(10, baseLocator.GetOffset().first);
}

TEST_F(LocatorTest, tesDeserializeLegacyUserData)
{
    std::string locatorString =
        "01000000000000009f9bf114edf605006b6d6f6e69746f725f6d65747269635f73756e666972655f686973746f72795f6662";
    auto formattedStr = autil::StringUtil::hexStrToStr(locatorString);
    Locator curLocator;
    ASSERT_TRUE(curLocator.Deserialize(formattedStr));
    ASSERT_EQ(1, curLocator.GetSrc());
    ASSERT_EQ(1678873002613663L, curLocator.GetOffset().first);
    ASSERT_EQ("kmonitor_metric_sunfire_history_fb", curLocator.GetUserData());
    ASSERT_TRUE(curLocator.IsLegacyLocator());
}

TEST_F(LocatorTest, testDeserialize)
{
    std::string locatorString = "c0749c5213ff0500c000239a13ff0500";
    auto formattedStr = autil::StringUtil::hexStrToStr(locatorString);
    Locator curLocator;
    ASSERT_TRUE(curLocator.Deserialize(formattedStr));
    ASSERT_EQ(1687833339000000L, curLocator.GetSrc());
    ASSERT_EQ(1687834539000000L, curLocator.GetOffset().first);
    ASSERT_TRUE(curLocator.IsLegacyLocator());
}

TEST_F(LocatorTest, tesDeserializeV0UserData)
{
    std::string locatorString =
        "0100000000000000e2ce065014ff05000100000000000000e2ce065014ff0500000000007f0000002b0000000000000062697a5f6f7264"
        "65725f76335f30325f77616c5f62697a5f6f726465725f73756d6d6172795f746f706963";
    auto formattedStr = autil::StringUtil::hexStrToStr(locatorString);
    Locator curLocator;
    ASSERT_TRUE(curLocator.Deserialize(formattedStr));
    ASSERT_EQ("biz_order_v3_02_wal_biz_order_summary_topic", curLocator.GetUserData());
    ASSERT_EQ(1, curLocator.GetSrc());
    ASSERT_EQ(1687837590605538L, curLocator.GetOffset().first);
    ASSERT_EQ(base::Progress(0, 127, {1687837590605538L, 0}), curLocator.GetMultiProgress()[0][0]);
}

TEST_F(LocatorTest, testSubOffset)
{
    Locator locator1, locator2, locator3, locator4;
    SetProgress(locator1, {{0, 65535, {1, 0}}});
    SetProgress(locator2, {{0, 65535, {1, 1}}});
    SetProgress(locator3, {{0, 65535, {1, 2}}});
    SetProgress(locator4, {{0, 32767, {1, 2}}, {32768, 65535, {1, 0}}});
    ASSERT_TRUE(locator1 != locator2);
    ASSERT_TRUE(locator2.IsFasterThan(locator1, false) == Locator::LocatorCompareResult::LCR_FULLY_FASTER);
    ASSERT_TRUE(locator2.IsFasterThan(locator2, false) == Locator::LocatorCompareResult::LCR_FULLY_FASTER);
    ASSERT_TRUE(locator2.IsFasterThan(locator3, false) == Locator::LocatorCompareResult::LCR_SLOWER);
    ASSERT_TRUE(locator2.IsFasterThan(locator4, false) == Locator::LocatorCompareResult::LCR_PARTIAL_FASTER);

    ASSERT_TRUE(locator2.IsFasterThan(Locator::DocInfo(/*hashId*/ 0, /*offset*/ 1, 0, 0)) ==
                Locator::LocatorCompareResult::LCR_FULLY_FASTER);
    ASSERT_TRUE(locator2.IsFasterThan(Locator::DocInfo(/*hashId*/ 0, /*offset*/ 1, 1, 0)) ==
                Locator::LocatorCompareResult::LCR_SLOWER);
}

TEST_F(LocatorTest, testDeserializeInvalidLocator)
{
    std::string locatorString = "0100000000000080b6d4290ee30006000200000000000000b6d4290ee300060000000000ff7f0000ffffff"
                                "ffffffffff00800000ffff0000";
    auto formattedStr = autil::StringUtil::hexStrToStr(locatorString);
    Locator curLocator;
    ASSERT_TRUE(curLocator.Deserialize(formattedStr)) << curLocator.DebugString();
    ASSERT_EQ(9223372036854775809UL, curLocator.GetSrc());
    ASSERT_EQ(1689825055462582L, curLocator.GetOffset().first);
}

TEST_F(LocatorTest, testUpdateLocatorWithInitialRange)
{
    Locator locator, locator1, locator2;
    locator1.SetMultiProgress({{{0, 32767, {123, 0}}}});
    locator.Update(locator1);
    locator2.SetMultiProgress({{{32768, 65535, {-1, 0}}}});
    locator.Update(locator2);
    ASSERT_EQ(-1, locator.GetOffset().first);
}

TEST_F(LocatorTest, testForMultiProgress)
{
    Locator locator, locator1, loactor2;
    locator.SetMultiProgress(
        {{{0, 32767, {50, 0}}, {32768, 65535, {100, 0}}}, {{0, 32767, {100, 0}}, {32768, 65535, {50, 0}}}});
    ASSERT_TRUE(locator.IsValid());
    ASSERT_EQ(50, locator.GetOffset().first);
    // test content
    base::MultiProgress multiProgress = locator.GetMultiProgress();
    ASSERT_EQ(2, multiProgress.size());
    ASSERT_EQ(2, multiProgress[0].size());
    ASSERT_EQ(base::Progress(0, 32767, {50, 0}), multiProgress[0][0]);
    ASSERT_EQ(base::Progress(32768, 65535, {100, 0}), multiProgress[0][1]);
    ASSERT_EQ(2, multiProgress[1].size());
    ASSERT_EQ(base::Progress(0, 32767, {100, 0}), multiProgress[1][0]);
    ASSERT_EQ(base::Progress(32768, 65535, {50, 0}), multiProgress[1][1]);
    // test serialize && deserialize
    auto serializedStr = locator.Serialize();
    locator1.Deserialize(serializedStr);
    ASSERT_EQ(locator, locator1) << locator1.DebugString() << "  " << locator.DebugString();
    // test debug string
    ASSERT_EQ("0:50:[{0,32767,50,0}{32768,65535,100,0};{0,32767,100,0}{32768,65535,50,0}]", locator.DebugString());
    // test shrank to range
    locator1.ShrinkToRange(10000, 60000);
    ASSERT_EQ("0:50:[{10000,32767,50,0}{32768,60000,100,0};{10000,32767,100,0}{32768,60000,50,0}]",
              locator1.DebugString());
}

TEST_F(LocatorTest, testIsFasterThanForMultiProgress)
{
    {
        Locator locator1, locator2;
        locator1.SetMultiProgress(
            {{{0, 32767, {50, 0}}, {32768, 65535, {100, 0}}}, {{0, 32767, {100, 0}}, {32768, 65535, {50, 0}}}});
        locator2.SetMultiProgress(
            {{{0, 32767, {50, 0}}, {32768, 65535, {100, 0}}}, {{0, 32767, {100, 0}}, {32768, 65535, {50, 0}}}});
        ASSERT_EQ(locator1, locator2);
        ASSERT_EQ(Locator::LocatorCompareResult::LCR_FULLY_FASTER, locator1.IsFasterThan(locator2, false));
        ASSERT_EQ(Locator::LocatorCompareResult::LCR_FULLY_FASTER, locator2.IsFasterThan(locator1, false));
    }
    {
        Locator locator1, locator2;
        locator1.SetMultiProgress(
            {{{0, 32767, {150, 0}}, {32768, 65535, {100, 0}}}, {{0, 32767, {150, 0}}, {32768, 65535, {50, 0}}}});
        locator2.SetMultiProgress(
            {{{0, 32767, {50, 0}}, {32768, 65535, {100, 0}}}, {{0, 32767, {100, 0}}, {32768, 65535, {50, 0}}}});
        ASSERT_EQ(Locator::LocatorCompareResult::LCR_FULLY_FASTER, locator1.IsFasterThan(locator2, false));
        ASSERT_EQ(Locator::LocatorCompareResult::LCR_SLOWER, locator2.IsFasterThan(locator1, false));
    }
    {
        Locator locator1, locator2;
        locator1.SetMultiProgress(
            {{{0, 32767, {50, 0}}, {32768, 65535, {150, 0}}}, {{0, 32767, {100, 0}}, {32768, 65535, {50, 0}}}});
        locator2.SetMultiProgress(
            {{{0, 32767, {50, 0}}, {32768, 65535, {100, 0}}}, {{0, 32767, {100, 0}}, {32768, 65535, {150, 0}}}});
        ASSERT_EQ(Locator::LocatorCompareResult::LCR_PARTIAL_FASTER, locator1.IsFasterThan(locator2, false));
        ASSERT_EQ(Locator::LocatorCompareResult::LCR_PARTIAL_FASTER, locator2.IsFasterThan(locator1, false));
    }
    {
        Locator locator1, locator2, locator3;
        locator1.SetMultiProgress(
            {{{0, 32767, {50, 0}}, {32768, 65535, {150, 0}}}, {{0, 32767, {100, 0}}, {32768, 65535, {50, 0}}}});
        // locator2 is invalid
        ASSERT_EQ(Locator::LocatorCompareResult::LCR_FULLY_FASTER, locator1.IsFasterThan(locator2, false));
        ASSERT_EQ(Locator::LocatorCompareResult::LCR_SLOWER, locator2.IsFasterThan(locator1, false));
        // loactor2, locator3
        ASSERT_EQ(Locator::LocatorCompareResult::LCR_FULLY_FASTER, locator2.IsFasterThan(locator3, false));
    }
    {
        Locator locator1;
        locator1.SetMultiProgress(
            {{{0, 32767, {50, 0}}, {32768, 65535, {150, 0}}}, {{0, 32767, {100, 0}}, {32768, 65535, {50, 0}}}});

        ASSERT_EQ(Locator::LocatorCompareResult::LCR_SLOWER,
                  locator1.IsFasterThan(
                      Locator::DocInfo(/*hashId*/ 60000, /*timestamp*/ 50, /*concurrentIdx*/ 0, /*sourceIdx*/ 1)));
        ASSERT_EQ(Locator::LocatorCompareResult::LCR_FULLY_FASTER,
                  locator1.IsFasterThan(
                      Locator::DocInfo(/*hashId*/ 60000, /*timestamp*/ 49, /*concurrentIdx*/ 0, /*sourceIdx*/ 1)));
        ASSERT_EQ(Locator::LocatorCompareResult::LCR_FULLY_FASTER,
                  locator1.IsFasterThan(
                      Locator::DocInfo(/*hashId*/ 60000, /*timestamp*/ 50, /*concurrentIdx*/ 0, /*sourceIdx*/ 0)));
        ASSERT_EQ(Locator::LocatorCompareResult::LCR_SLOWER,
                  locator1.IsFasterThan(
                      Locator::DocInfo(/*hashId*/ 60000, /*timestamp*/ 200, /*concurrentIdx*/ 0, /*sourceIdx*/ 0)));
    }
    {
        // locator is invalid
        Locator locator;
        ASSERT_EQ(Locator::LocatorCompareResult::LCR_SLOWER,
                  locator.IsFasterThan(
                      Locator::DocInfo(/*hashId*/ 60000, /*timestamp*/ 50, /*concurrentIdx*/ 0, /*sourceIdx*/ 1)));
    }
}

TEST_F(LocatorTest, testUpdateForMultiProgress)
{
    Locator locator, locator1, locator2;
    locator1.SetMultiProgress(
        {{{0, 32767, {150, 0}}, {32768, 65535, {100, 0}}}, {{0, 32767, {150, 0}}, {32768, 65535, {50, 0}}}});
    locator2.SetMultiProgress(
        {{{0, 32767, {50, 0}}, {32768, 65535, {100, 0}}}, {{0, 32767, {100, 0}}, {32768, 65535, {50, 0}}}});
    ASSERT_EQ(Locator::LocatorCompareResult::LCR_FULLY_FASTER, locator1.IsFasterThan(locator2, false));
    ASSERT_EQ(Locator::LocatorCompareResult::LCR_SLOWER, locator2.IsFasterThan(locator1, false));
}

} // namespace indexlibv2::framework
