#include "indexlib/table/common/LSMTabletLoader.h"

#include "autil/StringUtil.h"
#include "indexlib/framework/ResourceMap.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/framework/mock/MockDiskSegment.h"
#include "unittest/unittest.h"

namespace indexlibv2 { namespace table {

class LSMTabletLoaderTest : public TESTBASE
{
public:
    LSMTabletLoaderTest() {}
    ~LSMTabletLoaderTest() = default;
    void setUp() override {}
    void tearDown() override {}

private:
    framework::Locator MakeLocator(uint64_t src, int64_t minOffset,
                                   std::vector<std::tuple<uint32_t, uint32_t, int64_t>> progressParam);
    //"versionId:segmentId,locator(,progresses);segmentId,locator..."
    std::unique_ptr<framework::TabletData>
    CreateTabletData(const std::string& str, const framework::Locator versionLocator = framework::Locator());
};
framework::Locator LSMTabletLoaderTest::MakeLocator(uint64_t src, int64_t minOffset,
                                                    std::vector<std::tuple<uint32_t, uint32_t, int64_t>> progressParam)
{
    framework::Locator locator(src, minOffset);
    std::vector<base::Progress> progress;
    for (auto [from, to, ts] : progressParam) {
        base::Progress::Offset offset = {ts, 0};
        progress.emplace_back(from, to, offset);
    }
    locator.SetMultiProgress({progress});
    return locator;
}

std::unique_ptr<framework::TabletData> LSMTabletLoaderTest::CreateTabletData(const std::string& str,
                                                                             const framework::Locator versionLocator)
{
    std::vector<std::string> pairStr;
    autil::StringUtil::fromString(str, pairStr, ":");
    versionid_t versionId;
    autil::StringUtil::fromString(pairStr[0], versionId);
    framework::Version version(versionId);
    std::vector<std::string> segmentList;
    autil::StringUtil::fromString(pairStr[1], segmentList, ";");
    std::vector<std::shared_ptr<framework::Segment>> segments;
    for (size_t i = 0; i < segmentList.size(); i++) {
        std::vector<std::string> segInfos;
        autil::StringUtil::fromString(segmentList[i], segInfos, ",");
        assert(segInfos.size() == 2 || segInfos.size() == 3);
        segmentid_t segmentId;
        int64_t offset;
        autil::StringUtil::fromString(segInfos[0], segmentId);
        autil::StringUtil::fromString(segInfos[1], offset);

        version.AddSegment(segmentId);
        framework::SegmentMeta segMeta;
        segMeta.segmentId = segmentId;
        framework::Locator locator(1, offset);
        if (segInfos.size() == 3) {
            std::vector<std::string> progressStr;
            autil::StringUtil::fromString(segInfos[2], progressStr, "#");
            std::vector<base::Progress> progresses;
            for (auto& str : progressStr) {
                std::vector<int64_t> tmp;
                autil::StringUtil::fromString(str, tmp, "|");
                base::Progress::Offset offset = {tmp[2], 0};
                progresses.push_back({(uint32_t)tmp[0], (uint32_t)tmp[1], offset});
            }
            locator.SetMultiProgress({progresses});
        }
        segMeta.segmentInfo->SetLocator(locator);
        segments.push_back(std::make_shared<framework::MockDiskSegment>(segMeta));
        if (i == segmentList.size() - 1) {
            version.SetLocator((versionLocator.GetOffset().first == -1) ? locator : versionLocator);
        }
    }
    auto tabletData = std::make_unique<framework::TabletData>("test");
    Status status = tabletData->Init(version.Clone(), segments, std::make_shared<framework::ResourceMap>());
    assert(status.IsOK());
    return tabletData;
}

TEST_F(LSMTabletLoaderTest, TestSimpleProcess)
{
    auto currentTabletData = CreateTabletData("100:"           // version 100
                                              "536870912,20;"  // segment 536870912
                                              "536870913,50;"  // segment 536870913
                                              "536870914,80;"  // segment 536870914
                                              "536870915,100;" // segment 536870915
    );
    // version 1, segment 1
    auto mergedTabletData = CreateTabletData("1:1,50");
    LSMTabletLoader loader("test");
    auto status = loader.DoPreLoad(*currentTabletData, mergedTabletData->TEST_GetSegments(),
                                   mergedTabletData->GetOnDiskVersion());
    ASSERT_TRUE(status.IsOK());
    auto [ret, tabletData] = loader.FinalLoad(*currentTabletData);
    ASSERT_TRUE(ret.IsOK());
    auto segments = tabletData->TEST_GetSegments();
    ASSERT_EQ(3, segments.size());
    ASSERT_EQ(1u, segments[0]->GetSegmentId());
    ASSERT_EQ(536870914u, segments[1]->GetSegmentId());
    ASSERT_EQ(536870915u, segments[2]->GetSegmentId());
}

TEST_F(LSMTabletLoaderTest, TestIncLocatorEqual)
{
    auto currentTabletData = CreateTabletData("100:"           // version 100
                                              "536870912,10;"  // segment 536870912
                                              "536870913,50;"  // segment 536870913
                                              "536870914,80;"  // segment 536870914
                                              "536870915,100;" // segment 536870915
    );
    // version 1, segment 1
    auto mergedTabletData = CreateTabletData("1:1,100");
    LSMTabletLoader loader("test");
    auto status = loader.DoPreLoad(*currentTabletData, mergedTabletData->TEST_GetSegments(),
                                   mergedTabletData->GetOnDiskVersion());
    ASSERT_TRUE(status.IsOK());
    auto [ret, tabletData] = loader.FinalLoad(*currentTabletData);
    ASSERT_TRUE(ret.IsOK());
    auto segments = tabletData->TEST_GetSegments();
    ASSERT_EQ(1, segments.size());
    ASSERT_EQ(1u, segments[0]->GetSegmentId());
}

TEST_F(LSMTabletLoaderTest, TestIncLocatorGreater)
{
    auto currentTabletData = CreateTabletData("100:"           // version 100
                                              "536870912,10;"  // segment 536870912
                                              "536870913,50;"  // segment 536870913
                                              "536870914,80;"  // segment 536870914
                                              "536870915,100;" // segment 536870915
    );
    // version 1, segment 1
    auto mergedTabletData = CreateTabletData("1:1,101");
    LSMTabletLoader loader("test");
    auto status = loader.DoPreLoad(*currentTabletData, mergedTabletData->TEST_GetSegments(),
                                   mergedTabletData->GetOnDiskVersion());
    ASSERT_TRUE(status.IsOK());
    auto [ret, tabletData] = loader.FinalLoad(*currentTabletData);
    ASSERT_TRUE(ret.IsOK());
    auto segments = tabletData->TEST_GetSegments();
    ASSERT_EQ(1, segments.size());
    ASSERT_EQ(1u, segments[0]->GetSegmentId());
}

TEST_F(LSMTabletLoaderTest, TestReplaceMergedSegment)
{
    auto currentTabletData = CreateTabletData("100:"           // version 100
                                              "1,50;"          // segment 1
                                              "536870912,50;"  // segment 536870912
                                              "536870913,80;"  // segment 536870913
                                              "536870914,100;" // segment 536870914
    );
    // version 1, segment 1
    auto mergedTabletData = CreateTabletData("1:2,80");
    LSMTabletLoader loader("test");
    auto status = loader.DoPreLoad(*currentTabletData, mergedTabletData->TEST_GetSegments(),
                                   mergedTabletData->GetOnDiskVersion());
    ASSERT_TRUE(status.IsOK());
    auto [ret, tabletData] = loader.FinalLoad(*currentTabletData);
    ASSERT_TRUE(ret.IsOK());
    auto segments = tabletData->TEST_GetSegments();
    ASSERT_EQ(2, segments.size());
    ASSERT_EQ(2u, segments[0]->GetSegmentId());
    ASSERT_EQ(536870914u, segments[1]->GetSegmentId());
}

TEST_F(LSMTabletLoaderTest, TestIncCoverPartSegment)
{
    auto currentTabletData = CreateTabletData("100:"           // version 100
                                              "1,50;"          // segment 1
                                              "536870912,50;"  // segment 536870912
                                              "536870913,80;"  // segment 536870913
                                              "536870914,102;" // segment 536870914
    );
    // version 1, segment 1
    auto mergedTabletData = CreateTabletData("1:1,50;2,101");
    LSMTabletLoader loader("test");
    auto status = loader.DoPreLoad(*currentTabletData, mergedTabletData->TEST_GetSegments(),
                                   mergedTabletData->GetOnDiskVersion());
    ASSERT_TRUE(status.IsOK());
    auto [ret, tabletData] = loader.FinalLoad(*currentTabletData);
    ASSERT_TRUE(ret.IsOK());
    auto segments = tabletData->TEST_GetSegments();
    ASSERT_EQ(3, segments.size());
    ASSERT_EQ(1u, segments[0]->GetSegmentId());
    ASSERT_EQ(2u, segments[1]->GetSegmentId());
    ASSERT_EQ(536870914u, segments[2]->GetSegmentId());
}

TEST_F(LSMTabletLoaderTest, TestDifferentRange)
{
    {
        // offline full build, parallel num == 2, slave builder reopen public version
        auto currentTabletData = CreateTabletData("1073741826:"                // version 1073741826
                                                  "536870912,20,0|32767|20;"   // segment 536870914: {0, 32767, 50}
                                                  "536870914,50,0|32767|50;"); // segment 536870914: {0, 32767, 50}

        auto importedTabletData =
            CreateTabletData("536870912:"                   // version 536870914
                             "536870912,20,0|32767|20;"     // segment 536870914: {0, 32767, 50}
                             "536870913,60,32768|65535|60;" // segment 536870912: {32768, 65535, 60}
                             "536870914,50,0|32767|50;",    // segment 536870914: {0, 32767, 50}
                             MakeLocator(1, 50, {{0, 32767, 50}, {32768, 65535, 60}}));

        LSMTabletLoader loader("test");
        loader._isOnline = false;
        auto status = loader.DoPreLoad(*currentTabletData, importedTabletData->TEST_GetSegments(),
                                       importedTabletData->GetOnDiskVersion());
        ASSERT_TRUE(status.IsOK());
        auto [ret, tabletData] = loader.FinalLoad(*currentTabletData);
        ASSERT_TRUE(ret.IsOK());
        auto segments = tabletData->TEST_GetSegments();
        ASSERT_EQ(3, segments.size());
        ASSERT_EQ(536870912u, segments[0]->GetSegmentId());
        ASSERT_EQ(536870913u, segments[1]->GetSegmentId());
        ASSERT_EQ(536870914u, segments[2]->GetSegmentId());
    }
    {
        // offline inc build, parallel num == 2, slave builder reopen public version
        auto currentTabletData =
            CreateTabletData("1073741828:"                     // version 1073741828
                             "0,20,0|32767|20#32768|65535|30;" // segment 0: {0, 32767, 20}, {32768, 65535, 30}
                             "536870914,50,0|32767|50;"        // segment 536870914: {0, 32767, 50}
                             "536870916,80,0|32767|80",        // segment 536870916: {0, 32767, 80}
                             MakeLocator(1, 30, {{0, 32767, 80}, {32768, 65535, 30}}));

        auto mergedTabletData =
            CreateTabletData("536870914:"                        // version 536870914
                             "0,20,0|32767|20#32768|65535|30;"   // segment 0: {0, 32767, 20}, {32768, 65535, 30}
                             "1,50,0|32767|50#32768|65535|100"); // segment 1: {0, 32767, 50}, {32768, 65535, 100}

        LSMTabletLoader loader("test");
        loader._isOnline = false;
        auto status = loader.DoPreLoad(*currentTabletData, mergedTabletData->TEST_GetSegments(),
                                       mergedTabletData->GetOnDiskVersion());
        ASSERT_TRUE(status.IsOK());
        auto [ret, tabletData] = loader.FinalLoad(*currentTabletData);
        ASSERT_TRUE(ret.IsOK());
        auto segments = tabletData->TEST_GetSegments();
        ASSERT_EQ(3, segments.size());
        ASSERT_EQ(0u, segments[0]->GetSegmentId());
        ASSERT_EQ(1u, segments[1]->GetSegmentId());
        ASSERT_EQ(536870916u, segments[2]->GetSegmentId());
    }
}

}} // namespace indexlibv2::table
