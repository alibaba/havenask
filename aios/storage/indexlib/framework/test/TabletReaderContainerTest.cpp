#include "indexlib/framework/TabletReaderContainer.h"

#include "indexlib/file_system/DeployIndexMeta.h"
#include "indexlib/framework/ResourceMap.h"
#include "indexlib/framework/VersionDeployDescription.h"
#include "indexlib/framework/test/FakeTabletReader.h"
#include "unittest/unittest.h"

namespace indexlibv2::framework {

class TabletReaderContainerTest : public TESTBASE
{
};

TEST_F(TabletReaderContainerTest, EmptyTest)
{
    TabletReaderContainer tabletReaderContainer("ut");
    ASSERT_EQ(0, tabletReaderContainer.Size());

    ASSERT_FALSE(tabletReaderContainer.HasTabletReader(0));
    ASSERT_FALSE(tabletReaderContainer.HasTabletReader(-1));
    ASSERT_FALSE(tabletReaderContainer.IsUsingSegment(-1));
    ASSERT_FALSE(tabletReaderContainer.IsUsingSegment(100));
    ASSERT_TRUE(tabletReaderContainer.GetOldestTabletReader() == nullptr);
    ASSERT_TRUE(tabletReaderContainer.GetLatestTabletReader() == nullptr);

    tabletReaderContainer.Close();
    ASSERT_EQ(0, tabletReaderContainer.Size());
}

TEST_F(TabletReaderContainerTest, SimpleProcess)
{
    TabletReaderContainer tabletReaderContainer("ut");
    ASSERT_EQ(0, tabletReaderContainer.Size());

    versionid_t onDiskVersionId = -1;
    for (size_t i = 0; i < 3; ++i) {
        Version onDiskVersion(++onDiskVersionId);
        for (size_t j = 0; j < 3; ++j) {
            std::string tabletName(std::to_string(i * 3 + j));
            auto tabletData = std::make_shared<TabletData>(tabletName);
            Status st = tabletData->Init(onDiskVersion.Clone(), {}, std::make_shared<ResourceMap>());
            ASSERT_TRUE(st.IsOK());
            auto tabletReader = std::make_shared<FakeTabletReader>(tabletName);
            tabletReaderContainer.AddTabletReader(tabletData, tabletReader, nullptr);

            auto latestTabletReader = tabletReaderContainer.GetLatestTabletReader();
            auto reader = std::dynamic_pointer_cast<FakeTabletReader>(latestTabletReader);
            ASSERT_EQ(tabletName, reader->TabletName());
        }
    }

    ASSERT_EQ(9, tabletReaderContainer.Size());

    for (versionid_t i = 0; i < 3; ++i) {
        ASSERT_TRUE(tabletReaderContainer.HasTabletReader(i));
    }
    for (versionid_t i = 3; i < 22; ++i) {
        ASSERT_FALSE(tabletReaderContainer.HasTabletReader(i));
    }

    for (size_t i = 0; i < 3; ++i) {
        auto oldestTabletReader = tabletReaderContainer.GetOldestTabletReader();
        auto reader = std::dynamic_pointer_cast<FakeTabletReader>(oldestTabletReader);
        ASSERT_EQ(std::to_string(i), reader->TabletName());
        tabletReaderContainer.EvictOldestTabletReader();
    }

    tabletReaderContainer.Close();
    ASSERT_EQ(0, tabletReaderContainer.Size());
}

TEST_F(TabletReaderContainerTest, TestGetNeedKeepFilesAndDirs)
{
    TabletReaderContainer tabletReaderContainer("ut");
    ASSERT_EQ(0, tabletReaderContainer.Size());

    versionid_t onDiskVersionId = -1;
    for (size_t i = 0; i < 3; ++i) {
        Version onDiskVersion(++onDiskVersionId);
        onDiskVersion.AddSegment(i);
        std::string tabletName(std::to_string(i * 3));
        auto tabletData = std::make_shared<TabletData>(tabletName);
        auto versionDeployDescription = std::make_shared<VersionDeployDescription>();
        if (i < 2) {
            auto deployIndexMeta = std::make_shared<indexlib::file_system::DeployIndexMeta>();
            std::string segmentName = "/segment_" + std::to_string(i) + "_level_0/";
            deployIndexMeta->Append({segmentName, -1, 100});
            deployIndexMeta->Append({segmentName + "/data", 6, 100});
            versionDeployDescription->localDeployIndexMetas.push_back(deployIndexMeta);
        }
        Status st = tabletData->Init(onDiskVersion.Clone(), {}, std::make_shared<ResourceMap>());
        ASSERT_TRUE(st.IsOK());
        auto tabletReader = std::make_shared<FakeTabletReader>(tabletName);
        tabletReaderContainer.AddTabletReader(tabletData, tabletReader, versionDeployDescription);
    }
    std::set<std::string> keepFiles;
    std::set<segmentid_t> keepSegments;
    EXPECT_TRUE(tabletReaderContainer.GetNeedKeepFiles(&keepFiles));
    tabletReaderContainer.GetNeedKeepSegments(&keepSegments);
    for (size_t i = 0; i < 2; ++i) {
        std::string segmentName = "/segment_" + std::to_string(i) + "_level_0/";
        ASSERT_TRUE(keepFiles.find(segmentName) != keepFiles.end());
        ASSERT_TRUE(keepFiles.find(segmentName + "/data") != keepFiles.end());
    }
    ASSERT_TRUE(keepSegments.find(2) != keepSegments.end());
}

} // namespace indexlibv2::framework
