#include "indexlib/framework/cleaner/TabletReaderCleaner.h"

#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/framework/ResourceMap.h"
#include "indexlib/framework/test/FakeTabletReader.h"
#include "unittest/unittest.h"

namespace indexlibv2::framework {
class TabletReaderCleanerTest : public TESTBASE
{
};

// left one reader at least
TEST_F(TabletReaderCleanerTest, LeftReaderCntTest)
{
    auto tabletReaderContainer = std::make_shared<TabletReaderContainer>("ut");
    ASSERT_EQ(0, tabletReaderContainer->Size());

    size_t readerCnt = 20;
    versionid_t onDiskVersionId = -1;
    versionid_t buildVersionId = -1;
    for (size_t i = 0; i < readerCnt; ++i) {
        Version onDiskVersion(++onDiskVersionId);
        Version buildVersion(++buildVersionId);
        std::string tabletName(std::to_string(i));
        auto tabletData = std::make_shared<TabletData>(tabletName);
        Status st = tabletData->Init(onDiskVersion.Clone(), {}, std::make_shared<ResourceMap>());
        ASSERT_TRUE(st.IsOK());
        auto tabletReader = std::make_shared<FakeTabletReader>(tabletName);
        tabletReaderContainer->AddTabletReader(tabletData, tabletReader, /*version deploy description*/ nullptr);
    }

    indexlib::file_system::FileSystemOptions fsOptions;
    std::string rootPath = GET_TEMP_DATA_PATH();
    auto fs = indexlib::file_system::FileSystemCreator::Create("online", rootPath, fsOptions).GetOrThrow();

    TabletReaderCleaner tabletReaderCleaner(tabletReaderContainer.get(), fs, "" /* empty name */);
    ASSERT_TRUE(tabletReaderCleaner.Clean().IsOK());

    ASSERT_EQ(0, tabletReaderContainer->Size());
}

TEST_F(TabletReaderCleanerTest, RefTest)
{
    auto tabletReaderContainer = std::make_shared<TabletReaderContainer>("ut");
    ASSERT_EQ(0, tabletReaderContainer->Size());

    size_t readerCnt = 20;
    versionid_t onDiskVersionId = -1;
    versionid_t buildVersionId = -1;
    for (size_t i = 0; i < readerCnt; ++i) {
        Version onDiskVersion(++onDiskVersionId);
        Version buildVersion(++buildVersionId);
        std::string tabletName(std::to_string(i));
        auto tabletData = std::make_shared<TabletData>(tabletName);
        Status st = tabletData->Init(onDiskVersion.Clone(), {}, std::make_shared<ResourceMap>());
        ASSERT_TRUE(st.IsOK());
        auto tabletReader = std::make_shared<FakeTabletReader>(tabletName);
        tabletReaderContainer->AddTabletReader(tabletData, tabletReader, /*version deploy description*/ nullptr);
    }

    auto refReader = tabletReaderContainer->TEST_GetTabletReader(5);

    indexlib::file_system::FileSystemOptions fsOptions;
    std::string rootPath = GET_TEMP_DATA_PATH();
    auto fs = indexlib::file_system::FileSystemCreator::Create("online", rootPath, fsOptions).GetOrThrow();

    TabletReaderCleaner tabletReaderCleaner(tabletReaderContainer.get(), fs, "" /* empty name */);
    ASSERT_TRUE(tabletReaderCleaner.Clean().IsOK());

    ASSERT_EQ(15, tabletReaderContainer->Size());
}
} // namespace indexlibv2::framework
