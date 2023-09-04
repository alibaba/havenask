#include "indexlib/partition/test/on_disk_index_cleaner_unittest.h"

#include "indexlib/file_system/EntryTable.h"
#include "indexlib/index_base/deploy_index_wrapper.h"
#include "indexlib/partition/reader_container.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/version_maker.h"

using namespace std;
using namespace indexlib::test;
using namespace indexlib::index;
using namespace indexlib::file_system;
using namespace indexlib::config;
using namespace indexlib::index_base;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, OnDiskIndexCleanerTest);

namespace {
class MockReaderContainer : public ReaderContainer
{
public:
    MockReaderContainer() {}
    ~MockReaderContainer() {}

public:
    MOCK_METHOD(bool, HasReader, (versionid_t), (const, override));
};
DEFINE_SHARED_PTR(MockReaderContainer);
} // namespace

OnDiskIndexCleanerTest::OnDiskIndexCleanerTest() {}

OnDiskIndexCleanerTest::~OnDiskIndexCleanerTest() {}

void OnDiskIndexCleanerTest::CaseSetUp()
{
    file_system::FileSystemOptions fsOptions;
    fsOptions.isOffline = false;
    fsOptions.outputStorage = file_system::FSST_MEM;
    RESET_FILE_SYSTEM("ut", false, fsOptions);
}

void OnDiskIndexCleanerTest::CaseTearDown() {}

void OnDiskIndexCleanerTest::TestSimpleProcess()
{
    MakeOnDiskVersion(0, "0,1");
    MakeOnDiskVersion(1, "1,2");

    OnDiskIndexCleaner cleaner(1, ReaderContainerPtr(), GET_PARTITION_DIRECTORY());
    cleaner.Execute();
    ASSERT_TRUE(!GET_CHECK_DIRECTORY()->IsExist("version.0"));
    ASSERT_TRUE(!GET_CHECK_DIRECTORY()->IsExist("segment_0_level_0"));
    ASSERT_TRUE(GET_CHECK_DIRECTORY()->IsExist("segment_1_level_0"));
    // test entry_table.x
    ASSERT_TRUE(!GET_CHECK_DIRECTORY()->IsExist(EntryTable::GetEntryTableFileName(0)));
    ASSERT_TRUE(GET_CHECK_DIRECTORY()->IsExist(EntryTable::GetEntryTableFileName(1)));
}

void OnDiskIndexCleanerTest::TestDeployIndexMeta()
{
    MakeOnDiskVersion(0, "0,1");
    MakeOnDiskVersion(1, "1,2");
    ASSERT_TRUE(GET_CHECK_DIRECTORY()->IsExist("version.0"));
    ASSERT_TRUE(GET_CHECK_DIRECTORY()->IsExist("deploy_meta.0"));
    OnDiskIndexCleaner cleaner(1, ReaderContainerPtr(), GET_PARTITION_DIRECTORY());
    cleaner.Execute();
    ASSERT_TRUE(!GET_CHECK_DIRECTORY()->IsExist("version.0"));
    ASSERT_TRUE(!GET_CHECK_DIRECTORY()->IsExist("deploy_meta.0"));
    ASSERT_TRUE(GET_CHECK_DIRECTORY()->IsExist("version.1"));
    ASSERT_TRUE(GET_CHECK_DIRECTORY()->IsExist("deploy_meta.1"));
}

void OnDiskIndexCleanerTest::TestCleanWithNoClean()
{
    MakeOnDiskVersion(0, "0");
    MakeOnDiskVersion(1, "0,1");

    OnDiskIndexCleaner cleaner(2, ReaderContainerPtr(), GET_PARTITION_DIRECTORY());
    cleaner.Execute();

    ASSERT_TRUE(GET_CHECK_DIRECTORY()->IsExist("version.0"));
    ASSERT_TRUE(GET_CHECK_DIRECTORY()->IsExist("version.1"));

    ASSERT_TRUE(GET_CHECK_DIRECTORY()->IsExist("segment_0_level_0"));
    ASSERT_TRUE(GET_CHECK_DIRECTORY()->IsExist("segment_1_level_0"));

    // test entry_table.x
    ASSERT_TRUE(GET_CHECK_DIRECTORY()->IsExist(EntryTable::GetEntryTableFileName(0)));
    ASSERT_TRUE(GET_CHECK_DIRECTORY()->IsExist(EntryTable::GetEntryTableFileName(1)));
}

void OnDiskIndexCleanerTest::TestCleanWithInvalidKeepCount()
{
    MakeOnDiskVersion(0, "0");
    MakeOnDiskVersion(1, "1");

    OnDiskIndexCleaner cleaner(0, ReaderContainerPtr(), GET_PARTITION_DIRECTORY());
    cleaner.Execute();

    ASSERT_TRUE(!GET_CHECK_DIRECTORY()->IsExist("version.0"));
    ASSERT_TRUE(GET_CHECK_DIRECTORY()->IsExist("version.1"));

    ASSERT_TRUE(!GET_CHECK_DIRECTORY()->IsExist("segment_0_level_0"));
    ASSERT_TRUE(GET_CHECK_DIRECTORY()->IsExist("segment_1_level_0"));

    // test entry_table.x
    ASSERT_TRUE(!GET_CHECK_DIRECTORY()->IsExist(EntryTable::GetEntryTableFileName(0)));
    ASSERT_TRUE(GET_CHECK_DIRECTORY()->IsExist(EntryTable::GetEntryTableFileName(1)));
}

void OnDiskIndexCleanerTest::TestCleanWithIncontinuousVersion()
{
    MakeOnDiskVersion(0, "0,1,2");
    MakeOnDiskVersion(2, "0,3");
    MakeOnDiskVersion(4, "0,3,4");
    OnDiskIndexCleaner cleaner(2, ReaderContainerPtr(), GET_PARTITION_DIRECTORY());
    cleaner.Execute();

    ASSERT_TRUE(!GET_CHECK_DIRECTORY()->IsExist("version.0"));
    ASSERT_TRUE(GET_CHECK_DIRECTORY()->IsExist("version.2"));
    ASSERT_TRUE(GET_CHECK_DIRECTORY()->IsExist("version.4"));

    ASSERT_TRUE(GET_CHECK_DIRECTORY()->IsExist("segment_0_level_0"));
    ASSERT_TRUE(!GET_CHECK_DIRECTORY()->IsExist("segment_1_level_0"));
    ASSERT_TRUE(!GET_CHECK_DIRECTORY()->IsExist("segment_2_level_0"));
    ASSERT_TRUE(GET_CHECK_DIRECTORY()->IsExist("segment_3_level_0"));
    ASSERT_TRUE(GET_CHECK_DIRECTORY()->IsExist("segment_4_level_0"));

    // test entry_table.x
    ASSERT_TRUE(!GET_CHECK_DIRECTORY()->IsExist(EntryTable::GetEntryTableFileName(0)));
    ASSERT_TRUE(GET_CHECK_DIRECTORY()->IsExist(EntryTable::GetEntryTableFileName(2)));
    ASSERT_TRUE(GET_CHECK_DIRECTORY()->IsExist(EntryTable::GetEntryTableFileName(4)));
}

void OnDiskIndexCleanerTest::TestCleanWithInvalidSegments()
{
    MakeOnDiskVersion(0, "0");
    MakeOnDiskVersion(2, "9");

    MakeOnDiskVersion(INVALID_VERSION, "5,10");

    OnDiskIndexCleaner cleaner(1, ReaderContainerPtr(), GET_PARTITION_DIRECTORY());
    cleaner.Execute();

    ASSERT_TRUE(!GET_CHECK_DIRECTORY()->IsExist("version.0"));
    ASSERT_TRUE(GET_CHECK_DIRECTORY()->IsExist("version.2"));

    ASSERT_TRUE(!GET_CHECK_DIRECTORY()->IsExist("segment_5_level_0"));
    ASSERT_TRUE(GET_CHECK_DIRECTORY()->IsExist("segment_10_level_0"));
}

void OnDiskIndexCleanerTest::TestCleanWithEmptyVersion()
{
    MakeOnDiskVersion(0, "0");
    MakeOnDiskVersion(2, "");
    MakeOnDiskVersion(4, "");
    OnDiskIndexCleaner cleaner(1, ReaderContainerPtr(), GET_PARTITION_DIRECTORY());
    cleaner.Execute();

    ASSERT_TRUE(!GET_CHECK_DIRECTORY()->IsExist("version.0"));
    ASSERT_TRUE(!GET_CHECK_DIRECTORY()->IsExist("version.2"));
    ASSERT_TRUE(GET_CHECK_DIRECTORY()->IsExist("version.4"));
    ASSERT_TRUE(!GET_CHECK_DIRECTORY()->IsExist("segment_0_level_0"));

    // test entry_table.x
    ASSERT_TRUE(!GET_CHECK_DIRECTORY()->IsExist(EntryTable::GetEntryTableFileName(0)));
    ASSERT_TRUE(!GET_CHECK_DIRECTORY()->IsExist(EntryTable::GetEntryTableFileName(2)));
    ASSERT_TRUE(GET_CHECK_DIRECTORY()->IsExist(EntryTable::GetEntryTableFileName(4)));
}

void OnDiskIndexCleanerTest::TestCleanWithAllEmptyVersion()
{
    MakeOnDiskVersion(0, "");
    MakeOnDiskVersion(1, "");
    MakeOnDiskVersion(INVALID_VERSION, "0");
    OnDiskIndexCleaner cleaner(1, ReaderContainerPtr(), GET_PARTITION_DIRECTORY());
    cleaner.Execute();

    ASSERT_TRUE(!GET_CHECK_DIRECTORY()->IsExist("version.0"));
    ASSERT_TRUE(GET_CHECK_DIRECTORY()->IsExist("version.1"));
    ASSERT_TRUE(GET_CHECK_DIRECTORY()->IsExist("segment_0_level_0"));
    // test entry_table.x
    ASSERT_TRUE(!GET_CHECK_DIRECTORY()->IsExist(EntryTable::GetEntryTableFileName(0)));
    ASSERT_TRUE(GET_CHECK_DIRECTORY()->IsExist(EntryTable::GetEntryTableFileName(1)));
}

void OnDiskIndexCleanerTest::TestCleanWithUsingVersion()
{
    MockReaderContainerPtr container(new MockReaderContainer);
    EXPECT_CALL(*container, HasReader(0)).WillRepeatedly(Return(false));
    EXPECT_CALL(*container, HasReader(1)).WillRepeatedly(Return(true));
    EXPECT_CALL(*container, HasReader(2)).WillRepeatedly(Return(true));

    MakeOnDiskVersion(0, "0");
    MakeOnDiskVersion(1, "1,2");
    MakeOnDiskVersion(2, "1,3");
    MakeOnDiskVersion(3, "1,3,4");
    OnDiskIndexCleaner cleaner(1, container, GET_PARTITION_DIRECTORY());
    cleaner.Execute();

    ASSERT_TRUE(!GET_CHECK_DIRECTORY()->IsExist("version.0"));
    ASSERT_TRUE(GET_CHECK_DIRECTORY()->IsExist("version.1"));
    ASSERT_TRUE(GET_CHECK_DIRECTORY()->IsExist("version.2"));
    ASSERT_TRUE(GET_CHECK_DIRECTORY()->IsExist("version.3"));

    // test entry_table.x
    ASSERT_TRUE(!GET_CHECK_DIRECTORY()->IsExist(EntryTable::GetEntryTableFileName(0)));
    ASSERT_TRUE(GET_CHECK_DIRECTORY()->IsExist(EntryTable::GetEntryTableFileName(1)));
    ASSERT_TRUE(GET_CHECK_DIRECTORY()->IsExist(EntryTable::GetEntryTableFileName(2)));
    ASSERT_TRUE(GET_CHECK_DIRECTORY()->IsExist(EntryTable::GetEntryTableFileName(3)));

    ASSERT_TRUE(!GET_CHECK_DIRECTORY()->IsExist("segment_0_level_0"));
    ASSERT_TRUE(GET_CHECK_DIRECTORY()->IsExist("segment_1_level_0"));
    ASSERT_TRUE(GET_CHECK_DIRECTORY()->IsExist("segment_2_level_0"));
    ASSERT_TRUE(GET_CHECK_DIRECTORY()->IsExist("segment_3_level_0"));
    ASSERT_TRUE(GET_CHECK_DIRECTORY()->IsExist("segment_4_level_0"));
}

void OnDiskIndexCleanerTest::TestCleanWithUsingOldVersion()
{
    MockReaderContainerPtr container(new MockReaderContainer);
    EXPECT_CALL(*container, HasReader(0)).WillRepeatedly(Return(true));
    EXPECT_CALL(*container, HasReader(1)).WillRepeatedly(Return(false));
    EXPECT_CALL(*container, HasReader(2)).WillRepeatedly(Return(false));

    MakeOnDiskVersion(0, "0");
    MakeOnDiskVersion(1, "1,2");
    MakeOnDiskVersion(2, "1,3");
    OnDiskIndexCleaner cleaner(1, container, GET_PARTITION_DIRECTORY());
    cleaner.Execute();

    ASSERT_TRUE(GET_CHECK_DIRECTORY()->IsExist("version.0"));
    ASSERT_TRUE(GET_CHECK_DIRECTORY()->IsExist("version.1"));
    ASSERT_TRUE(GET_CHECK_DIRECTORY()->IsExist("version.2"));

    // test entry_table.x
    ASSERT_TRUE(GET_CHECK_DIRECTORY()->IsExist(EntryTable::GetEntryTableFileName(0)));
    ASSERT_TRUE(GET_CHECK_DIRECTORY()->IsExist(EntryTable::GetEntryTableFileName(1)));
    ASSERT_TRUE(GET_CHECK_DIRECTORY()->IsExist(EntryTable::GetEntryTableFileName(2)));

    ASSERT_TRUE(GET_CHECK_DIRECTORY()->IsExist("segment_0_level_0"));
    ASSERT_TRUE(GET_CHECK_DIRECTORY()->IsExist("segment_1_level_0"));
    ASSERT_TRUE(GET_CHECK_DIRECTORY()->IsExist("segment_2_level_0"));
    ASSERT_TRUE(GET_CHECK_DIRECTORY()->IsExist("segment_3_level_0"));
}

void OnDiskIndexCleanerTest::TestCleanWithRollbackVersion()
{
    MakeOnDiskVersion(0, "0");
    MakeOnDiskVersion(1, "1");
    MakeOnDiskVersion(2, "0");
    OnDiskIndexCleaner cleaner(1, ReaderContainerPtr(), GET_PARTITION_DIRECTORY());
    cleaner.Execute();

    ASSERT_TRUE(!GET_CHECK_DIRECTORY()->IsExist("version.0"));
    ASSERT_TRUE(!GET_CHECK_DIRECTORY()->IsExist("version.1"));

    // test entry_table.x
    ASSERT_TRUE(!GET_CHECK_DIRECTORY()->IsExist(EntryTable::GetEntryTableFileName(0)));
    ASSERT_TRUE(!GET_CHECK_DIRECTORY()->IsExist(EntryTable::GetEntryTableFileName(1)));

    ASSERT_TRUE(GET_CHECK_DIRECTORY()->IsExist("version.2"));
    ASSERT_TRUE(GET_CHECK_DIRECTORY()->IsExist("segment_0_level_0"));
    ASSERT_TRUE(!GET_CHECK_DIRECTORY()->IsExist("segment_1_level_0"));
}

void OnDiskIndexCleanerTest::MakeOnDiskVersion(versionid_t versionId, const std::string& segmentIds)
{
    DirectoryPtr physicalDir = GET_CHECK_DIRECTORY(false);
    Version version = VersionMaker::Make(versionId, segmentIds);
    if (versionId != INVALID_VERSION) {
        version.Store(physicalDir, false);
        // test reclaim entryTable.x
        physicalDir->Store(EntryTable::GetEntryTableFileName(version.GetVersionId()), "");
        physicalDir->Store(DeployIndexWrapper::GetDeployMetaFileName(version.GetVersionId()), "");
    }

    for (size_t i = 0; i < version.GetSegmentCount(); ++i) {
        string segPath = version.GetSegmentDirName(version[i]);
        if (!physicalDir->IsExist(segPath)) {
            physicalDir->MakeDirectory(segPath);
        }
    }
}
}} // namespace indexlib::partition
