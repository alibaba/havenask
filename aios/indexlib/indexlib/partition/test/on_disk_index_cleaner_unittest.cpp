#include "indexlib/partition/test/on_disk_index_cleaner_unittest.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/partition/reader_container.h"
#include "indexlib/test/version_maker.h"
#include "indexlib/index_base/deploy_index_wrapper.h"

using namespace std;
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, OnDiskIndexCleanerTest);

namespace {
class MockReaderContainer : public ReaderContainer
{
public:
    MockReaderContainer() {}
    ~MockReaderContainer() {}

public:
    MOCK_CONST_METHOD1(HasReader, bool(versionid_t));
};
DEFINE_SHARED_PTR(MockReaderContainer);
}

OnDiskIndexCleanerTest::OnDiskIndexCleanerTest()
{
}

OnDiskIndexCleanerTest::~OnDiskIndexCleanerTest()
{
}

void OnDiskIndexCleanerTest::CaseSetUp()
{
    mRootDir = GET_PARTITION_DIRECTORY();
}

void OnDiskIndexCleanerTest::CaseTearDown()
{
}

void OnDiskIndexCleanerTest::TestSimpleProcess()
{
    MakeOnDiskVersion(0, "0,1");
    MakeOnDiskVersion(1, "1,2");
    OnDiskIndexCleaner cleaner(1, ReaderContainerPtr(), mRootDir);
    cleaner.Execute();
    ASSERT_TRUE(!mRootDir->IsExist("version.0"));
    ASSERT_TRUE(!mRootDir->IsExist("segment_0_level_0"));
    ASSERT_TRUE(mRootDir->IsExist("segment_1_level_0"));
}

void OnDiskIndexCleanerTest::TestDeployIndexMeta()
{
    MakeOnDiskVersion(0, "0,1");
    MakeOnDiskVersion(1, "1,2");
    ASSERT_TRUE(mRootDir->IsExist("version.0"));
    ASSERT_TRUE(mRootDir->IsExist("deploy_meta.0"));
    OnDiskIndexCleaner cleaner(1, ReaderContainerPtr(), mRootDir);
    cleaner.Execute();
    ASSERT_TRUE(!mRootDir->IsExist("version.0"));
    ASSERT_TRUE(!mRootDir->IsExist("deploy_meta.0"));
    ASSERT_TRUE(mRootDir->IsExist("version.1"));
    ASSERT_TRUE(mRootDir->IsExist("deploy_meta.1"));
}

void OnDiskIndexCleanerTest::TestCleanWithNoClean()
{
    MakeOnDiskVersion(0, "0");
    MakeOnDiskVersion(1, "0,1");

    OnDiskIndexCleaner cleaner(2, ReaderContainerPtr(), mRootDir);
    cleaner.Execute();

    ASSERT_TRUE(mRootDir->IsExist("version.0"));
    ASSERT_TRUE(mRootDir->IsExist("version.1"));

    ASSERT_TRUE(mRootDir->IsExist("segment_0_level_0"));
    ASSERT_TRUE(mRootDir->IsExist("segment_1_level_0"));
}

void OnDiskIndexCleanerTest::TestCleanWithInvalidKeepCount()
{
    MakeOnDiskVersion(0, "0");
    MakeOnDiskVersion(1, "1");

    OnDiskIndexCleaner cleaner(0, ReaderContainerPtr(), mRootDir);
    cleaner.Execute();

    ASSERT_TRUE(!mRootDir->IsExist("version.0"));
    ASSERT_TRUE(mRootDir->IsExist("version.1"));

    ASSERT_TRUE(!mRootDir->IsExist("segment_0_level_0"));
    ASSERT_TRUE(mRootDir->IsExist("segment_1_level_0"));
}

void OnDiskIndexCleanerTest::TestCleanWithIncontinuousVersion() 
{
    MakeOnDiskVersion(0, "0,1,2");
    MakeOnDiskVersion(2, "0,3");
    MakeOnDiskVersion(4, "0,3,4");
    OnDiskIndexCleaner cleaner(2, ReaderContainerPtr(), mRootDir);
    cleaner.Execute();

    ASSERT_TRUE(!mRootDir->IsExist("version.0"));
    ASSERT_TRUE(mRootDir->IsExist("version.2"));
    ASSERT_TRUE(mRootDir->IsExist("version.4"));

    ASSERT_TRUE(mRootDir->IsExist("segment_0_level_0"));
    ASSERT_TRUE(!mRootDir->IsExist("segment_1_level_0"));
    ASSERT_TRUE(!mRootDir->IsExist("segment_2_level_0"));
    ASSERT_TRUE(mRootDir->IsExist("segment_3_level_0"));
    ASSERT_TRUE(mRootDir->IsExist("segment_4_level_0"));
}

void OnDiskIndexCleanerTest::TestCleanWithInvalidSegments()
{
    MakeOnDiskVersion(0, "0");
    MakeOnDiskVersion(2, "9");

    MakeOnDiskVersion(INVALID_VERSION, "5,10");
    
    OnDiskIndexCleaner cleaner(1, ReaderContainerPtr(), mRootDir);
    cleaner.Execute();

    ASSERT_TRUE(!mRootDir->IsExist("version.0"));
    ASSERT_TRUE(mRootDir->IsExist("version.2"));

    ASSERT_TRUE(!mRootDir->IsExist("segment_5_level_0"));
    ASSERT_TRUE(mRootDir->IsExist("segment_10_level_0"));
}

void OnDiskIndexCleanerTest::TestCleanWithEmptyVersion()
{
    MakeOnDiskVersion(0, "0");
    MakeOnDiskVersion(2, "");
    MakeOnDiskVersion(4, "");
    OnDiskIndexCleaner cleaner(1, ReaderContainerPtr(), mRootDir);
    cleaner.Execute();

    ASSERT_TRUE(!mRootDir->IsExist("version.0"));
    ASSERT_TRUE(!mRootDir->IsExist("version.2"));
    ASSERT_TRUE(mRootDir->IsExist("version.4"));
    ASSERT_TRUE(!mRootDir->IsExist("segment_0_level_0"));
}

void OnDiskIndexCleanerTest::TestCleanWithAllEmptyVersion()
{
    MakeOnDiskVersion(0, "");
    MakeOnDiskVersion(1, "");
    MakeOnDiskVersion(INVALID_VERSION, "0");
    OnDiskIndexCleaner cleaner(1, ReaderContainerPtr(), mRootDir);
    cleaner.Execute();

    ASSERT_TRUE(!mRootDir->IsExist("version.0"));
    ASSERT_TRUE(mRootDir->IsExist("version.1"));
    ASSERT_TRUE(mRootDir->IsExist("segment_0_level_0"));
}

void OnDiskIndexCleanerTest::TestCleanWithUsingVersion()
{
    MockReaderContainerPtr container(new MockReaderContainer);
    EXPECT_CALL(*container, HasReader(0))
        .WillRepeatedly(Return(false));
    EXPECT_CALL(*container, HasReader(1))
        .WillRepeatedly(Return(true));
    EXPECT_CALL(*container, HasReader(2))
        .WillRepeatedly(Return(true));

    MakeOnDiskVersion(0, "0");
    MakeOnDiskVersion(1, "1,2");
    MakeOnDiskVersion(2, "1,3");
    MakeOnDiskVersion(3, "1,3,4");
    OnDiskIndexCleaner cleaner(1, container, mRootDir);
    cleaner.Execute();

    ASSERT_TRUE(!mRootDir->IsExist("version.0"));
    ASSERT_TRUE(mRootDir->IsExist("version.1"));
    ASSERT_TRUE(mRootDir->IsExist("version.2"));
    ASSERT_TRUE(mRootDir->IsExist("version.3"));
    ASSERT_TRUE(!mRootDir->IsExist("segment_0_level_0"));
    ASSERT_TRUE(mRootDir->IsExist("segment_1_level_0"));
    ASSERT_TRUE(mRootDir->IsExist("segment_2_level_0"));
    ASSERT_TRUE(mRootDir->IsExist("segment_3_level_0"));
    ASSERT_TRUE(mRootDir->IsExist("segment_4_level_0"));
}

void OnDiskIndexCleanerTest::TestCleanWithUsingOldVersion()
{
    MockReaderContainerPtr container(new MockReaderContainer);
    EXPECT_CALL(*container, HasReader(0))
        .WillRepeatedly(Return(true));
    EXPECT_CALL(*container, HasReader(1))
        .WillRepeatedly(Return(false));
    EXPECT_CALL(*container, HasReader(2))
        .WillRepeatedly(Return(false));

    MakeOnDiskVersion(0, "0");
    MakeOnDiskVersion(1, "1,2");
    MakeOnDiskVersion(2, "1,3");
    OnDiskIndexCleaner cleaner(1, container, mRootDir);
    cleaner.Execute();

    ASSERT_TRUE(mRootDir->IsExist("version.0"));
    ASSERT_TRUE(mRootDir->IsExist("version.1"));
    ASSERT_TRUE(mRootDir->IsExist("version.2"));
    ASSERT_TRUE(mRootDir->IsExist("segment_0_level_0"));
    ASSERT_TRUE(mRootDir->IsExist("segment_1_level_0"));
    ASSERT_TRUE(mRootDir->IsExist("segment_2_level_0"));
    ASSERT_TRUE(mRootDir->IsExist("segment_3_level_0"));
}

void OnDiskIndexCleanerTest::TestCleanWithRollbackVersion()
{
    MakeOnDiskVersion(0, "0");
    MakeOnDiskVersion(1, "1");
    MakeOnDiskVersion(2, "0");
    OnDiskIndexCleaner cleaner(1, ReaderContainerPtr(), mRootDir);
    cleaner.Execute();

    ASSERT_TRUE(!mRootDir->IsExist("version.0"));
    ASSERT_TRUE(!mRootDir->IsExist("version.1"));
    ASSERT_TRUE(mRootDir->IsExist("version.2"));
    ASSERT_TRUE(mRootDir->IsExist("segment_0_level_0"));
    ASSERT_TRUE(!mRootDir->IsExist("segment_1_level_0"));
}

void OnDiskIndexCleanerTest::MakeOnDiskVersion(
        versionid_t versionId, const std::string& segmentIds)
{
    Version version = VersionMaker::Make(versionId, segmentIds);
    if (versionId != INVALID_VERSION)
    {
        version.Store(mRootDir, false);
        mRootDir->Store(DeployIndexWrapper::GetDeployMetaFileName(version.GetVersionId()), "");
    }
    for (size_t i = 0; i < version.GetSegmentCount(); ++i)
    {
        string segPath = version.GetSegmentDirName(version[i]);
        if (!mRootDir->IsExist(segPath))
        {
            mRootDir->MakeDirectory(segPath);
        }
    }
}

IE_NAMESPACE_END(partition);
