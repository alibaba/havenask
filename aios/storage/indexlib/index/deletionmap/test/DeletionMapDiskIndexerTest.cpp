#include "indexlib/index/deletionmap/DeletionMapDiskIndexer.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/index/deletionmap/DeletionMapConfig.h"
#include "indexlib/util/Bitmap.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;

namespace indexlibv2::index {

class DeletionMapDiskIndexerTest : public indexlib::INDEXLIB_TESTBASE
{
public:
    DeletionMapDiskIndexerTest();
    ~DeletionMapDiskIndexerTest();

    DECLARE_CLASS_NAME(DeletionMapDiskIndexerTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    AUTIL_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(DeletionMapDiskIndexerTest, TestSimpleProcess);
AUTIL_LOG_SETUP(indexlib.index, DeletionMapDiskIndexerTest);

DeletionMapDiskIndexerTest::DeletionMapDiskIndexerTest() {}

DeletionMapDiskIndexerTest::~DeletionMapDiskIndexerTest() {}

void DeletionMapDiskIndexerTest::CaseSetUp() {}

void DeletionMapDiskIndexerTest::CaseTearDown() {}

void DeletionMapDiskIndexerTest::TestSimpleProcess()
{
    DeletionMapDiskIndexer diskIndexer(100, 0);
    shared_ptr<DeletionMapConfig> config(new DeletionMapConfig);

    indexlib::file_system::FileSystemOptions fsOptions;
    fsOptions.enableAsyncFlush = false;
    auto fs = indexlib::file_system::FileSystemCreator::Create("test", GET_TEMP_DATA_PATH(), fsOptions).GetOrThrow();
    auto rootDir = indexlib::file_system::Directory::Get(fs);
    ASSERT_TRUE(diskIndexer.Open(config, rootDir->GetIDirectory()).IsOK());
    ASSERT_FALSE(diskIndexer.IsDeleted(0));
    ASSERT_FALSE(diskIndexer.IsDeleted(99));
    ASSERT_TRUE(diskIndexer.Delete(0).IsOK());
    ASSERT_TRUE(diskIndexer.Delete(99).IsOK());
    ASSERT_TRUE(diskIndexer.IsDeleted(0));
    ASSERT_TRUE(diskIndexer.IsDeleted(99));
    ASSERT_EQ((uint32_t)2, diskIndexer.GetDeletedDocCount());

    indexlib::util::Bitmap bitmap(100, false);
    ASSERT_TRUE(bitmap.Set(11));
    ASSERT_TRUE(diskIndexer.ApplyDeletionMapPatch(&bitmap).IsOK());
    ASSERT_TRUE(diskIndexer.IsDeleted(11));
}

} // namespace indexlibv2::index
