#include "indexlib/partition/test/partition_reader_cleaner_unittest.h"
#include "indexlib/partition/reader_container.h"
#include "indexlib/test/version_maker.h"
#include "indexlib/index_base/segment/online_segment_directory.h"
#include "indexlib/file_system/indexlib_file_system_impl.h"
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, PartitionReaderCleanerTest);

namespace {
    class MockIndexlibFileSystemImpl : public IndexlibFileSystemImpl
    {
    public:
        MockIndexlibFileSystemImpl(const string& rootPath)
            : IndexlibFileSystemImpl(rootPath)
        {}
        ~MockIndexlibFileSystemImpl() {}
    public:
        MOCK_METHOD0(CleanCache, void());
    };
    DEFINE_SHARED_PTR(MockIndexlibFileSystemImpl);
}

PartitionReaderCleanerTest::PartitionReaderCleanerTest()
{
}

PartitionReaderCleanerTest::~PartitionReaderCleanerTest()
{
}

void PartitionReaderCleanerTest::CaseSetUp()
{
    mFileSystem = GET_FILE_SYSTEM();
    mRootDir = GET_PARTITION_DIRECTORY();
}

void PartitionReaderCleanerTest::CaseTearDown()
{
}

void PartitionReaderCleanerTest::TestCleanWithUsingReader()
{
    ReaderContainerPtr container(new ReaderContainer);
    // add one not using reader
    container->AddReader(MakeMockIndexPartitionReader(0, "0", "0", "0"));

    // add one using reader
    IndexPartitionReaderPtr usingReader = MakeMockIndexPartitionReader(1, "1", "1", "1");
    container->AddReader(usingReader);

    // add another not using reader
    container->AddReader(MakeMockIndexPartitionReader(2, "2", "2", "2"));

    PartitionReaderCleaner cleaner(container, mFileSystem, mDataLock);
    cleaner.Execute();

    ASSERT_EQ((size_t)2, container->Size());
    ASSERT_EQ(usingReader, container->GetOldestReader());
}

void PartitionReaderCleanerTest::TestCleanCache()
{
    ReaderContainerPtr container(new ReaderContainer);
    IndexPartitionReaderPtr usingReader = MakeMockIndexPartitionReader(0, "0", "0", "0");
    container->AddReader(usingReader);

    MockIndexlibFileSystemImplPtr fileSystem(new MockIndexlibFileSystemImpl(
                    mRootDir->GetPath()));
    FileSystemOptions fileSystemOptions;
    util::MemoryQuotaControllerPtr quotaControllor(
            new util::MemoryQuotaController(1024*1024*1024));
    util::PartitionMemoryQuotaControllerPtr controllor(
            new util::PartitionMemoryQuotaController(quotaControllor));
    fileSystemOptions.memoryQuotaController = controllor;
    fileSystem->Init(fileSystemOptions);
    EXPECT_CALL(*fileSystem, CleanCache())
        .WillOnce(Return());

    PartitionReaderCleaner cleaner(container, fileSystem, mDataLock);
    cleaner.Execute();
}

void PartitionReaderCleanerTest::TestCleanMultipleUnusedReader()
{
    ReaderContainerPtr container(new ReaderContainer);
    container->AddReader(MakeMockIndexPartitionReader(0, "0", "0", "0"));
    container->AddReader(MakeMockIndexPartitionReader(1, "1", "1", "1"));
    container->AddReader(MakeMockIndexPartitionReader(2, "2", "2", "2"));
    IndexPartitionReaderPtr usingReader = MakeMockIndexPartitionReader(3, "3", "3", "3");
    container->AddReader(usingReader);

    PartitionReaderCleaner cleaner(container, mFileSystem, mDataLock);
    cleaner.Execute();
    ASSERT_EQ((size_t)1, container->Size());
    ASSERT_EQ(usingReader, container->GetOldestReader());
}

IndexPartitionReaderPtr PartitionReaderCleanerTest::MakeMockIndexPartitionReader(
        versionid_t versionId, const string& incSegmentIds, 
        const string& rtSegmentIds, const string& joinSegmentIds)
{
    MockIndexPartitionReaderPtr reader(new MockIndexPartitionReader());
    Version version = VersionMaker::Make(versionId, incSegmentIds, 
            rtSegmentIds, joinSegmentIds);

    EXPECT_CALL(*reader, GetVersion())
        .WillRepeatedly(Return(version));
    return reader;
}

IE_NAMESPACE_END(partition);

