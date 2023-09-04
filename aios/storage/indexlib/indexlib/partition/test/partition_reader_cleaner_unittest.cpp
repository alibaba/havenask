#include "indexlib/partition/test/partition_reader_cleaner_unittest.h"

#include "autil/StringUtil.h"
#include "autil/Thread.h"
#include "indexlib/file_system/LogicalFileSystem.h"
#include "indexlib/index_base/segment/online_segment_directory.h"
#include "indexlib/partition/reader_container.h"
#include "indexlib/test/version_maker.h"

using namespace std;
using namespace autil;
using namespace indexlib::index;
using namespace indexlib::file_system;
using namespace indexlib::index_base;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, PartitionReaderCleanerTest);

namespace {
class MockFileSystem : public LogicalFileSystem
{
public:
    MockFileSystem(const string& rootPath) : LogicalFileSystem(/*name=*/"unused", rootPath, util::MetricProviderPtr())
    {
    }
    ~MockFileSystem() {}

public:
    MOCK_METHOD(void, CleanCache, (), (noexcept, override));
};
DEFINE_SHARED_PTR(MockFileSystem);
} // namespace

PartitionReaderCleanerTest::PartitionReaderCleanerTest() {}

PartitionReaderCleanerTest::~PartitionReaderCleanerTest() {}

void PartitionReaderCleanerTest::CaseSetUp()
{
    mFileSystem = GET_FILE_SYSTEM();
    mRootDir = GET_PARTITION_DIRECTORY();
}

void PartitionReaderCleanerTest::CaseTearDown() {}

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

    MockFileSystemPtr fileSystem(new MockFileSystem(GET_TEMP_DATA_PATH()));
    FileSystemOptions fileSystemOptions;
    util::MemoryQuotaControllerPtr quotaControllor(new util::MemoryQuotaController(1024 * 1024 * 1024));
    util::PartitionMemoryQuotaControllerPtr controllor(new util::PartitionMemoryQuotaController(quotaControllor));
    fileSystemOptions.memoryQuotaController = controllor;
    ASSERT_EQ(FSEC_OK, fileSystem->Init(fileSystemOptions));
    EXPECT_CALL(*fileSystem, CleanCache()).WillOnce(Return());

    PartitionReaderCleaner cleaner(container, fileSystem, mDataLock);
    cleaner.Execute();
}

void PartitionReaderCleanerTest::TestEndlessLoop()
{
    // xxxx://invalid/issue/34863136
    ReaderContainerPtr container(new ReaderContainer);
    for (size_t i = 0; i < 5; ++i) {
        container->AddReader(MakeMockIndexPartitionReader(0, "0", "0", "0"));
    }
    // add one not using reader
    atomic<bool> done(false);
    auto rtBuildThread = autil::Thread::createThread([&]() {
        while (!done.load()) {
            {
                // try to obtain the lock is realy time-consuming, so add the interval
                ScopedLock lock(mDataLock);
                container->AddReader(MakeMockIndexPartitionReader(0, "0", "0", "0"));
                container->AddReader(MakeMockIndexPartitionReader(0, "0", "0", "0"));
            }
            usleep(10000);
        }
    });
    PartitionReaderCleaner cleaner(container, mFileSystem, mDataLock);
    cleaner.Execute();
    done = true;
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

IndexPartitionReaderPtr PartitionReaderCleanerTest::MakeMockIndexPartitionReader(versionid_t versionId,
                                                                                 const string& incSegmentIds,
                                                                                 const string& rtSegmentIds,
                                                                                 const string& joinSegmentIds)
{
    MockIndexPartitionReaderPtr reader(new MockIndexPartitionReader());
    Version version = VersionMaker::Make(versionId, incSegmentIds, rtSegmentIds, joinSegmentIds);

    EXPECT_CALL(*reader, GetVersion()).WillRepeatedly(Return(version));
    return reader;
}
}} // namespace indexlib::partition
