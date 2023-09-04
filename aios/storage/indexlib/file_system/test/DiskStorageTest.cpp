

#include <assert.h>
#include <iosfwd>
#include <memory>
#include <stdint.h>
#include <string.h>

#include "autil/CommonMacros.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/file_system/EntryTable.h"
#include "indexlib/file_system/EntryTableBuilder.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FileBlockCache.h"
#include "indexlib/file_system/FileBlockCacheContainer.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/LogicalFileSystem.h"
#include "indexlib/file_system/ReaderOption.h"
#include "indexlib/file_system/Storage.h"
#include "indexlib/file_system/StorageMetrics.h"
#include "indexlib/file_system/WriterOption.h"
#include "indexlib/file_system/file/BlockFileNode.h"
#include "indexlib/file_system/file/BufferedFileReader.h"
#include "indexlib/file_system/file/FileNode.h"
#include "indexlib/file_system/file/FileNodeCache.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/FileWriterImpl.h"
#include "indexlib/file_system/file/MemFileNode.h"
#include "indexlib/file_system/file/MmapFileNode.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/load_config/LoadConfig.h"
#include "indexlib/file_system/load_config/LoadConfigList.h"
#include "indexlib/file_system/load_config/LoadStrategy.h"
#include "indexlib/file_system/load_config/MemLoadStrategy.h"
#include "indexlib/file_system/load_config/MmapLoadStrategy.h"
#include "indexlib/file_system/package/PackageFileMeta.h"
#include "indexlib/file_system/test/FileSystemTestUtil.h"
#include "indexlib/file_system/test/LoadConfigListCreator.h"
#include "indexlib/file_system/test/PackageFileUtil.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/TaskScheduler.h"
#include "indexlib/util/byte_slice_list/ByteSliceList.h"
#include "indexlib/util/memory_control/BlockMemoryQuotaController.h"
#include "indexlib/util/memory_control/MemoryQuotaControllerCreator.h"
#include "indexlib/util/memory_control/PartitionMemoryQuotaController.h"
#include "indexlib/util/metrics/MetricProvider.h"
#include "indexlib/util/testutil/unittest.h"

namespace indexlib { namespace util {
class UnSupportedException;
}} // namespace indexlib::util

using namespace std;
using namespace autil;

using namespace indexlib::util;

namespace indexlib { namespace file_system {
class DiskStorageTest : public INDEXLIB_TESTBASE
{
public:
    DiskStorageTest();
    ~DiskStorageTest();

    DECLARE_CLASS_NAME(DiskStorageTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestLocalInit();
    void TestMmapFileReader();
    void TestBlockFileReader();
    void TestLocalBlockCache();
    void TestGlobalBlockFileReader();
    void TestMemFileReader();
    void TestBufferedFileReader();
    void TestBufferedFileWriter();
    void TestUnknownFileReader();
    void TestNoLoadConfigListWithCacheHit();
    void TestNoLoadConfigListWithCacheMiss();
    void TestLoadConfigListWithNoCache();
    void TestLoadConfigListWithCacheHit();
    void TestLoadConfigListWithCacheMiss();
    void TestBlockFileLoadConfigMatch();
    void TestLoadWithLifecycleConfig();
    void TestStorageMetrics();
    void TestStorageMetricsForCacheMiss();
    void TestRemoveFile();
    void TestRemoveDirectory();

    void TestPackageFileAndSharedMMap();                 // dcache
    void TestPackageFileAndSharedMMapWithBufferedFile(); // dcache
    void TestPackageFileAndSharedBuffer();               // dcache
    void TestCleanSharePackageFileWithFileOpened();
    void TestMmapInDfs();
    void TestMmapInDCache();
    void TestGetLoadConfigOpenType();
    void TestMultiBlockCacheWithLifeCycle();
    void TestResourceFile();

private:
    void CreateLoadConfigList(LoadConfigList& loadConfigList);
    void CheckFileReader(std::shared_ptr<DiskStorage> storage, std::string filePath, FSOpenType type,
                         std::string expectContext, bool fromLoadConfig = false, bool writable = false);
    std::shared_ptr<FileReader> CheckPackageFile(const std::shared_ptr<LogicalFileSystem>& fs,
                                                 const std::string& logicalFilePath, const std::string& expectContent,
                                                 FSOpenType openType);
    std::shared_ptr<LogicalFileSystem> CreateFSWithLoadConfig(const std::string& rootPath,
                                                              const std::string& loadConfigJsonStr);

    std::shared_ptr<DiskStorage> PrepareDiskStorage();
    void InnerTestBufferedFileWriter(bool atomicWrite);
    void InnerTestLifecycle(const std::string& loadConfigJsonStr, const std::string& matchPath,
                            const std::string lifecycle, FSOpenType openType, bool expectLock,
                            std::function<bool(std::shared_ptr<FileNode>&)> getLockModeFunc,
                            std::function<void(std::shared_ptr<FileNode>&)> checkFunc);
    std::string GetTestPath(const std::string& path);

private:
    util::BlockMemoryQuotaControllerPtr _memoryController;
    FileBlockCacheContainerPtr _fileBlockCacheContainer;
    TaskSchedulerPtr _taskScheduler;
    util::MetricProviderPtr _metricProvider;
    autil::RecursiveThreadMutex _mutex;

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP(indexlib.file_system, DiskStorageTest);

INDEXLIB_UNIT_TEST_CASE(DiskStorageTest, TestLocalInit);
INDEXLIB_UNIT_TEST_CASE(DiskStorageTest, TestMmapFileReader);
INDEXLIB_UNIT_TEST_CASE(DiskStorageTest, TestBlockFileReader);
INDEXLIB_UNIT_TEST_CASE(DiskStorageTest, TestLocalBlockCache);
INDEXLIB_UNIT_TEST_CASE(DiskStorageTest, TestGlobalBlockFileReader);
INDEXLIB_UNIT_TEST_CASE(DiskStorageTest, TestMemFileReader);
INDEXLIB_UNIT_TEST_CASE(DiskStorageTest, TestBufferedFileReader);
INDEXLIB_UNIT_TEST_CASE(DiskStorageTest, TestBufferedFileWriter);
INDEXLIB_UNIT_TEST_CASE(DiskStorageTest, TestUnknownFileReader);
INDEXLIB_UNIT_TEST_CASE(DiskStorageTest, TestNoLoadConfigListWithCacheHit);
INDEXLIB_UNIT_TEST_CASE(DiskStorageTest, TestNoLoadConfigListWithCacheMiss);
INDEXLIB_UNIT_TEST_CASE(DiskStorageTest, TestLoadConfigListWithNoCache);
INDEXLIB_UNIT_TEST_CASE(DiskStorageTest, TestLoadConfigListWithCacheHit);
INDEXLIB_UNIT_TEST_CASE(DiskStorageTest, TestLoadConfigListWithCacheMiss);
INDEXLIB_UNIT_TEST_CASE(DiskStorageTest, TestBlockFileLoadConfigMatch);
INDEXLIB_UNIT_TEST_CASE(DiskStorageTest, TestLoadWithLifecycleConfig);
INDEXLIB_UNIT_TEST_CASE(DiskStorageTest, TestStorageMetrics);
INDEXLIB_UNIT_TEST_CASE(DiskStorageTest, TestStorageMetricsForCacheMiss);
INDEXLIB_UNIT_TEST_CASE(DiskStorageTest, TestRemoveFile);
INDEXLIB_UNIT_TEST_CASE(DiskStorageTest, TestRemoveDirectory);

INDEXLIB_UNIT_TEST_CASE(DiskStorageTest, TestPackageFileAndSharedMMap);
INDEXLIB_UNIT_TEST_CASE(DiskStorageTest, TestPackageFileAndSharedMMapWithBufferedFile);
INDEXLIB_UNIT_TEST_CASE(DiskStorageTest, TestCleanSharePackageFileWithFileOpened);
INDEXLIB_UNIT_TEST_CASE(DiskStorageTest, TestMmapInDfs);
INDEXLIB_UNIT_TEST_CASE(DiskStorageTest, TestMmapInDCache);
INDEXLIB_UNIT_TEST_CASE(DiskStorageTest, TestGetLoadConfigOpenType);
INDEXLIB_UNIT_TEST_CASE(DiskStorageTest, TestMultiBlockCacheWithLifeCycle);
INDEXLIB_UNIT_TEST_CASE(DiskStorageTest, TestResourceFile);

//////////////////////////////////////////////////////////////////////

class MockLoadStrategy : public LoadStrategy
{
public:
    MockLoadStrategy() : _name("READ_MODE_MOCK") {}
    ~MockLoadStrategy() {}

    const string& GetLoadStrategyName() const override { return _name; }
    void Jsonize(legacy::Jsonizable::JsonWrapper& json) override {}
    bool EqualWith(const LoadStrategyPtr& loadStrategy) const override { return true; }
    void Check() override {}
    void SetEnableLoadSpeedLimit(const std::shared_ptr<bool>& enableLoadSpeedLimit) override {}

private:
    string _name;
};
typedef std::shared_ptr<MockLoadStrategy> MockLoadStrategyPtr;

DiskStorageTest::DiskStorageTest() {}

DiskStorageTest::~DiskStorageTest() {}

void DiskStorageTest::CaseSetUp()
{
    _memoryController = MemoryQuotaControllerCreator::CreateBlockMemoryController();
    _metricProvider.reset(new util::MetricProvider());
    _taskScheduler.reset(new TaskScheduler());
    MemoryQuotaControllerPtr memoryQuotaController =
        MemoryQuotaControllerCreator::CreateMemoryQuotaController(10 * 1024 * 1024);
    _fileBlockCacheContainer.reset(new FileBlockCacheContainer());
    _fileBlockCacheContainer->Init("cache_size=1", memoryQuotaController, _taskScheduler, _metricProvider);
}

void DiskStorageTest::CaseTearDown() {}

std::string DiskStorageTest::GetTestPath(const std::string& path)
{
    return PathUtil::NormalizePath(PathUtil::JoinPath(GET_TEMP_DATA_PATH(), path));
}

void DiskStorageTest::TestLocalInit()
{
    MockLoadStrategyPtr mockLoadStrategy(new MockLoadStrategy);
    LoadConfig loadConfig;
    loadConfig.SetLoadStrategyPtr(mockLoadStrategy);
    LoadConfigList loadConfigList;
    loadConfigList.PushFront(loadConfig);
    std::shared_ptr<FileSystemOptions> options(new FileSystemOptions);
    options->loadConfigList = loadConfigList;
    DiskStorage diskStorage("", _memoryController, /*entryTable=*/nullptr);

    ASSERT_EQ(FSEC_NOTSUP, diskStorage.Init(options));
}

void DiskStorageTest::TestMmapFileReader()
{
    std::shared_ptr<DiskStorage> diskStorage = PrepareDiskStorage();
    ReaderOption readerOption = ReaderOption::Writable(FSOT_MMAP);
    auto InnerTest = [&](const string& path, bool expectedIsMemLock) -> void {
        AUTIL_LOG(INFO, "test path [%s], expected mem lock[%d]", path.c_str(), expectedIsMemLock);
        FileSystemTestUtil::CreateDiskFile(GetTestPath(path), "abcd");

        std::shared_ptr<FileReader> reader0 =
            diskStorage->CreateFileReader(path, GetTestPath(path), readerOption).GetOrThrow();
        ASSERT_EQ(FSEC_OK, reader0->Open());
        std::shared_ptr<FileReader> reader1 =
            diskStorage->CreateFileReader(path, GetTestPath(path), readerOption).GetOrThrow();
        ASSERT_EQ(FSEC_OK, reader1->Open());
        ASSERT_EQ(FSOT_MMAP, reader0->GetOpenType());
        ASSERT_EQ(FSOT_MMAP, reader1->GetOpenType());

        uint8_t* data0 = (uint8_t*)reader0->GetBaseAddress();
        uint8_t* data1 = (uint8_t*)reader1->GetBaseAddress();
        ASSERT_EQ(data0, data1);
        data0[2] = '0';

        uint8_t buffer[1024];
        ASSERT_EQ(FSEC_OK, reader1->Read(buffer, 2, 1).Code());
        ASSERT_EQ('b', buffer[0]);
        ASSERT_EQ('0', buffer[1]);

        ASSERT_EQ(expectedIsMemLock, reader0->IsMemLock());
        ASSERT_EQ(expectedIsMemLock, reader1->IsMemLock());
    };

    InnerTest("test_path_match_with_pattern", false);
    InnerTest("test_lock_path_match_with_pattern", true);
}

void DiskStorageTest::TestBlockFileReader()
{
    std::shared_ptr<DiskStorage> diskStorage = PrepareDiskStorage();

    std::shared_ptr<FileNodeCache> fileNodeCache = diskStorage->_fileNodeCache;
    string path = "test_cache_path_match_with_pattern";
    FileSystemTestUtil::CreateDiskFile(GetTestPath(path), "abcd");

    std::shared_ptr<FileReader> reader0 =
        diskStorage->CreateFileReader(path, GetTestPath(path), FSOT_CACHE).GetOrThrow();
    ASSERT_EQ(FSEC_OK, reader0->Open());
    std::shared_ptr<FileNode> fileNode0 = fileNodeCache->Find(path);
    ASSERT_TRUE(fileNode0);
    ASSERT_EQ(3, fileNode0.use_count());
    std::shared_ptr<FileReader> reader1 =
        diskStorage->CreateFileReader(path, GetTestPath(path), FSOT_CACHE).GetOrThrow();
    ASSERT_EQ(FSEC_OK, reader1->Open());
    ASSERT_EQ(FSOT_CACHE, reader0->GetOpenType());
    ASSERT_EQ(FSOT_CACHE, reader1->GetOpenType());
    ASSERT_EQ(4, fileNode0.use_count());

    ASSERT_FALSE(reader0->GetBaseAddress());
    ASSERT_FALSE(reader1->GetBaseAddress());
    ASSERT_FALSE(reader0->IsMemLock());
    ASSERT_FALSE(reader1->IsMemLock());

    string defaultFilePath = "notsupportdefault";
    FileSystemTestUtil::CreateDiskFile(GetTestPath(defaultFilePath), "abcd");
    ASSERT_EQ(FSEC_NOTSUP,
              diskStorage->CreateFileReader(defaultFilePath, GetTestPath(defaultFilePath), FSOT_CACHE).Code());
}

void DiskStorageTest::TestLocalBlockCache()
{
    ASSERT_EQ(static_cast<int32_t>(1), _taskScheduler->GetTaskCount());
    std::shared_ptr<DiskStorage> diskStorage = PrepareDiskStorage();
    ASSERT_EQ(static_cast<int32_t>(2), _taskScheduler->GetTaskCount());

    string path = "test_cache_path_match_with_pattern";
    FileSystemTestUtil::CreateDiskFile(GetTestPath(path), "abcd");
    FileBlockCachePtr fileBlockCache = _fileBlockCacheContainer->GetAvailableFileCache("test", "local_cache");
    ASSERT_TRUE(fileBlockCache);

    vector<util::TaskScheduler::TaskGroupMetric> taskGroupMetrics;
    const int MAX_LOOP_COUNT = 2000;
    int count = 0;
    bool loop = true;
    while (loop && count < MAX_LOOP_COUNT) {
        _taskScheduler->GetTaskGroupMetrics(taskGroupMetrics);
        for (size_t i = 0; i < taskGroupMetrics.size(); ++i) {
            const auto& metric = taskGroupMetrics[i];
            if (metric.groupName == "report_metrics" && metric.executeTasksCount != 0) {
                loop = false;
                break;
            }
        }
        usleep(1000); // 1ms
        ++count;
    }
    ASSERT_TRUE(count <= MAX_LOOP_COUNT);

    diskStorage.reset();
    fileBlockCache.reset();
    ASSERT_EQ(static_cast<int32_t>(1), _taskScheduler->GetTaskCount());
    fileBlockCache = _fileBlockCacheContainer->GetAvailableFileCache("test", "local_cache");
    ASSERT_FALSE(fileBlockCache);
}

void DiskStorageTest::TestGlobalBlockFileReader()
{
    std::shared_ptr<DiskStorage> diskStorage = PrepareDiskStorage();

    std::shared_ptr<FileNodeCache> fileNodeCache = diskStorage->_fileNodeCache;
    string path = "test_global_cache_path_match_with_pattern";
    FileSystemTestUtil::CreateDiskFile(GetTestPath(path), "abcd");

    std::shared_ptr<FileReader> reader =
        diskStorage->CreateFileReader(path, GetTestPath(path), FSOT_CACHE).GetOrThrow();
    ASSERT_EQ(FSEC_OK, reader->Open());
    std::shared_ptr<FileNode> fileNode = fileNodeCache->Find(path);
    ASSERT_TRUE(fileNode);
    ASSERT_EQ(3, fileNode.use_count());
    ASSERT_EQ(FSOT_CACHE, reader->GetOpenType());

    char buffer[32];
    ASSERT_EQ(FSEC_OK, reader->Read(buffer, 4).Code());
    ASSERT_EQ("abcd", string(buffer, 4));
}

void DiskStorageTest::TestMemFileReader()
{
    std::shared_ptr<DiskStorage> diskStorage = PrepareDiskStorage();

    std::shared_ptr<FileNodeCache> fileNodeCache = diskStorage->_fileNodeCache;
    string path = "in_mem_file";
    FileSystemTestUtil::CreateDiskFile(GetTestPath(path), "abcd");

    std::shared_ptr<FileReader> reader0 = diskStorage->CreateFileReader(path, GetTestPath(path), FSOT_MEM).GetOrThrow();
    ASSERT_EQ(FSEC_OK, reader0->Open());
    std::shared_ptr<FileNode> fileNode = fileNodeCache->Find(path);
    ASSERT_EQ(3, fileNode.use_count());
    std::shared_ptr<FileReader> reader1 = diskStorage->CreateFileReader(path, GetTestPath(path), FSOT_MEM).GetOrThrow();
    ASSERT_EQ(FSEC_OK, reader1->Open());
    ASSERT_EQ(FSOT_MEM, reader0->GetOpenType());
    ASSERT_EQ(FSOT_MEM, reader1->GetOpenType());

    ASSERT_TRUE(reader0->IsMemLock());
    ASSERT_TRUE(reader1->IsMemLock());

    uint8_t* data0 = (uint8_t*)reader0->GetBaseAddress();
    uint8_t* data1 = (uint8_t*)reader1->GetBaseAddress();
    ASSERT_EQ(data0, data1);
    data0[2] = '0';

    uint8_t buffer[1024];
    ASSERT_EQ(FSEC_OK, reader1->Read(buffer, 2, 1).Code());
    ASSERT_EQ('b', buffer[0]);
    ASSERT_EQ('0', buffer[1]);
    fileNode = fileNodeCache->Find(path);
    ASSERT_EQ(4, fileNode.use_count());
}

void DiskStorageTest::TestBufferedFileReader()
{
    std::shared_ptr<DiskStorage> diskStorage = PrepareDiskStorage();

    string path = "buffered_file";
    FileSystemTestUtil::CreateDiskFile(GetTestPath(path), "abcd");
    std::shared_ptr<FileReader> reader =
        diskStorage->CreateFileReader(path, GetTestPath(path), FSOT_BUFFERED).GetOrThrow();
    ASSERT_EQ(FSEC_OK, reader->Open());
    ASSERT_EQ(FSOT_BUFFERED, reader->GetOpenType());
    ASSERT_EQ((size_t)4, reader->GetLength());
    ASSERT_EQ(path, reader->GetLogicalPath());
    ASSERT_FALSE(reader->GetBaseAddress());
    ASSERT_FALSE(reader->IsMemLock());

    uint8_t buffer[1024];
    ASSERT_EQ((size_t)4, reader->Read(buffer, 4).GetOrThrow());
    ASSERT_TRUE(memcmp("abcd", buffer, 4) == 0);

    ASSERT_EQ((size_t)2, reader->Read(buffer, 2, 2).GetOrThrow());
    ASSERT_TRUE(memcmp("cd", buffer, 2) == 0);

    ASSERT_EQ(nullptr, reader->ReadToByteSliceList(4, 0, ReadOption()));

    BufferedFileReaderPtr bufferedReader = std::dynamic_pointer_cast<BufferedFileReader>(reader);
    ASSERT_TRUE(bufferedReader);
}

void DiskStorageTest::TestBufferedFileWriter()
{
    InnerTestBufferedFileWriter(true);
    InnerTestBufferedFileWriter(false);
}

void DiskStorageTest::InnerTestBufferedFileWriter(bool atomicWrite)
{
    tearDown();
    setUp();
    std::shared_ptr<DiskStorage> diskStorage = PrepareDiskStorage();
    string filePath = "local/file";
    // ASSERT_THROW(diskStorage->CreateFileWriter(
    //                  filePath, WriterOption(atomicWrite)),
    //              NonExistException);
    ASSERT_EQ(FSEC_OK, diskStorage->MakeDirectory("local", GetTestPath("local"), true, false));
    WriterOption param;
    param.atomicDump = atomicWrite;
    param.bufferSize = 2;
    std::shared_ptr<FileWriter> fileWriter =
        diskStorage->CreateFileWriter(filePath, GetTestPath(filePath), param).GetOrThrow();
    ASSERT_TRUE(fileWriter);
    uint8_t buffer[] = "abcd";
    ASSERT_EQ(FSEC_OK, fileWriter->Write(buffer, 4).Code());
    if (atomicWrite) {
        ASSERT_FALSE(FslibWrapper::IsExist(GetTestPath(filePath)).GetOrThrow());
        ASSERT_TRUE(FslibWrapper::IsExist(GetTestPath(filePath + TEMP_FILE_SUFFIX)).GetOrThrow());
    } else {
        ASSERT_TRUE(FslibWrapper::IsExist(GetTestPath(filePath)).GetOrThrow());
    }
    ASSERT_EQ(FSEC_OK, fileWriter->Close());
    ASSERT_TRUE(FslibWrapper::IsExist(GetTestPath(filePath)).GetOrThrow());
}

void DiskStorageTest::TestUnknownFileReader()
{
    std::shared_ptr<DiskStorage> diskStorage = PrepareDiskStorage();

    string path = "unknown_file";
    FileSystemTestUtil::CreateDiskFile(GetTestPath(path), "abcd");
    ASSERT_EQ(FSEC_NOTSUP, diskStorage->CreateFileReader(path, GetTestPath(path), FSOT_UNKNOWN).Code());
}

void DiskStorageTest::TestResourceFile()
{
    std::shared_ptr<DiskStorage> diskStorage(new DiskStorage(false, _memoryController, /*entryTable=*/nullptr));
    ASSERT_EQ(FSEC_OK, diskStorage->Init(std::shared_ptr<FileSystemOptions>(new FileSystemOptions())));

    string path = "resource_file";
    ResourceFilePtr file = diskStorage->CreateResourceFile(path, GetTestPath(path), FSMG_LOCAL).GetOrThrow();

    int* res = new int(10);
    file->Reset<int>(res);

    size_t memUse = 50 * 1024 * 1024;
    int64_t beforeQuota = _memoryController->GetUsedQuota();
    file->UpdateMemoryUse(memUse);
    int64_t afterQuota = _memoryController->GetUsedQuota();

    ASSERT_GT(afterQuota - beforeQuota, memUse);
    file.reset();
    diskStorage.reset();
    int64_t endQuota = _memoryController->GetUsedQuota();
    ASSERT_EQ(endQuota, beforeQuota);
}

void DiskStorageTest::TestNoLoadConfigListWithCacheHit()
{
    std::shared_ptr<DiskStorage> diskStorage(new DiskStorage(false, _memoryController, /*entryTable=*/nullptr));
    ASSERT_EQ(FSEC_OK, diskStorage->Init(std::shared_ptr<FileSystemOptions>(new FileSystemOptions())));

    string inMemPath = "in_mem";
    string mmapRPath = "mmap_r";
    string mmapRWPath = "mmap_rw";
    string loadConfigPath = "load_config";

    ASSERT_ANY_THROW(CheckFileReader(diskStorage, inMemPath, FSOT_MEM, "in memory"));
    ASSERT_ANY_THROW(CheckFileReader(diskStorage, mmapRPath, FSOT_MMAP, "mmap for r"));
    ASSERT_ANY_THROW(CheckFileReader(diskStorage, mmapRWPath, FSOT_MMAP, "mmap for rw", false, true));

    FileSystemTestUtil::CreateDiskFile(GetTestPath(inMemPath), "in memory");
    FileSystemTestUtil::CreateDiskFile(GetTestPath(mmapRPath), "mmap for r");
    FileSystemTestUtil::CreateDiskFile(GetTestPath(mmapRWPath), "mmap for rw");
    FileSystemTestUtil::CreateDiskFile(GetTestPath(loadConfigPath), "load config");

    // test for first create file reader
    CheckFileReader(diskStorage, inMemPath, FSOT_MEM, "in memory");
    CheckFileReader(diskStorage, mmapRPath, FSOT_MMAP, "mmap for r");
    CheckFileReader(diskStorage, mmapRWPath, FSOT_MMAP, "mmap for rw", false, true);
    CheckFileReader(diskStorage, loadConfigPath, FSOT_LOAD_CONFIG, "load config");

    // test for cache hit
    ASSERT_EQ(FSEC_OK, FslibWrapper::DeleteFile(GetTestPath(inMemPath), DeleteOption::NoFence(false)).Code());
    ASSERT_EQ(FSEC_OK, FslibWrapper::DeleteFile(GetTestPath(mmapRPath), DeleteOption::NoFence(false)).Code());
    ASSERT_EQ(FSEC_OK, FslibWrapper::DeleteFile(GetTestPath(mmapRWPath), DeleteOption::NoFence(false)).Code());
    CheckFileReader(diskStorage, inMemPath, FSOT_MEM, "in memory");
    CheckFileReader(diskStorage, mmapRPath, FSOT_MMAP, "mmap for r");
    CheckFileReader(diskStorage, mmapRWPath, FSOT_MMAP, "mmap for rw", false, true);
    CheckFileReader(diskStorage, loadConfigPath, FSOT_LOAD_CONFIG, "load config");

    // cache hit will use same base address
    std::shared_ptr<FileReader> reader0 =
        diskStorage->CreateFileReader(mmapRWPath, GetTestPath(mmapRWPath), ReaderOption::Writable(FSOT_MMAP))
            .GetOrThrow();
    ASSERT_EQ(FSEC_OK, reader0->Open());
    std::shared_ptr<FileReader> reader1 =
        diskStorage->CreateFileReader(mmapRWPath, GetTestPath(mmapRWPath), ReaderOption::Writable(FSOT_MMAP))
            .GetOrThrow();
    ASSERT_EQ(FSEC_OK, reader1->Open());
    ASSERT_EQ(FSOT_MMAP, reader0->GetOpenType());
    ASSERT_EQ(FSOT_MMAP, reader1->GetOpenType());
    uint8_t* data0 = (uint8_t*)reader0->GetBaseAddress();
    uint8_t* data1 = (uint8_t*)reader1->GetBaseAddress();
    ASSERT_EQ(data0, data1);
    data0[5] = '0';
}

void DiskStorageTest::TestNoLoadConfigListWithCacheMiss()
{
    std::shared_ptr<DiskStorage> diskStorage(new DiskStorage(false, _memoryController, /*entryTable=*/nullptr));
    ASSERT_EQ(FSEC_OK, diskStorage->Init(std::shared_ptr<FileSystemOptions>(new FileSystemOptions())));

    string inMemPath = "in_mem";
    string mmapRPath = "mmap_r";
    string mmapRWPath = "mmap_rw";
    string loadConfigPath = "load_config";

    FileSystemTestUtil::CreateDiskFile(GetTestPath(inMemPath), "in memory");
    FileSystemTestUtil::CreateDiskFile(GetTestPath(mmapRPath), "mmap for r");
    FileSystemTestUtil::CreateDiskFile(GetTestPath(mmapRWPath), "mmap for rw");

    // test for first create file reader
    CheckFileReader(diskStorage, inMemPath, FSOT_MEM, "in memory");
    CheckFileReader(diskStorage, mmapRPath, FSOT_MMAP, "mmap for r");
    CheckFileReader(diskStorage, mmapRWPath, FSOT_MMAP, "mmap for rw", false, true);

    // test for cache Miss
    CheckFileReader(diskStorage, inMemPath, FSOT_MMAP, "in memory");
    CheckFileReader(diskStorage, mmapRWPath, FSOT_MMAP, "mmap for rw", false, true);
}

void DiskStorageTest::TestLoadConfigListWithNoCache()
{
    std::shared_ptr<DiskStorage> diskStorage = PrepareDiskStorage();

    string noMatchPath = "miss_file";
    string matchPath = "test_path_match_with_pattern";

    FileSystemTestUtil::CreateDiskFile(GetTestPath(noMatchPath), "no match file");
    FileSystemTestUtil::CreateDiskFile(GetTestPath(matchPath), "match file");

    // Open type with FSOT_MMAP
    CheckFileReader(diskStorage, noMatchPath, FSOT_MMAP, "no match file");
    CheckFileReader(diskStorage, matchPath, FSOT_MMAP, "match file", true);

    // Open type with FSOT_LOAD_CONFIG
    diskStorage->CleanCache();
    std::shared_ptr<FileNodeCache> fileNodeCache = diskStorage->_fileNodeCache;
    ASSERT_EQ((size_t)0, fileNodeCache->GetFileCount());
    CheckFileReader(diskStorage, noMatchPath, FSOT_LOAD_CONFIG, "no match file", true);
    std::shared_ptr<FileReader> reader =
        diskStorage->CreateFileReader(matchPath, GetTestPath(matchPath), FSOT_LOAD_CONFIG).GetOrThrow();
    ASSERT_EQ(FSEC_OK, reader->Open());
    ASSERT_EQ(FSOT_LOAD_CONFIG, reader->GetOpenType());
    std::shared_ptr<FileNode> fileNode = fileNodeCache->Find(matchPath);
    ASSERT_TRUE(fileNode->GetType() == FSFT_MMAP);
    ASSERT_EQ(FSEC_OK, reader->Close());
}

void DiskStorageTest::TestLoadConfigListWithCacheHit()
{
    std::shared_ptr<DiskStorage> diskStorage = PrepareDiskStorage();

    std::shared_ptr<FileNodeCache> fileNodeCache = diskStorage->_fileNodeCache;

    string inMemPath = "test_path_match_with_pattern_in_mem";
    string mmapReadPath = "test_path_match_with_pattern_mmap_read";

    FileSystemTestUtil::CreateDiskFile(GetTestPath(inMemPath), "in mem file");
    FileSystemTestUtil::CreateDiskFile(GetTestPath(mmapReadPath), "mmap read file");

    std::shared_ptr<FileReader> readerInMemFirst =
        diskStorage->CreateFileReader(inMemPath, GetTestPath(inMemPath), FSOT_MEM).GetOrThrow();
    ASSERT_EQ(FSEC_OK, readerInMemFirst->Open());
    ASSERT_EQ(FSOT_MEM, readerInMemFirst->GetOpenType());
    std::shared_ptr<FileReader> readerMmapFirst =
        diskStorage->CreateFileReader(mmapReadPath, GetTestPath(mmapReadPath), FSOT_LOAD_CONFIG).GetOrThrow();
    ASSERT_EQ(FSEC_OK, readerMmapFirst->Open());
    ASSERT_EQ(FSOT_LOAD_CONFIG, readerMmapFirst->GetOpenType());

    // to test cache hit
    // case 1: IN_MEM(mmap in loadConfig) + IN_MEM -> IN_MEM
    ASSERT_EQ(FSEC_OK, FslibWrapper::DeleteFile(GetTestPath(inMemPath), DeleteOption::NoFence(false)).Code());
    std::shared_ptr<FileReader> readerInMemSecond =
        diskStorage->CreateFileReader(inMemPath, GetTestPath(inMemPath), FSOT_MEM).GetOrThrow();
    ASSERT_EQ(FSEC_OK, readerInMemSecond->Open());
    ASSERT_EQ(FSOT_MEM, readerInMemSecond->GetOpenType());
    ASSERT_EQ(readerInMemFirst->GetBaseAddress(), readerInMemSecond->GetBaseAddress());

    // case 2: LOAD_CONFIG(mmap in loadConfig) + LOAD_CONFIG -> LOAD_CONFIG
    ASSERT_EQ(FSEC_OK, FslibWrapper::DeleteFile(GetTestPath(mmapReadPath), DeleteOption::NoFence(false)).Code());
    std::shared_ptr<FileReader> readerMmapSecond =
        diskStorage->CreateFileReader(mmapReadPath, GetTestPath(mmapReadPath), FSOT_LOAD_CONFIG).GetOrThrow();
    ASSERT_EQ(FSEC_OK, readerMmapSecond->Open());
    ASSERT_EQ(FSOT_LOAD_CONFIG, readerMmapSecond->GetOpenType());
    ASSERT_EQ(readerMmapFirst->GetBaseAddress(), readerMmapSecond->GetBaseAddress());
}

void DiskStorageTest::TestLoadConfigListWithCacheMiss()
{
    std::shared_ptr<DiskStorage> diskStorage = PrepareDiskStorage();

    std::shared_ptr<FileNodeCache> fileNodeCache = diskStorage->_fileNodeCache;

    string inMemPath = "test_path_match_with_pattern_in_mem";
    string mmapReadPath = "test_path_match_with_pattern_mmap_read";

    FileSystemTestUtil::CreateDiskFile(GetTestPath(inMemPath), "in mem file");
    FileSystemTestUtil::CreateDiskFile(GetTestPath(mmapReadPath), "mmap read file");

    CheckFileReader(diskStorage, inMemPath, FSOT_MEM, "in mem file");
    {
        std::shared_ptr<FileReader> reader =
            diskStorage->CreateFileReader(mmapReadPath, GetTestPath(mmapReadPath), FSOT_LOAD_CONFIG).GetOrThrow();
        ASSERT_EQ(FSEC_OK, reader->Open());
        ASSERT_EQ(FSOT_LOAD_CONFIG, reader->GetOpenType());
        std::shared_ptr<FileNode> fileNode = fileNodeCache->Find(mmapReadPath);
        ASSERT_TRUE(fileNode->MatchType(FSOT_LOAD_CONFIG, FSFT_UNKNOWN, false));
    }

    // to test cache miss
    // case1: IN_MEM(mmap in loadConfig) + LOAD_CONFIG -> MMAP_R
    std::shared_ptr<FileReader> reader =
        diskStorage->CreateFileReader(inMemPath, GetTestPath(inMemPath), FSOT_LOAD_CONFIG).GetOrThrow();
    ASSERT_EQ(FSEC_OK, reader->Open());
    ASSERT_EQ(FSOT_LOAD_CONFIG, reader->GetOpenType());
    std::shared_ptr<FileNode> fileNode = fileNodeCache->Find(inMemPath);
    ASSERT_TRUE(fileNode->MatchType(FSOT_LOAD_CONFIG, FSFT_UNKNOWN, false));

    // case2: LOAD_CONFIG(mmap in loadConfig) + IN_MEM -> IN_MEM
    CheckFileReader(diskStorage, mmapReadPath, FSOT_MEM, "mmap read file");
}

void DiskStorageTest::TestBlockFileLoadConfigMatch()
{
    std::shared_ptr<DiskStorage> diskStorage = PrepareDiskStorage();

    std::shared_ptr<FileNodeCache> fileNodeCache = diskStorage->_fileNodeCache;

    string blockMatchPath = "test_cache_path_match_with_pattern";
    FileSystemTestUtil::CreateDiskFile(GetTestPath(blockMatchPath), "abcd");

    std::shared_ptr<FileReader> reader =
        diskStorage->CreateFileReader(blockMatchPath, GetTestPath(blockMatchPath), FSOT_CACHE).GetOrThrow();
    ASSERT_EQ(FSEC_OK, reader->Open());
    std::shared_ptr<FileNode> fileNode = fileNodeCache->Find(blockMatchPath);
    ASSERT_TRUE(fileNode->MatchType(FSOT_CACHE, FSFT_UNKNOWN, false));
}

void DiskStorageTest::TestMultiBlockCacheWithLifeCycle()
{
    MemoryQuotaControllerPtr memoryQuotaController =
        MemoryQuotaControllerCreator::CreateMemoryQuotaController(10 * 1024 * 1024);
    _fileBlockCacheContainer.reset(new FileBlockCacheContainer());

    string cacheParam = "cache_size=1|cache_size=1;life_cycle=hot|cache_size=1;life_cycle=cold";
    _fileBlockCacheContainer->Init(cacheParam, memoryQuotaController);

    util::BlockCache* defaultCache = _fileBlockCacheContainer->GetAvailableFileCache("")->GetBlockCache().get();
    util::BlockCache* hotCache = _fileBlockCacheContainer->GetAvailableFileCache("hot")->GetBlockCache().get();
    util::BlockCache* coldCache = _fileBlockCacheContainer->GetAvailableFileCache("cold")->GetBlockCache().get();

    ASSERT_NE(defaultCache, hotCache);
    ASSERT_NE(defaultCache, coldCache);
    ASSERT_NE(hotCache, coldCache);

    string jsonStr = R"(
    {
        "load_config" :
        [
        {
            "file_patterns" : ["test_path_match_with_pattern"],
            "lifecycle" : "hot",
            "load_strategy" : "cache",
            "load_strategy_param" : {
                "global_cache" : true
            }
        },
        {
            "file_patterns" : ["test_path_match_with_pattern"],
            "lifecycle" : "cold",
            "load_strategy" : "cache",
            "load_strategy_param" : {
                "global_cache" : true
            }
        },
        {
            "file_patterns" : [".*"],
            "load_strategy" : "cache",
            "load_strategy_param" : {
                "global_cache" : true
            }
        }
        ]
    })";

    string matchPath = "test_path_match_with_pattern";
    FileSystemTestUtil::CreateDiskFile(GetTestPath(matchPath), "value of " + matchPath);

    auto checkBlockCacheWithLifeCycle = [this](const std::string& jsonStr, const std::string& matchPath,
                                               const std::string& lifeCycle) {
        std::shared_ptr<FileSystemOptions> options(new FileSystemOptions);
        LoadConfigList loadConfigList = LoadConfigListCreator::CreateLoadConfigListFromJsonString(jsonStr);
        options->loadConfigList = loadConfigList;
        options->fileBlockCacheContainer = _fileBlockCacheContainer;
        std::shared_ptr<DiskStorage> diskStorage(new DiskStorage(false, _memoryController, /*entryTable=*/nullptr));
        ASSERT_EQ(FSEC_OK, diskStorage->Init(options));
        auto mit = matchPath.rbegin();
        for (; mit != matchPath.rend() && *mit != '/'; mit++)
            ;
        diskStorage->SetDirLifecycle(matchPath.substr(0, matchPath.rend() - mit), lifeCycle);
        std::shared_ptr<FileReader> reader =
            diskStorage->CreateFileReader(matchPath, GetTestPath(matchPath), FSOT_LOAD_CONFIG).GetOrThrow();
        std::shared_ptr<FileNode> fileNode = reader->GetFileNode();
        std::shared_ptr<BlockFileNode> blockFileNode = std::dynamic_pointer_cast<BlockFileNode>(fileNode);
        ASSERT_TRUE(blockFileNode);

        util::BlockCache* cache = _fileBlockCacheContainer->GetAvailableFileCache(lifeCycle)->GetBlockCache().get();
        ASSERT_EQ(cache, blockFileNode->GetBlockCache());
    };

    checkBlockCacheWithLifeCycle(jsonStr, matchPath, "");
    checkBlockCacheWithLifeCycle(jsonStr, matchPath, "cold");
    checkBlockCacheWithLifeCycle(jsonStr, matchPath, "hot");
}

void DiskStorageTest::TestLoadWithLifecycleConfig()
{
    string jsonStr = R"(
    {
        "load_config" :
        [
        {
            "file_patterns" : ["test_path_match_with_pattern"],
            "lifecycle" : "hot",
            "load_strategy" : "mmap",
            "load_strategy_param" : {
                "lock": true
            }
        },
        {
            "file_patterns" : ["test_path_match_with_pattern"],
            "lifecycle" : "cold",
            "load_strategy" : "mmap",
            "load_strategy_param" : {
                "lock": false
            }
        },
        {
            "file_patterns" : [".*"],
            "load_strategy" : "cache",
            "load_strategy_param" : {

            }
        }
        ]
    })";

    string matchPath = "test_path_match_with_pattern";
    FileSystemTestUtil::CreateDiskFile(GetTestPath(matchPath), "value of " + matchPath);

    auto getMmapLockMode = [](std::shared_ptr<FileNode>& fileNode) {
        std::shared_ptr<MmapFileNode> _mapFileNode = std::dynamic_pointer_cast<MmapFileNode>(fileNode);
        assert(_mapFileNode);
        MmapLoadStrategyPtr loadStrategy = _mapFileNode->_loadStrategy;
        return loadStrategy->IsLock();
    };

    InnerTestLifecycle(jsonStr, matchPath, "cold", FSOT_LOAD_CONFIG, false, getMmapLockMode, nullptr);
    InnerTestLifecycle(jsonStr, matchPath, "cold", FSOT_MMAP, false, getMmapLockMode, nullptr);
    InnerTestLifecycle(jsonStr, matchPath, "hot", FSOT_MMAP, true, getMmapLockMode, nullptr);

    jsonStr = R"(
    {
        "load_config" :
        [
        {
            "file_patterns" : ["test_path_match_with_pattern"],
            "lifecycle" : "cold",
            "load_strategy" : "mmap",
            "load_strategy_param" : {
                "lock": false
            }
        },
        {
            "file_patterns" : ["test_path_match_with_pattern"],
            "lifecycle" : "hot",
            "load_strategy" : "mmap",
            "load_strategy_param" : {
                "lock": true
            }
        },
        {
            "file_patterns" : [".*"],
            "load_strategy" : "cache",
            "load_strategy_param" : {

            }
        }
        ]
    })";
    InnerTestLifecycle(jsonStr, matchPath, "cold", FSOT_MMAP, false, getMmapLockMode, nullptr);
    InnerTestLifecycle(jsonStr, matchPath, "hot", FSOT_MMAP, true, getMmapLockMode, nullptr);
    InnerTestLifecycle(jsonStr, matchPath, "hot", FSOT_LOAD_CONFIG, true, getMmapLockMode, nullptr);

    jsonStr = R"(
        {
            "load_config" :
            [
            {
                "file_patterns" : ["test_path_match_with_pattern"],
                "lifecycle" : "cold",
                "load_strategy" : "mmap",
                "load_strategy_param" : {
                    "lock": false
                }
            },
            {
                "file_patterns" : [".*"],
                "load_strategy" : "cache",
                "load_strategy_param" : {

                }
            }
            ]
        })";
    auto checkCacheLoadStrategy = [](std::shared_ptr<FileNode>& fileNode) {
        std::shared_ptr<BlockFileNode> blockFileNode = std::dynamic_pointer_cast<BlockFileNode>(fileNode);
        ASSERT_TRUE(fileNode != NULL);
    };
    InnerTestLifecycle(jsonStr, matchPath, "hot", FSOT_LOAD_CONFIG, false, nullptr, checkCacheLoadStrategy);
    InnerTestLifecycle(jsonStr, matchPath, "hot", FSOT_MMAP, false, getMmapLockMode, nullptr);
}

void DiskStorageTest::InnerTestLifecycle(const string& loadConfigJsonStr, const string& matchPath,
                                         const string lifecycle, FSOpenType openType, bool expectLock,
                                         std::function<bool(std::shared_ptr<FileNode>&)> getLockModeFunc,
                                         std::function<void(std::shared_ptr<FileNode>&)> checkFunc)
{
    LoadConfigList loadConfigList = LoadConfigListCreator::CreateLoadConfigListFromJsonString(loadConfigJsonStr);

    std::shared_ptr<FileSystemOptions> options(new FileSystemOptions);
    options->loadConfigList = loadConfigList;
    options->fileBlockCacheContainer = _fileBlockCacheContainer;
    std::shared_ptr<DiskStorage> diskStorage(new DiskStorage(false, _memoryController, /*entryTable=*/nullptr));
    ASSERT_EQ(FSEC_OK, diskStorage->Init(options));
    // diskStorage->_pathMetaContainer->AddFileInfo(matchPath, -1, -1, false);
    auto mit = matchPath.rbegin();
    for (; mit != matchPath.rend() && *mit != '/'; mit++)
        ;
    diskStorage->SetDirLifecycle(matchPath.substr(0, matchPath.rend() - mit), lifecycle);

    std::shared_ptr<FileReader> reader =
        diskStorage->CreateFileReader(matchPath, GetTestPath(matchPath), openType).GetOrThrow();
    std::shared_ptr<FileNode> fileNode = reader->GetFileNode();

    if (getLockModeFunc) {
        bool lockMode = getLockModeFunc(fileNode);
        ASSERT_EQ(expectLock, lockMode);
    }
    if (checkFunc) {
        checkFunc(fileNode);
    }
}

void DiskStorageTest::TestStorageMetrics()
{
    std::shared_ptr<DiskStorage> diskStorage = PrepareDiskStorage();

    string text = "disk file";
    FileSystemTestUtil::CreateDiskFiles(GetTestPath("disk/"), 5, text);
    FileSystemTestUtil::CreateDiskFile(GetTestPath("disk/test_cache_path_match_with_pattern"), text);

    ASSERT_TRUE(diskStorage->CreateFileReader("disk/0", GetTestPath("disk/0"), FSOT_MEM).OK());
    std::shared_ptr<FileReader> mmapRReader =
        diskStorage->CreateFileReader("disk/1", GetTestPath("disk/1"), FSOT_MMAP).GetOrThrow();
    ASSERT_EQ(FSEC_OK, mmapRReader->Open());
    std::shared_ptr<FileReader> mmapRWReader =
        diskStorage->CreateFileReader("disk/2", GetTestPath("disk/2"), FSOT_MMAP).GetOrThrow();
    ASSERT_TRUE(diskStorage
                    ->CreateFileReader("disk/test_cache_path_match_with_pattern",
                                       GetTestPath("disk/test_cache_path_match_with_pattern"), FSOT_CACHE)
                    .OK());
    ASSERT_TRUE(diskStorage->CreateFileReader("disk/4", GetTestPath("disk/4"), FSOT_BUFFERED).OK());

    StorageMetrics metrics = diskStorage->GetMetrics();
    ASSERT_EQ((int64_t)text.size() * 5, metrics.GetTotalFileLength());
    ASSERT_EQ(5, metrics.GetTotalFileCount());
    ASSERT_EQ((int64_t)text.size(), metrics.GetFileLength(FSMG_LOCAL, FSFT_MEM));
    ASSERT_EQ((int64_t)text.size() * 2, metrics.GetFileLength(FSMG_LOCAL, FSFT_MMAP));
    ASSERT_EQ((int64_t)text.size(), metrics.GetFileLength(FSMG_LOCAL, FSFT_BLOCK));
    ASSERT_EQ((int64_t)text.size(), metrics.GetFileLength(FSMG_LOCAL, FSFT_BUFFERED));
    ASSERT_EQ(1, metrics.GetFileCount(FSMG_LOCAL, FSFT_MEM));
    ASSERT_EQ(2, metrics.GetFileCount(FSMG_LOCAL, FSFT_MMAP));
    ASSERT_EQ(1, metrics.GetFileCount(FSMG_LOCAL, FSFT_BLOCK));
    ASSERT_EQ(1, metrics.GetFileCount(FSMG_LOCAL, FSFT_BUFFERED));

    ASSERT_EQ(FSEC_OK, diskStorage->RemoveFileFromCache("disk/0")); // TODO: @qingran, storage or fs deal this?
    ASSERT_EQ(FSEC_OK, diskStorage->RemoveFileFromCache(
                           "disk/test_cache_path_match_with_pattern")); // TODO: @qingran, storage or fs deal this?
    ASSERT_EQ(FSEC_OK, diskStorage->RemoveFile("disk/0", GetTestPath("disk/0"), FenceContext::NoFence()));
    ASSERT_EQ(FSEC_OK,
              diskStorage->RemoveFile("disk/test_cache_path_match_with_pattern",
                                      GetTestPath("disk/test_cache_path_match_with_pattern"), FenceContext::NoFence()));

    metrics = diskStorage->GetMetrics();
    ASSERT_EQ((int64_t)text.size() * 3, metrics.GetTotalFileLength());
    ASSERT_EQ(3, metrics.GetTotalFileCount());
    ASSERT_EQ((int64_t)text.size() * 2, metrics.GetFileLength(FSMG_LOCAL, FSFT_MMAP));
    ASSERT_EQ((int64_t)text.size(), metrics.GetFileLength(FSMG_LOCAL, FSFT_BUFFERED));
    ASSERT_EQ(2, metrics.GetFileCount(FSMG_LOCAL, FSFT_MMAP));
    ASSERT_EQ(1, metrics.GetFileCount(FSMG_LOCAL, FSFT_BUFFERED));

    diskStorage->CleanCache();
    metrics = diskStorage->GetMetrics();
    ASSERT_EQ((int64_t)text.size() * 2, metrics.GetTotalFileLength());
    ASSERT_EQ(2, metrics.GetTotalFileCount());
    ASSERT_EQ((int64_t)text.size() * 2, metrics.GetFileLength(FSMG_LOCAL, FSFT_MMAP));
    ASSERT_EQ(2, metrics.GetFileCount(FSMG_LOCAL, FSFT_MMAP));

    mmapRReader.reset();
    ASSERT_TRUE(diskStorage->CreateFileReader("disk/1", GetTestPath("disk/1"), FSOT_MMAP).OK());
    metrics = diskStorage->GetMetrics();
    ASSERT_EQ((int64_t)text.size() * 2, metrics.GetFileLength(FSMG_LOCAL, FSFT_MMAP));
    ASSERT_EQ(2, metrics.GetFileCount(FSMG_LOCAL, FSFT_MMAP));

    mmapRWReader.reset();
    ASSERT_EQ(FSEC_OK, diskStorage->RemoveDirectoryFromCache("disk")); // TODO: @qingran, storage or fs deal this?
    ASSERT_EQ(FSEC_OK, diskStorage->RemoveDirectory("disk", GetTestPath("disk"), FenceContext::NoFence()));
    metrics = diskStorage->GetMetrics();
    ASSERT_EQ(0, metrics.GetTotalFileLength());
    ASSERT_EQ(0, metrics.GetTotalFileCount());
    ASSERT_FALSE(diskStorage->IsExistInCache("disk/5"));
}
void DiskStorageTest::TestStorageMetricsForCacheMiss()
{
    std::shared_ptr<DiskStorage> diskStorage = PrepareDiskStorage();

    FileSystemTestUtil::CreateDiskFiles(GetTestPath("disk/"), 1, "disk");
    std::shared_ptr<FileReader> mmapRReader =
        diskStorage->CreateFileReader("disk/0", GetTestPath("disk/0"), FSOT_MMAP).GetOrThrow();
    ASSERT_EQ(FSEC_OK, mmapRReader->Open());

    FileCacheMetrics metrics = diskStorage->GetMetrics().GetFileCacheMetrics();
    ASSERT_EQ(0, metrics.hitCount.getValue());
    ASSERT_EQ(1, metrics.missCount.getValue());
    ASSERT_EQ(0, metrics.replaceCount.getValue());
}

void DiskStorageTest::TestRemoveFile()
{
    std::shared_ptr<DiskStorage> diskStorage = PrepareDiskStorage();

    string text = "disk file";
    FileSystemTestUtil::CreateDiskFiles(GetTestPath("disk/"), 2, text);

    string path = "disk/0";
    std::shared_ptr<FileReader> reader =
        diskStorage->CreateFileReader(path, GetTestPath(path), FSOT_BUFFERED).GetOrThrow();
    ASSERT_EQ(FSEC_OK, reader->Open());
    ASSERT_EQ(FSEC_ERROR, diskStorage->RemoveFileFromCache(path));
    ASSERT_TRUE(diskStorage->IsExistInCache(path));
    reader.reset();
    ASSERT_EQ(FSEC_OK, diskStorage->RemoveFileFromCache(path));
    ASSERT_EQ(FSEC_OK, diskStorage->RemoveFile(path, GetTestPath(path), FenceContext::NoFence()));
    ASSERT_FALSE(diskStorage->IsExistInCache(path));
    ASSERT_EQ(FSEC_NOENT, diskStorage->RemoveFile(path, GetTestPath(path), FenceContext::NoFence()));

    string onDiskOnlyFilePath = "disk/1";
    ASSERT_EQ(FSEC_OK,
              diskStorage->RemoveFile(onDiskOnlyFilePath, GetTestPath(onDiskOnlyFilePath), FenceContext::NoFence()));
    ASSERT_EQ(FSEC_NOENT,
              diskStorage->RemoveFile(onDiskOnlyFilePath, GetTestPath(onDiskOnlyFilePath), FenceContext::NoFence()));
}

void DiskStorageTest::TestRemoveDirectory()
{
    std::shared_ptr<DiskStorage> diskStorage = PrepareDiskStorage();

    string diskDirPath = "disk";
    ASSERT_EQ(FSEC_OK, FslibWrapper::MkDir(GetTestPath(diskDirPath)).Code());
    ASSERT_EQ(FSEC_OK, diskStorage->RemoveDirectoryFromCache(diskDirPath));
    ASSERT_EQ(FSEC_OK, diskStorage->RemoveDirectory(diskDirPath, GetTestPath(diskDirPath), FenceContext::NoFence()));
    ASSERT_FALSE(FslibWrapper::IsExist(GetTestPath(diskDirPath)).GetOrThrow());
    ASSERT_EQ(FSEC_OK, diskStorage->RemoveDirectory(diskDirPath, GetTestPath(diskDirPath), FenceContext::NoFence()));

    // test rm file node in cache
    ASSERT_EQ(FSEC_OK, FslibWrapper::MkDir(GetTestPath(diskDirPath)).Code());
    string path = "disk/0";
    FileSystemTestUtil::CreateDiskFile(GetTestPath(path), "disk file");
    ASSERT_TRUE(diskStorage->CreateFileReader(path, GetTestPath(path), FSOT_BUFFERED).OK());
    ASSERT_TRUE(diskStorage->_fileNodeCache->IsExist(path));
    ASSERT_EQ(FSEC_OK, diskStorage->RemoveDirectoryFromCache(diskDirPath));
    ASSERT_EQ(FSEC_OK, diskStorage->RemoveDirectory(diskDirPath, GetTestPath(diskDirPath), FenceContext::NoFence()));
    ASSERT_FALSE(diskStorage->_fileNodeCache->IsExist(path));
    ASSERT_FALSE(diskStorage->IsExistInCache(diskDirPath));
}

void DiskStorageTest::CheckFileReader(std::shared_ptr<DiskStorage> storage, string logicalPath, FSOpenType type,
                                      string expectContext, bool fromLoadConfig, bool writable)
{
    SCOPED_TRACE(expectContext);
    std::shared_ptr<FileReader> reader;
    ReaderOption readerOption(type);
    readerOption.isWritable = writable;
    reader = storage->CreateFileReader(logicalPath, GetTestPath(logicalPath), readerOption).GetOrThrow();
    ASSERT_EQ(FSEC_OK, reader->Open());
    ASSERT_TRUE(reader);
    size_t expectSize = expectContext.size();
    ASSERT_EQ(type, reader->GetOpenType());
    ASSERT_EQ(expectSize, reader->GetLength());
    ASSERT_EQ(logicalPath, reader->GetLogicalPath());
    uint8_t buffer[1024];
    ASSERT_EQ(expectSize, reader->Read(buffer, expectSize).GetOrThrow());
    ASSERT_TRUE(memcmp(buffer, expectContext.data(), expectSize) == 0);

    std::shared_ptr<FileNodeCache> fileNodeCache = storage->_fileNodeCache;
    std::shared_ptr<FileNode> fileNode = fileNodeCache->Find(logicalPath);

    ASSERT_EQ(FSEC_OK, reader->Close());
}

void DiskStorageTest::CreateLoadConfigList(LoadConfigList& loadConfigList)
{
    string jsonStr = R"(
    {
        "load_config" :
        [
        {
            "file_patterns" : ["test_path_match_with_pattern"],
            "load_strategy" : "mmap"
        },
        {
            "file_patterns" : ["test_lock_path_match_with_pattern"],
            "load_strategy" : "mmap",
            "load_strategy_param" : {
                "lock" : true
            }
        },
        {
            "name" : "local_cache",
            "file_patterns" : ["test_cache_path_match_with_pattern"],
            "load_strategy" : "cache"
        },
        {
            "name" : "global_cache",
            "file_patterns" : ["test_global_cache_path_match_with_pattern"],
            "load_strategy" : "cache",
            "load_strategy_param" : {
                "global_cache" : true
            }
        }
        ]
    })";

    loadConfigList = LoadConfigListCreator::CreateLoadConfigListFromJsonString(jsonStr);
}

std::shared_ptr<DiskStorage> DiskStorageTest::PrepareDiskStorage()
{
    LoadConfigList loadConfigList;
    CreateLoadConfigList(loadConfigList);
    std::shared_ptr<FileSystemOptions> options(new FileSystemOptions);
    options->fileSystemIdentifier = "test";
    options->loadConfigList = loadConfigList;
    options->fileBlockCacheContainer = _fileBlockCacheContainer;
    std::shared_ptr<DiskStorage> diskStorage(new DiskStorage(false, _memoryController, /*entryTable=*/nullptr));
    EXPECT_EQ(FSEC_OK, diskStorage->Init(options));
    diskStorage->_fileSystemLock = &_mutex;
    return diskStorage;
}

void DiskStorageTest::TestPackageFileAndSharedMMap()
{
    string rootPath = GET_TEMP_DATA_PATH();
    PackageFileUtil::CreatePackageFile(rootPath, "s1",
                                       "summary/data:xyz:MERGE;"
                                       "index/str/offset:offset:MERGE#index/str/data:12345:MERGE;"
                                       "index/aitheta/data:aitheta:AI;"
                                       "attribute/long/patch.0:patch;",
                                       "deletionmap:attribute:index");
    string jsonStr = R"({
        "load_config" :
            [{ "file_patterns" : ["package_file.__data__.MERGE.[0-9]+"], "remote": true,
               "load_strategy" : "mmap", "load_strategy_param": { "lock": true } },
             { "file_patterns" : ["package_file.__data__.AI.[0-9]+"], "remote": true,
               "load_strategy" : "mmap", "load_strategy_param": { "lock": false } }
            ]
    })";
    std::shared_ptr<LogicalFileSystem> fs = CreateFSWithLoadConfig(rootPath, jsonStr);
    ASSERT_EQ(FSEC_OK, fs->MountSegment("s1"));

    util::BlockMemoryQuotaControllerPtr memoryController = fs->TEST_GetInputStorage()->_memController;
    fs->CleanCache();
    ASSERT_EQ(0, memoryController->GetUsedQuota());

    ASSERT_EQ(4096 + 5, FslibWrapper::GetFileLength(rootPath + "/s1/package_file.__data__.MERGE.1").GetOrThrow());
    std::shared_ptr<FileReader> reader1 = CheckPackageFile(fs, "s1/summary/data", "xyz", FSOT_MMAP);
    // size_t usedQuota = memoryController->GetUsedQuota();
    ASSERT_EQ(4 * 1024 * 1024, memoryController->GetUsedQuota());
    std::shared_ptr<FileReader> reader2 = CheckPackageFile(fs, "s1/index/str/offset", "offset", FSOT_BUFFERED);
    // package file length = 6 align to 4096 + tail 5, quota align to 4M
    ASSERT_EQ(4 * 1024 * 1024, memoryController->GetUsedQuota());
    std::shared_ptr<FileReader> reader3 = CheckPackageFile(fs, "s1/index/str/data", "12345", FSOT_MMAP);
    ASSERT_EQ(4 * 1024 * 1024, memoryController->GetUsedQuota());
    std::shared_ptr<FileReader> reader4 = CheckPackageFile(fs, "s1/index/aitheta/data", "aitheta", FSOT_MEM);
    std::shared_ptr<FileReader> reader5 = CheckPackageFile(fs, "s1/attribute/long/patch.0", "patch", FSOT_BUFFERED);
    ASSERT_EQ(4 * 1024 * 1024, memoryController->GetUsedQuota());
    ASSERT_TRUE(fs->IsExist("s1/attribute").GetOrThrow());
    ASSERT_TRUE(fs->IsExist("s1/index").GetOrThrow());
    ASSERT_TRUE(fs->IsExist("s1/deletionmap").GetOrThrow());

    auto fileNode1 = std::dynamic_pointer_cast<MmapFileNode>(reader1->GetFileNode());
    ASSERT_TRUE(fileNode1->_dependFileNode);
    auto fileNode2 = std::dynamic_pointer_cast<MmapFileNode>(reader2->GetFileNode());
    auto fileNode3 = std::dynamic_pointer_cast<MmapFileNode>(reader3->GetFileNode());
    ASSERT_TRUE(fileNode2->_dependFileNode);
    ASSERT_NE(fileNode1->_dependFileNode, fileNode2->_dependFileNode);
    ASSERT_EQ(fileNode2->_dependFileNode, fileNode3->_dependFileNode);
    ASSERT_EQ(3, fileNode2->_dependFileNode.use_count());
    std::weak_ptr<FileNode> sharedPackageFile = fileNode2->_dependFileNode;
    ASSERT_EQ(3, sharedPackageFile.use_count());
    ASSERT_EQ(FSEC_OK, fileNode2->Close());
    ASSERT_EQ(2, sharedPackageFile.use_count());
    ASSERT_EQ(FSEC_OK, fileNode3->Close());
    ASSERT_EQ(1, sharedPackageFile.use_count());
    fs->CleanCache();
    ASSERT_TRUE(sharedPackageFile.expired());

    ASSERT_EQ(FSOT_MEM, reader4->GetOpenType());
    ASSERT_EQ(FSOT_MEM, reader4->GetFileNode()->GetOpenType());
    ASSERT_EQ(FSOT_BUFFERED, reader5->GetOpenType());
    ASSERT_EQ(FSOT_BUFFERED, reader5->GetFileNode()->GetOpenType());
}

std::shared_ptr<LogicalFileSystem> DiskStorageTest::CreateFSWithLoadConfig(const std::string& rootPath,
                                                                           const std::string& loadConfigJsonStr)
{
    FileSystemOptions options = FileSystemOptions::Offline();
    options.loadConfigList = LoadConfigListCreator::CreateLoadConfigListFromJsonString(loadConfigJsonStr);
    options.fileBlockCacheContainer = _fileBlockCacheContainer;
    options.memoryQuotaController = util::MemoryQuotaControllerCreator::CreatePartitionMemoryController();
    std::shared_ptr<LogicalFileSystem> fs(new LogicalFileSystem("", rootPath, nullptr));
    THROW_IF_FS_ERROR(fs->Init(options), "");
    return fs;
}

void DiskStorageTest::TestPackageFileAndSharedMMapWithBufferedFile()
{
    string rootPath = GET_TEMP_DATA_PATH();
    PackageFileUtil::CreatePackageFile(rootPath, "s1",
                                       "index/str/offset:offset:MERGE#index/str/data:12345:MERGE;"
                                       "summary/data:xyz:MERGE;"
                                       "attribute/long/patch.0:patch;",
                                       "deletionmap:attribute:index");
    string jsonStr = R"({
        "load_config" :
            [{ "file_patterns" : ["package_file.__data__.MERGE.[0-9]+"],
               "load_strategy" : "mmap", "load_strategy_param": { "lock": true } }
            ]
    })";
    std::shared_ptr<LogicalFileSystem> fs = CreateFSWithLoadConfig(rootPath, jsonStr);
    ASSERT_EQ(FSEC_OK, fs->MountSegment("s1"));

    {
        auto fileReader = CheckPackageFile(fs, "s1/index/str/offset", "offset", FSOT_BUFFERED);
        char buffer[10] = {0};
        ASSERT_EQ(6, fileReader->Read(buffer, 6, 0).GetOrThrow());
        ASSERT_STREQ("offset", buffer);
    }
    {
        CheckPackageFile(fs, "s1/index/str/offset", "offset", FSOT_MMAP);
    }
    // storage->CleanCache();
    // {
    //     std::shared_ptr<FileReader> reader1 = CheckPackageFile(storage, "s1/index/str/offset", "offset",
    //     FSOT_BUFFERED); std::shared_ptr<FileReader> reader2 = CheckPackageFile(storage, "s1/index/str/data", "12345",
    //     FSOT_MMAP); auto fileNode1 = std::dynamic_pointer_cast<BufferedFileNode>(reader1->GetFileNode()); auto
    //     fileNode2 = std::dynamic_pointer_cast<MmapFileNode>(reader2->GetFileNode());
    //     ASSERT_TRUE(fileNode1->_dependFileNode); ASSERT_EQ(fileNode1->_dependFileNode, fileNode2->_dependFileNode);
    //     ASSERT_EQ(2, fileNode1->_dependFileNode.use_count());
    // }
    fs->CleanCache();
}

void DiskStorageTest::TestCleanSharePackageFileWithFileOpened()
{
    PackageFileUtil::CreatePackageFile(GET_TEMP_DATA_PATH(), "s1", "index/str/offset:offset#index/str/data:12345;", "");
    string jsonStr = R"({
            "load_config" :
                [{ "file_patterns" : ["package_file.__data__[0-9]+"], "remote": true,
                   "load_strategy" : "mmap", "load_strategy_param": { "lock": true } }]
        })";
    auto GetMMapFileNode = [&](const std::shared_ptr<LogicalFileSystem>& fs, const string& fileName,
                               const string& content) {
        return std::dynamic_pointer_cast<MmapFileNode>(
            CheckPackageFile(fs, fileName, content, FSOT_MMAP)->GetFileNode());
    };

    {
        std::shared_ptr<LogicalFileSystem> fs = CreateFSWithLoadConfig(GET_TEMP_DATA_PATH(), jsonStr);
        ASSERT_EQ(FSEC_OK, fs->MountSegment("s1"));
        auto fileNode1 = GetMMapFileNode(fs, "s1/index/str/offset", "offset");
        ASSERT_TRUE(fileNode1->_dependFileNode);
        {
            auto fileNode2 = GetMMapFileNode(fs, "s1/index/str/data", "12345");
            ASSERT_EQ(fileNode1->_dependFileNode, fileNode2->_dependFileNode);
        }
        ASSERT_EQ(3, fileNode1->_dependFileNode.use_count());
        {
            auto fileNode3 = GetMMapFileNode(fs, "s1/index/str/data", "12345");
            ASSERT_EQ(fileNode1->_dependFileNode, fileNode3->_dependFileNode);
        }
        auto fileNode4 = GetMMapFileNode(fs, "s1/index/str/offset", "offset");
        ASSERT_EQ(fileNode1->_dependFileNode, fileNode4->_dependFileNode);

        ASSERT_EQ(3, fileNode1->_dependFileNode.use_count());
        fs->CleanCache();
        {
            auto fileNode5 = GetMMapFileNode(fs, "s1/index/str/data", "12345");
            ASSERT_EQ(fileNode1->_dependFileNode, fileNode5->_dependFileNode);
        }
        auto fileNode6 = GetMMapFileNode(fs, "s1/index/str/offset", "offset");
        ASSERT_EQ(fileNode1->_dependFileNode, fileNode6->_dependFileNode);
    }
    {
        std::shared_ptr<LogicalFileSystem> fs = CreateFSWithLoadConfig(GET_TEMP_DATA_PATH(), jsonStr);
        ASSERT_EQ(FSEC_OK, fs->MountSegment("s1"));
        std::weak_ptr<FileNode> sharedFileNode;
        {
            auto fileNode1 = GetMMapFileNode(fs, "s1/index/str/offset", "offset");
            ASSERT_TRUE(fileNode1->_dependFileNode);
            sharedFileNode = fileNode1->_dependFileNode;
        }
        {
            auto fileNode2 = GetMMapFileNode(fs, "s1/index/str/data", "12345");
            ASSERT_EQ(sharedFileNode.lock(), fileNode2->_dependFileNode);
        }
    }
    {
        std::shared_ptr<LogicalFileSystem> fs = CreateFSWithLoadConfig(GET_TEMP_DATA_PATH(), jsonStr);
        ASSERT_EQ(FSEC_OK, fs->MountSegment("s1"));
        std::weak_ptr<FileNode> sharedFileNode;
        {
            auto fileNode1 = GetMMapFileNode(fs, "s1/index/str/offset", "offset");
            ASSERT_TRUE(fileNode1->_dependFileNode);
            sharedFileNode = fileNode1->_dependFileNode;
        }
        fs->CleanCache();
        ASSERT_TRUE(sharedFileNode.expired());
    }
}

std::shared_ptr<FileReader> DiskStorageTest::CheckPackageFile(const std::shared_ptr<LogicalFileSystem>& fs,
                                                              const string& logicalFilePath,
                                                              const string& expectContent, FSOpenType openType)
{
    std::shared_ptr<FileReader> reader = fs->CreateFileReader(logicalFilePath, openType).GetOrThrow();
    EXPECT_EQ(expectContent.size(), fs->GetFileLength(logicalFilePath).GetOrThrow());
    EXPECT_EQ(expectContent, reader->TEST_Load());
    EXPECT_EQ(logicalFilePath, reader->GetLogicalPath());
    return reader;
};

void DiskStorageTest::TestMmapInDfs()
{
    std::string rootPath = PathUtil::NormalizePath(GET_TEMP_DATA_PATH());
    string jsonStr = R"( {
        "load_config" : [{
            "file_patterns" : [".*"],
            "load_strategy" : "mmap",
            "remote": true,
            "load_strategy_param": {
                "slice" : 8192,
                "interval" : 10,
                "lock":true
             }
        }]
    } )";

    std::shared_ptr<FileSystemOptions> options(new FileSystemOptions);
    options->loadConfigList = LoadConfigListCreator::CreateLoadConfigListFromJsonString(jsonStr);
    std::shared_ptr<DiskStorage> storage(new DiskStorage(false, _memoryController, nullptr));
    ASSERT_EQ(FSEC_OK, storage->Init(options));
    storage->TEST_EnableSupportMmap(false);

    string simpleContent = "helloworld";
    ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicStore(PathUtil::JoinPath(rootPath, "file"), simpleContent).Code());
    ReaderOption readerOption(FSOT_LOAD_CONFIG);
    readerOption.isWritable = false;
    readerOption.fileLength = simpleContent.size();
    readerOption.fileOffset = -1;
    std::shared_ptr<FileNode> fileNode =
        storage->CreateFileNode("file", PathUtil::JoinPath(rootPath, "file"), readerOption).GetOrThrow();
    ASSERT_TRUE(fileNode);
    EXPECT_EQ(FSFT_MEM, fileNode->GetType());
    std::shared_ptr<MemFileNode> memFileNode = std::dynamic_pointer_cast<MemFileNode>(fileNode);
    ASSERT_TRUE(memFileNode);
    auto strategy = memFileNode->_loadStrategy;
    ASSERT_TRUE(strategy);
    ASSERT_FALSE(strategy->_enableLoadSpeedLimit);

    string content;
    size_t len = 8192 * 2 + 123;
    for (size_t i = 0; i < len; ++i) {
        content += static_cast<char>(i);
    }
    ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicStore(PathUtil::JoinPath(rootPath, "largefile"), content).Code());

    readerOption = ReaderOption(FSOT_LOAD_CONFIG);
    readerOption.fileLength = len;
    std::shared_ptr<FileReader> reader =
        storage->CreateFileReader("largefile", PathUtil::JoinPath(rootPath, "largefile"), readerOption).GetOrThrow();
    ASSERT_EQ(FSEC_OK, reader->Open());
    EXPECT_EQ(FSFT_MEM, reader->GetFileNode()->GetType());
    EXPECT_EQ(content.size(), reader->GetLength());
    auto base = reader->GetBaseAddress();
    ASSERT_TRUE(base);
    EXPECT_EQ(content.size(), reader->GetLength());
    EXPECT_EQ(content, string(static_cast<const char*>(base), content.size()));
}

void DiskStorageTest::TestMmapInDCache()
{
    std::string rootPath = PathUtil::NormalizePath(GET_TEMP_DATA_PATH());
    string jsonStr = R"( {
        "load_config" : [ {
            "file_patterns" : [".*"],
            "load_strategy" : "mmap",
            "remote": true,
            "load_strategy_param": {
                "slice" : 4096,
                "interval" : 20,
                 "lock":true
             }
        }]
    } )";

    std::shared_ptr<FileSystemOptions> options(new FileSystemOptions);
    options->loadConfigList = LoadConfigListCreator::CreateLoadConfigListFromJsonString(jsonStr);
    std::shared_ptr<DiskStorage> storage(new DiskStorage(false, _memoryController, nullptr));
    ASSERT_EQ(FSEC_OK, storage->Init(options));

    string simpleContent = "helloworld";
    ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicStore(PathUtil::JoinPath(rootPath, "file"), simpleContent).Code());
    ReaderOption readerOption(FSOT_LOAD_CONFIG);
    readerOption.isWritable = false;
    readerOption.fileLength = simpleContent.size();
    readerOption.fileOffset = -1;
    auto fileNode = storage->CreateFileNode("file", PathUtil::JoinPath(rootPath, "file"), readerOption).GetOrThrow();
    ASSERT_TRUE(fileNode);
    auto mmapFileNode = std::dynamic_pointer_cast<MmapFileNode>(fileNode);
    ASSERT_TRUE(mmapFileNode);
    auto strategy = mmapFileNode->_loadStrategy;
    ASSERT_TRUE(strategy);
    ASSERT_TRUE(strategy->_enableLoadSpeedLimit);
    EXPECT_TRUE(*strategy->_enableLoadSpeedLimit);
    EXPECT_EQ(4096u, strategy->GetSlice());
    EXPECT_EQ(20u, strategy->GetInterval());
    EXPECT_TRUE(strategy->IsLock());

    string content;
    size_t len = 8192 * 2 + 123;
    for (size_t i = 0; i < len; ++i) {
        content += static_cast<char>(i);
    }
    ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicStore(PathUtil::JoinPath(rootPath, "largefile"), content).Code());

    readerOption = ReaderOption(FSOT_LOAD_CONFIG);
    readerOption.fileLength = len;
    std::shared_ptr<FileReader> reader =
        storage->CreateFileReader("largefile", PathUtil::JoinPath(rootPath, "largefile"), readerOption).GetOrThrow();
    ASSERT_EQ(FSEC_OK, reader->Open());
    EXPECT_EQ(FSFT_MMAP_LOCK, reader->GetFileNode()->GetType());
    EXPECT_EQ(content.size(), reader->GetLength());
    auto base = reader->GetBaseAddress();
    ASSERT_TRUE(base);
    EXPECT_EQ(content.size(), reader->GetLength());
    EXPECT_EQ(content, string(static_cast<const char*>(base), content.size()));
}

void DiskStorageTest::TestGetLoadConfigOpenType()
{
    std::shared_ptr<FileSystemOptions> options(new FileSystemOptions);
    string filePath = "file";

    // ""
    options->loadConfigList = LoadConfigListCreator::CreateLoadConfigList("");
    std::shared_ptr<DiskStorage> storage(new DiskStorage(false, _memoryController, nullptr));
    ReaderOption op(FSOT_UNKNOWN);
    ASSERT_EQ(FSEC_OK, storage->Init(options));
    ASSERT_EQ(FSEC_OK, storage->GetLoadConfigOpenType(filePath, filePath, op));
    ASSERT_EQ(FSOT_MMAP, op.openType);

    // READ_MODE_MMAP
    options->loadConfigList = LoadConfigListCreator::CreateLoadConfigList(READ_MODE_MMAP);
    storage.reset(new DiskStorage(false, _memoryController, nullptr));
    ASSERT_EQ(FSEC_OK, storage->Init(options));
    ASSERT_EQ(FSEC_OK, storage->GetLoadConfigOpenType(filePath, filePath, op));
    ASSERT_EQ(FSOT_MMAP, op.openType);

    // READ_MODE_CACHE
    options->loadConfigList = LoadConfigListCreator::CreateLoadConfigList(READ_MODE_CACHE);
    storage.reset(new DiskStorage(false, _memoryController, nullptr));
    ASSERT_EQ(FSEC_OK, storage->Init(options));
    ASSERT_EQ(FSEC_OK, storage->GetLoadConfigOpenType(filePath, filePath, op));
    ASSERT_EQ(FSOT_CACHE, op.openType);
}
}} // namespace indexlib::file_system
