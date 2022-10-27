#include "indexlib/misc/exception.h"
#include "indexlib/file_system/load_config/load_config_list.h"
#include "indexlib/file_system/load_config/load_strategy.h"
#include "indexlib/util/path_util.h"
#include "indexlib/file_system/file_reader.h"
#include "indexlib/file_system/buffered_file_reader.h"
#include "indexlib/file_system/file_node.h"
#include "indexlib/file_system/test/local_storage_unittest.h"
#include "indexlib/file_system/test/file_system_test_util.h"
#include "indexlib/file_system/test/load_config_list_creator.h"
#include "indexlib/util/memory_control/memory_quota_controller_creator.h"
#include "indexlib/file_system/file_block_cache.h"
#include "indexlib/util/cache/block_cache.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, LocalStorageTest);

class MockLoadStrategy : public LoadStrategy
{
public:
    MockLoadStrategy()
        :mName("READ_MODE_MOCK")
    {}
    ~MockLoadStrategy() {}
 
    const string& GetLoadStrategyName() const
    { return mName; }
    void Jsonize(legacy::Jsonizable::JsonWrapper& json) {}
    bool EqualWith(const LoadStrategyPtr& loadStrategy) const { return true; }
    void Check() {}
private:
    string mName;
};

DEFINE_SHARED_PTR(MockLoadStrategy);

LocalStorageTest::LocalStorageTest()
{
}

LocalStorageTest::~LocalStorageTest()
{
}

void LocalStorageTest::CaseSetUp()
{
    mRootDir = GET_TEST_DATA_PATH();
    mMemoryController = MemoryQuotaControllerCreator::CreateBlockMemoryController();
    mFileBlockCache.reset(new FileBlockCache());
    mFileBlockCache->Init(128 * 4 * 1024, 4 * 1024,
                          MemoryQuotaControllerCreator::CreateSimpleMemoryController());
}

void LocalStorageTest::CaseTearDown()
{
}

void LocalStorageTest::TestLocalInit()
{
    MockLoadStrategyPtr mockLoadStrategy(new MockLoadStrategy);
    LoadConfig loadConfig;
    loadConfig.SetLoadStrategyPtr(mockLoadStrategy);
    LoadConfigList loadConfigList;
    loadConfigList.PushFront(loadConfig);
    FileSystemOptions options;
    options.loadConfigList = loadConfigList;
    LocalStorage localStorage("", mMemoryController);

    ASSERT_THROW(localStorage.Init(options), UnSupportedException);
}

void LocalStorageTest::TestMmapFileReader()
{
    LocalStoragePtr localStorage = PrepareLocalStorage();
    
    string mmapPath = mRootDir + "mmap_file";
    FileSystemTestUtil::CreateDiskFile(mmapPath, "abcd");

    FileReaderPtr reader0 =
        localStorage->CreateFileReader(mmapPath, FSOT_MMAP);
    reader0->Open();
    FileReaderPtr reader1 =
        localStorage->CreateFileReader(mmapPath, FSOT_MMAP);
    reader1->Open();
    ASSERT_EQ(FSOT_MMAP, reader0->GetOpenType());
    ASSERT_EQ(FSOT_MMAP, reader1->GetOpenType());

    uint8_t* data0 = (uint8_t*) reader0->GetBaseAddress();
    uint8_t* data1 = (uint8_t*) reader1->GetBaseAddress();
    ASSERT_EQ(data0, data1);
    data0[2] = '0';

    uint8_t buffer[1024];
    reader1->Read(buffer, 2, 1);
    ASSERT_EQ('b', buffer[0]);
    ASSERT_EQ('0', buffer[1]);
}

void LocalStorageTest::TestBlockFileReader()
{
    LocalStoragePtr localStorage = PrepareLocalStorage();
    
    FileNodeCachePtr fileNodeCache = localStorage->mFileNodeCache;
    string blockFilePath = mRootDir + "test_cache_path_match_with_pattern";
    FileSystemTestUtil::CreateDiskFile(blockFilePath, "abcd");

    FileReaderPtr reader0 =
        localStorage->CreateFileReader(blockFilePath, FSOT_CACHE);
    reader0->Open();
    FileNodePtr fileNode0 = fileNodeCache->Find(blockFilePath);
    ASSERT_TRUE(fileNode0);
    ASSERT_EQ(3, fileNode0.use_count());
    FileReaderPtr reader1 =
        localStorage->CreateFileReader(blockFilePath, FSOT_CACHE);
    reader1->Open();
    ASSERT_EQ(FSOT_CACHE, reader0->GetOpenType());
    ASSERT_EQ(FSOT_CACHE, reader1->GetOpenType());
    ASSERT_EQ(4, fileNode0.use_count());

    ASSERT_FALSE(reader0->GetBaseAddress());
    ASSERT_FALSE(reader1->GetBaseAddress());

    string defaultFilePath = mRootDir + "notsupportdefault";
    FileSystemTestUtil::CreateDiskFile(defaultFilePath, "abcd");
    ASSERT_THROW(localStorage->CreateFileReader(defaultFilePath, FSOT_CACHE), 
                 UnSupportedException);

}

void LocalStorageTest::TestGlobalBlockFileReader()
{
    LocalStoragePtr localStorage = PrepareLocalStorage();
    
    FileNodeCachePtr fileNodeCache = localStorage->mFileNodeCache;
    string blockFilePath = mRootDir + "test_global_cache_path_match_with_pattern";
    FileSystemTestUtil::CreateDiskFile(blockFilePath, "abcd");

    FileReaderPtr reader =
        localStorage->CreateFileReader(blockFilePath, FSOT_CACHE);
    reader->Open();
    FileNodePtr fileNode = fileNodeCache->Find(blockFilePath);
    ASSERT_TRUE(fileNode);
    ASSERT_EQ(3, fileNode.use_count());
    ASSERT_EQ(FSOT_CACHE, reader->GetOpenType());

    ASSERT_EQ(0, mFileBlockCache->GetBlockCache()->GetTotalAccessCount());

    char buffer[32];
    reader->Read(buffer, 4);
    ASSERT_EQ("abcd", string(buffer, 4));

    ASSERT_EQ(1, mFileBlockCache->GetBlockCache()->GetTotalAccessCount());
}

void LocalStorageTest::TestInMemFileReader()
{
    LocalStoragePtr localStorage = PrepareLocalStorage();
    
    FileNodeCachePtr fileNodeCache = localStorage->mFileNodeCache;
    string inMemPath = mRootDir + "in_mem_file";
    FileSystemTestUtil::CreateDiskFile(inMemPath, "abcd");

    FileReaderPtr reader0 =
        localStorage->CreateFileReader(inMemPath, FSOT_IN_MEM);
    reader0->Open();
    FileNodePtr fileNode = fileNodeCache->Find(inMemPath);
    ASSERT_EQ(3, fileNode.use_count());
    FileReaderPtr reader1 =
        localStorage->CreateFileReader(inMemPath, FSOT_IN_MEM);
    reader1->Open();
    ASSERT_EQ(FSOT_IN_MEM, reader0->GetOpenType());
    ASSERT_EQ(FSOT_IN_MEM, reader1->GetOpenType());

    uint8_t* data0 = (uint8_t*) reader0->GetBaseAddress();
    uint8_t* data1 = (uint8_t*) reader1->GetBaseAddress();
    ASSERT_EQ(data0, data1);
    data0[2] = '0';

    uint8_t buffer[1024];
    reader1->Read(buffer, 2, 1);
    ASSERT_EQ('b', buffer[0]);
    ASSERT_EQ('0', buffer[1]);
    fileNode = fileNodeCache->Find(inMemPath);
    ASSERT_EQ(4, fileNode.use_count());
}

void LocalStorageTest::TestBufferedFileReader()
{
    LocalStoragePtr localStorage = PrepareLocalStorage();

    string bufferedPath = mRootDir + "buffered_file";
    FileSystemTestUtil::CreateDiskFile(bufferedPath, "abcd");
    FileReaderPtr reader = 
        localStorage->CreateFileReader(bufferedPath, FSOT_BUFFERED);
    reader->Open();
    ASSERT_EQ(FSOT_BUFFERED, reader->GetOpenType());
    ASSERT_EQ((size_t)4, reader->GetLength());
    ASSERT_EQ(bufferedPath, reader->GetPath());
    ASSERT_FALSE(reader->GetBaseAddress());

    uint8_t buffer[1024];
    ASSERT_EQ((size_t)4, reader->Read(buffer, 4));
    ASSERT_TRUE(memcmp("abcd", buffer, 4) == 0);

    ASSERT_EQ((size_t)2, reader->Read(buffer, 2, 2));
    ASSERT_TRUE(memcmp("cd", buffer, 2) == 0);

    ASSERT_THROW(reader->Read(4, 0), UnSupportedException);

    BufferedFileReaderPtr bufferedReader = 
        DYNAMIC_POINTER_CAST(BufferedFileReader, reader);
    ASSERT_TRUE(bufferedReader);
}

void LocalStorageTest::TestBufferedFileWriter()
{
    InnerTestBufferedFileWriter(true);
    InnerTestBufferedFileWriter(false);
}

void LocalStorageTest::InnerTestBufferedFileWriter(bool atomicWrite)
{
    TearDown();
    SetUp();
    LocalStoragePtr localStorage = PrepareLocalStorage();
    string filePath = mRootDir + "local/file";
    ASSERT_THROW(localStorage->CreateFileWriter(
                     filePath, FSWriterParam(atomicWrite)),
                 NonExistException);
    localStorage->MakeDirectory(mRootDir + "local");
    FileWriterPtr fileWriter = 
        localStorage->CreateFileWriter(filePath, FSWriterParam(atomicWrite));
    ASSERT_TRUE(fileWriter);
    uint8_t buffer[] = "abcd";
    fileWriter->Write(buffer, 4);
    if (atomicWrite)
    {
        ASSERT_FALSE(FileSystemWrapper::IsExist(filePath));
        ASSERT_TRUE(FileSystemWrapper::IsExist(filePath + TEMP_FILE_SUFFIX));
    }
    else
    {
        ASSERT_TRUE(FileSystemWrapper::IsExist(filePath));
    }
    fileWriter->Close();
    ASSERT_TRUE(FileSystemWrapper::IsExist(filePath));
}


void LocalStorageTest::TestUnknownFileReader()
{
    LocalStoragePtr localStorage = PrepareLocalStorage();

    string filePath = mRootDir + "unknown_file";
    FileSystemTestUtil::CreateDiskFile(filePath, "abcd");
    ASSERT_THROW(localStorage->CreateFileReader(filePath, FSOT_UNKNOWN),
                 UnSupportedException);
}

void LocalStorageTest::TestNoLoadConfigListWithCacheHit()
{
    LocalStoragePtr localStorage(new LocalStorage(mRootDir, mMemoryController));
    localStorage->Init(FileSystemOptions());
    
    string inMemPath = mRootDir + "in_mem";
    string mmapRWPath = mRootDir + "mmap_rw";
    string loadConfigPath = mRootDir + "load_config";

    ASSERT_ANY_THROW(CheckFileReader(localStorage, inMemPath, FSOT_IN_MEM, "in memory"));
    ASSERT_ANY_THROW(CheckFileReader(localStorage, mmapRWPath, FSOT_MMAP, "mmap for rw"));

    FileSystemTestUtil::CreateDiskFile(inMemPath, "in memory");
    FileSystemTestUtil::CreateDiskFile(mmapRWPath, "mmap for rw");
    FileSystemTestUtil::CreateDiskFile(loadConfigPath, "load config");

    // test for first create file reader
    CheckFileReader(localStorage, inMemPath, FSOT_IN_MEM, "in memory");
    CheckFileReader(localStorage, mmapRWPath, FSOT_MMAP, "mmap for rw");
    CheckFileReader(localStorage, loadConfigPath, FSOT_LOAD_CONFIG, "load config");

    // test for cache hit
    FileSystemWrapper::DeleteFile(inMemPath);
    FileSystemWrapper::DeleteFile(mmapRWPath);
    CheckFileReader(localStorage, inMemPath, FSOT_IN_MEM, "in memory");
    CheckFileReader(localStorage, mmapRWPath, FSOT_MMAP, "mmap for rw");
    CheckFileReader(localStorage, loadConfigPath, FSOT_LOAD_CONFIG, "load config");

    // cache hit will use same base address
    FileReaderPtr reader0 =
        localStorage->CreateFileReader(mmapRWPath, FSOT_MMAP);
    reader0->Open();
    FileReaderPtr reader1 =
        localStorage->CreateFileReader(mmapRWPath, FSOT_MMAP);
    reader1->Open();
    ASSERT_EQ(FSOT_MMAP, reader0->GetOpenType());
    ASSERT_EQ(FSOT_MMAP, reader1->GetOpenType());
    uint8_t* data0 = (uint8_t*) reader0->GetBaseAddress();
    uint8_t* data1 = (uint8_t*) reader1->GetBaseAddress();
    ASSERT_EQ(data0, data1);
    data0[5] = '0';
}

void LocalStorageTest::TestNoLoadConfigListWithCacheMiss()
{
    LocalStoragePtr localStorage(new LocalStorage(mRootDir, mMemoryController));
    localStorage->Init(FileSystemOptions());
    
    string inMemPath = mRootDir + "in_mem";
    // string mmapRPath = mRootDir + "mmap_r";
    string mmapRWPath = mRootDir + "mmap_rw";
    string loadConfigPath = mRootDir + "load_config";

    FileSystemTestUtil::CreateDiskFile(inMemPath, "in memory");
    // FileSystemTestUtil::CreateDiskFile(mmapRPath, "mmap for r");
    FileSystemTestUtil::CreateDiskFile(mmapRWPath, "mmap for rw");

    // test for first create file reader
    CheckFileReader(localStorage, inMemPath, FSOT_IN_MEM, "in memory");
    // CheckFileReader(localStorage, mmapRPath, FSOT_MMAP_R, "mmap for r");
    CheckFileReader(localStorage, mmapRWPath, FSOT_MMAP, "mmap for rw");

    // test for cache Miss
    // CheckFileReader(localStorage, inMemPath, FSOT_MMAP_R, "in memory");
    CheckFileReader(localStorage, mmapRWPath, FSOT_MMAP, "mmap for rw");
}

void LocalStorageTest::TestLoadConfigListWithNoCache()
{
    LocalStoragePtr localStorage = PrepareLocalStorage();
    
    string noMatchPath = mRootDir + "miss_file";
    string matchPath = mRootDir + "test_path_match_with_pattern";

    FileSystemTestUtil::CreateDiskFile(noMatchPath, "no match file");
    FileSystemTestUtil::CreateDiskFile(matchPath, "match file");

    // Open type with FSOT_MMAP_R
    CheckFileReader(localStorage, noMatchPath, FSOT_MMAP, "no match file");
    CheckFileReader(localStorage, matchPath, FSOT_MMAP, "match file", true);

    // Open type with FSOT_LOAD_CONFIG
    localStorage->CleanCache();
    FileNodeCachePtr fileNodeCache = localStorage->mFileNodeCache;
    ASSERT_EQ((size_t)0, fileNodeCache->GetFileCount());
    CheckFileReader(localStorage, noMatchPath, FSOT_LOAD_CONFIG, "no match file", true);
    FileReaderPtr reader = 
        localStorage->CreateFileReader(matchPath, FSOT_LOAD_CONFIG);
    reader->Open();
    ASSERT_EQ(FSOT_LOAD_CONFIG, reader->GetOpenType());
    FileNodePtr fileNode = fileNodeCache->Find(matchPath);
    ASSERT_TRUE(fileNode->GetType() == FSFT_MMAP);
    reader->Close();
}

void LocalStorageTest::TestLoadConfigListWithCacheHit()
{
    LocalStoragePtr localStorage = PrepareLocalStorage();

    FileNodeCachePtr fileNodeCache = localStorage->mFileNodeCache;
    
    string inMemPath = mRootDir + "test_path_match_with_pattern_in_mem";
    string mmapReadPath = 
        mRootDir + "test_path_match_with_pattern_mmap_read";

    FileSystemTestUtil::CreateDiskFile(inMemPath, "in mem file");
    FileSystemTestUtil::CreateDiskFile(mmapReadPath, "mmap read file");

    FileReaderPtr readerInMemFirst = 
        localStorage->CreateFileReader(inMemPath, FSOT_IN_MEM);
    readerInMemFirst->Open();
    ASSERT_EQ(FSOT_IN_MEM, readerInMemFirst->GetOpenType());
    FileReaderPtr readerMmapFirst = 
        localStorage->CreateFileReader(mmapReadPath, FSOT_LOAD_CONFIG); 
    readerMmapFirst->Open();
    ASSERT_EQ(FSOT_LOAD_CONFIG, readerMmapFirst->GetOpenType());   

    // to test cache hit
    // case 1: IN_MEM(mmap in loadConfig) + IN_MEM -> IN_MEM
    FileSystemWrapper::DeleteFile(inMemPath);
    FileReaderPtr readerInMemSecond = 
        localStorage->CreateFileReader(inMemPath, FSOT_IN_MEM);
    readerInMemSecond->Open();
    ASSERT_EQ(FSOT_IN_MEM, readerInMemSecond->GetOpenType());
    ASSERT_EQ(readerInMemFirst->GetBaseAddress(), readerInMemSecond->GetBaseAddress());
 
    // case 2: LOAD_CONFIG(mmap in loadConfig) + LOAD_CONFIG -> LOAD_CONFIG
    FileSystemWrapper::DeleteFile(mmapReadPath);
    FileReaderPtr readerMmapSecond = 
        localStorage->CreateFileReader(mmapReadPath, FSOT_LOAD_CONFIG);
    readerMmapSecond->Open();
    ASSERT_EQ(FSOT_LOAD_CONFIG, readerMmapSecond->GetOpenType());
    ASSERT_EQ(readerMmapFirst->GetBaseAddress(), readerMmapSecond->GetBaseAddress());
}

void LocalStorageTest::TestLoadConfigListWithCacheMiss()
{
    LocalStoragePtr localStorage = PrepareLocalStorage();

    FileNodeCachePtr fileNodeCache = localStorage->mFileNodeCache;
    
    string inMemPath = mRootDir + "test_path_match_with_pattern_in_mem";
    string mmapReadPath = 
        mRootDir + "test_path_match_with_pattern_mmap_read";

    FileSystemTestUtil::CreateDiskFile(inMemPath, "in mem file");
    FileSystemTestUtil::CreateDiskFile(mmapReadPath, "mmap read file");
    
    CheckFileReader(localStorage, inMemPath, FSOT_IN_MEM, "in mem file");
    {
        FileReaderPtr reader = 
            localStorage->CreateFileReader(mmapReadPath, FSOT_LOAD_CONFIG);
        reader->Open();
        ASSERT_EQ(FSOT_LOAD_CONFIG, reader->GetOpenType());
        FileNodePtr fileNode = fileNodeCache->Find(mmapReadPath);
        ASSERT_TRUE(fileNode->MatchType(FSOT_LOAD_CONFIG));
    }
    
    // to test cache miss
    // case1: IN_MEM(mmap in loadConfig) + LOAD_CONFIG -> MMAP_R
    FileReaderPtr reader = 
        localStorage->CreateFileReader(inMemPath, FSOT_LOAD_CONFIG);
    reader->Open();
    ASSERT_EQ(FSOT_LOAD_CONFIG, reader->GetOpenType());
    FileNodePtr fileNode = fileNodeCache->Find(inMemPath);
    ASSERT_TRUE(fileNode->MatchType(FSOT_LOAD_CONFIG));

    // case2: LOAD_CONFIG(mmap in loadConfig) + IN_MEM -> IN_MEM
    CheckFileReader(localStorage, mmapReadPath, FSOT_IN_MEM, "mmap read file");
}

void LocalStorageTest::TestBlockFileLoadConfigMatch()
{
    LocalStoragePtr localStorage = PrepareLocalStorage();

    FileNodeCachePtr fileNodeCache = localStorage->mFileNodeCache;

    string blockMatchPath = mRootDir + "test_cache_path_match_with_pattern";
    FileSystemTestUtil::CreateDiskFile(blockMatchPath, "abcd");

    FileReaderPtr reader =
        localStorage->CreateFileReader(blockMatchPath, FSOT_CACHE);
    reader->Open();
    FileNodePtr fileNode = fileNodeCache->Find(blockMatchPath);
    ASSERT_TRUE(fileNode->MatchType(FSOT_CACHE));
}

void LocalStorageTest::TestStorageMetrics()
{
    LocalStoragePtr localStorage = PrepareLocalStorage();

    string text = "disk file";
    FileSystemTestUtil::CreateDiskFiles(mRootDir + "disk/", 5, text);
    FileSystemTestUtil::CreateDiskFile(
            mRootDir + "disk/test_cache_path_match_with_pattern", text);

    localStorage->CreateFileReader(mRootDir + "disk/0", FSOT_IN_MEM);
    FileReaderPtr mmapRWReader = 
    localStorage->CreateFileReader(mRootDir + "disk/2", FSOT_MMAP);
    localStorage->CreateFileReader(mRootDir + "disk/test_cache_path_match_with_pattern", FSOT_CACHE);
    localStorage->CreateFileReader(mRootDir + "disk/4", FSOT_BUFFERED);

    StorageMetrics metrics = localStorage->GetMetrics();
    ASSERT_EQ((int64_t)text.size() * 4, metrics.GetTotalFileLength());
    ASSERT_EQ(4, metrics.GetTotalFileCount());
    ASSERT_EQ((int64_t)text.size(), metrics.GetFileLength(FSFT_IN_MEM));
    ASSERT_EQ((int64_t)text.size(), metrics.GetFileLength(FSFT_MMAP));
    ASSERT_EQ((int64_t)text.size(), metrics.GetFileLength(FSFT_BLOCK));
    ASSERT_EQ((int64_t)text.size(), metrics.GetFileLength(FSFT_BUFFERED));
    ASSERT_EQ(1, metrics.GetFileCount(FSFT_IN_MEM));
    ASSERT_EQ(1, metrics.GetFileCount(FSFT_MMAP));
    ASSERT_EQ(1, metrics.GetFileCount(FSFT_BLOCK));
    ASSERT_EQ(1, metrics.GetFileCount(FSFT_BUFFERED));

    localStorage->RemoveFile(mRootDir + "disk/0");
    localStorage->RemoveFile(mRootDir + "disk/test_cache_path_match_with_pattern");
    
    metrics = localStorage->GetMetrics();
    ASSERT_EQ((int64_t)text.size() * 2, metrics.GetTotalFileLength());
    ASSERT_EQ(2, metrics.GetTotalFileCount());
    ASSERT_EQ((int64_t)text.size(), metrics.GetFileLength(FSFT_MMAP));
    ASSERT_EQ(( int64_t)text.size(), metrics.GetFileLength(FSFT_BUFFERED));
    ASSERT_EQ(1, metrics.GetFileCount(FSFT_MMAP));
    ASSERT_EQ(1, metrics.GetFileCount(FSFT_BUFFERED));

    localStorage->CleanCache();
    metrics = localStorage->GetMetrics();
    ASSERT_EQ((int64_t)text.size() * 1, metrics.GetTotalFileLength());
    ASSERT_EQ(1, metrics.GetTotalFileCount());
    ASSERT_EQ((int64_t)text.size(), metrics.GetFileLength(FSFT_MMAP));
    ASSERT_EQ(1, metrics.GetFileCount(FSFT_MMAP));

    localStorage->CreateFileReader(mRootDir + "disk/2", FSOT_MMAP);
    metrics = localStorage->GetMetrics();
    ASSERT_EQ((int64_t)text.size(), metrics.GetFileLength(FSFT_MMAP));
    ASSERT_EQ(1, metrics.GetFileCount(FSFT_MMAP));

    mmapRWReader.reset();
    localStorage->RemoveDirectory(mRootDir + "disk");
    metrics = localStorage->GetMetrics();
    ASSERT_EQ(0, metrics.GetTotalFileLength());
    ASSERT_EQ(0, metrics.GetTotalFileCount());
    ASSERT_FALSE(localStorage->IsExist(mRootDir + "disk/5"));
}
void LocalStorageTest::TestStorageMetricsForCacheMiss()
{
    LocalStoragePtr localStorage = PrepareLocalStorage();

    FileSystemTestUtil::CreateDiskFiles(mRootDir + "disk/", 1, "disk");
    FileReaderPtr mmapRWReader = 
        localStorage->CreateFileReader(mRootDir + "disk/0", FSOT_MMAP);
    mmapRWReader->Open();
    
    FileCacheMetrics metrics = localStorage->GetMetrics().GetFileCacheMetrics();
    ASSERT_EQ(0, metrics.hitCount.getValue());
    ASSERT_EQ(1, metrics.missCount.getValue());
    ASSERT_EQ(0, metrics.replaceCount.getValue());
}

void LocalStorageTest::TestBlockCacheMetrics()
{
    LocalStoragePtr localStorage = PrepareLocalStorage();

    BlockCacheMetricsVec metrics;
    localStorage->GetBlockCacheMetricsVec(metrics);
    ASSERT_EQ((size_t)1, metrics.size());
    ASSERT_EQ("load_config1", metrics[0].name);
    ASSERT_EQ((uint64_t)0, metrics[0].totalAccessCount);
    ASSERT_EQ((uint64_t)0, metrics[0].totalHitCount);
    ASSERT_EQ((uint32_t)0, metrics[0].last1000HitCount);

    string blockMatchPath = mRootDir + "test_cache_path_match_with_pattern";
    FileSystemTestUtil::CreateDiskFile(blockMatchPath, "abcd");
    FileReaderPtr reader =
        localStorage->CreateFileReader(blockMatchPath, FSOT_CACHE);
    reader->Open();
    uint8_t buffer[64];
    reader->Read(buffer, 1, 0);
    localStorage->GetBlockCacheMetricsVec(metrics);
    ASSERT_EQ((uint64_t)1, metrics[0].totalAccessCount);
    ASSERT_EQ((uint64_t)0, metrics[0].totalHitCount);
    ASSERT_EQ((uint32_t)0, metrics[0].last1000HitCount);

    reader->Read(buffer, 1, 0);
    localStorage->GetBlockCacheMetricsVec(metrics);
    ASSERT_EQ("load_config1", metrics[0].name);
    ASSERT_EQ((uint64_t)2, metrics[0].totalAccessCount);
    ASSERT_EQ((uint64_t)1, metrics[0].totalHitCount);
    ASSERT_EQ((uint32_t)1, metrics[0].last1000HitCount);
}

void LocalStorageTest::TestRemoveFile()
{
    LocalStoragePtr localStorage = PrepareLocalStorage();

    string text = "disk file";
    FileSystemTestUtil::CreateDiskFiles(mRootDir + "disk/", 2, text);

    string path = mRootDir + "disk/0";
    FileReaderPtr reader = localStorage->CreateFileReader(path, FSOT_BUFFERED);
    reader->Open();
    ASSERT_THROW(localStorage->RemoveFile(path), MemFileIOException);
    ASSERT_TRUE(localStorage->IsExist(path));
    reader.reset();
    ASSERT_NO_THROW(localStorage->RemoveFile(path));
    ASSERT_FALSE(localStorage->IsExist(path));
    ASSERT_THROW(localStorage->RemoveFile(path), NonExistException);

    string onDiskOnlyFilePath = mRootDir + "disk/1";
    ASSERT_NO_THROW(localStorage->RemoveFile(onDiskOnlyFilePath));
    ASSERT_THROW(localStorage->RemoveFile(onDiskOnlyFilePath),
                 NonExistException);
}

void LocalStorageTest::TestRemoveDirectory()
{
    LocalStoragePtr localStorage = PrepareLocalStorage();

    string diskDirPath = mRootDir + "disk";
    FileSystemWrapper::MkDir(diskDirPath);
    ASSERT_TRUE(localStorage->IsExist(diskDirPath));
    ASSERT_NO_THROW(localStorage->RemoveDirectory(diskDirPath));
    ASSERT_FALSE(localStorage->IsExist(diskDirPath));
    ASSERT_THROW(localStorage->RemoveDirectory(diskDirPath),
                 NonExistException);

    // test rm file node in cache
    FileSystemWrapper::MkDir(diskDirPath);
    string path = mRootDir + "disk/0";
    FileSystemTestUtil::CreateDiskFile(path, "disk file");
    localStorage->CreateFileReader(path, FSOT_BUFFERED);
    ASSERT_TRUE(localStorage->IsExist(diskDirPath));
    ASSERT_TRUE(localStorage->mFileNodeCache->IsExist(path));
    ASSERT_NO_THROW(localStorage->RemoveDirectory(diskDirPath));
    ASSERT_FALSE(localStorage->mFileNodeCache->IsExist(path));
    ASSERT_FALSE(localStorage->IsExist(diskDirPath));
}

void LocalStorageTest::CheckFileReader(LocalStoragePtr storage,
                                       string filePath, FSOpenType type,
                                       string expectContext, bool fromLoadConfig)
{
    SCOPED_TRACE(expectContext);
    FileReaderPtr reader = storage->CreateFileReader(filePath, type);
    reader->Open();
    ASSERT_TRUE(reader);
    size_t expectSize = expectContext.size();
    ASSERT_EQ(type, reader->GetOpenType());
    ASSERT_EQ(expectSize, reader->GetLength());
    ASSERT_EQ(filePath, reader->GetPath());
    uint8_t buffer[1024];
    ASSERT_EQ(expectSize, reader->Read(buffer, expectSize)); 
    ASSERT_TRUE(memcmp(buffer, expectContext.data(), expectSize) == 0);

    FileNodeCachePtr fileNodeCache = storage->mFileNodeCache;
    FileNodePtr fileNode = fileNodeCache->Find(filePath);

    reader->Close();
}

void LocalStorageTest::CreateLoadConfigList(LoadConfigList& loadConfigList)
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

    loadConfigList = 
        LoadConfigListCreator::CreateLoadConfigListFromJsonString(jsonStr);
}

LocalStoragePtr LocalStorageTest::PrepareLocalStorage()
{
    LoadConfigList loadConfigList;
    CreateLoadConfigList(loadConfigList);
    FileSystemOptions options;
    options.loadConfigList = loadConfigList;
    options.fileBlockCache = mFileBlockCache;
    LocalStoragePtr localStorage(new LocalStorage(PathUtil::NormalizePath(
                            GET_TEST_DATA_PATH()), mMemoryController));
    localStorage->Init(options);
    return localStorage;
}

IE_NAMESPACE_END(file_system);

