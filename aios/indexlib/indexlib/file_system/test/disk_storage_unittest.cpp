#include "indexlib/misc/exception.h"
#include "indexlib/file_system/load_config/load_config_list.h"
#include "indexlib/file_system/load_config/load_strategy.h"
#include "indexlib/util/path_util.h"
#include "indexlib/file_system/file_reader.h"
#include "indexlib/file_system/buffered_file_reader.h"
#include "indexlib/file_system/block_file_node.h"
#include "indexlib/file_system/file_node.h"
#include "indexlib/file_system/test/disk_storage_unittest.h"
#include "indexlib/file_system/test/file_system_test_util.h"
#include "indexlib/file_system/test/load_config_list_creator.h"
#include "indexlib/util/memory_control/memory_quota_controller_creator.h"
#include "indexlib/file_system/file_block_cache.h"
#include "indexlib/util/cache/block_cache.h"


using namespace std;
using namespace autil;

IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, DiskStorageTest);

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

DiskStorageTest::DiskStorageTest()
{
}

DiskStorageTest::~DiskStorageTest()
{
}

void DiskStorageTest::CaseSetUp()
{
    mRootDir = GET_TEST_DATA_PATH();
    mMemoryController = MemoryQuotaControllerCreator::CreateBlockMemoryController();
    mFileBlockCache.reset(new FileBlockCache());
    mFileBlockCache->Init(128 * 4 * 1024, 4 * 1024,
                          MemoryQuotaControllerCreator::CreateSimpleMemoryController());
}

void DiskStorageTest::CaseTearDown()
{
}

void DiskStorageTest::TestLocalInit()
{
    MockLoadStrategyPtr mockLoadStrategy(new MockLoadStrategy);
    LoadConfig loadConfig;
    loadConfig.SetLoadStrategyPtr(mockLoadStrategy);
    LoadConfigList loadConfigList;
    loadConfigList.PushFront(loadConfig);
    FileSystemOptions options;
    options.loadConfigList = loadConfigList;
    DiskStorage diskStorage("", mMemoryController);

    ASSERT_THROW(diskStorage.Init(options), UnSupportedException);
}

void DiskStorageTest::TestMmapFileReader()
{
    DiskStoragePtr diskStorage = PrepareDiskStorage();
    
    string mmapPath = mRootDir + "mmap_file";
    FileSystemTestUtil::CreateDiskFile(mmapPath, "abcd");

    FileReaderPtr reader0 =
        diskStorage->CreateWritableFileReader(mmapPath, FSOT_MMAP);
    reader0->Open();
    FileReaderPtr reader1 =
        diskStorage->CreateWritableFileReader(mmapPath, FSOT_MMAP);
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

void DiskStorageTest::TestBlockFileReader()
{
    DiskStoragePtr diskStorage = PrepareDiskStorage();
    
    FileNodeCachePtr fileNodeCache = diskStorage->mFileNodeCache;
    string blockFilePath = mRootDir + "test_cache_path_match_with_pattern";
    FileSystemTestUtil::CreateDiskFile(blockFilePath, "abcd");

    FileReaderPtr reader0 =
        diskStorage->CreateFileReader(blockFilePath, FSOT_CACHE);
    reader0->Open();
    FileNodePtr fileNode0 = fileNodeCache->Find(blockFilePath);
    ASSERT_TRUE(fileNode0);
    ASSERT_EQ(3, fileNode0.use_count());
    FileReaderPtr reader1 =
        diskStorage->CreateFileReader(blockFilePath, FSOT_CACHE);
    reader1->Open();
    ASSERT_EQ(FSOT_CACHE, reader0->GetOpenType());
    ASSERT_EQ(FSOT_CACHE, reader1->GetOpenType());
    ASSERT_EQ(4, fileNode0.use_count());

    ASSERT_FALSE(reader0->GetBaseAddress());
    ASSERT_FALSE(reader1->GetBaseAddress());

    string defaultFilePath = mRootDir + "notsupportdefault";
    FileSystemTestUtil::CreateDiskFile(defaultFilePath, "abcd");
    ASSERT_THROW(diskStorage->CreateFileReader(defaultFilePath, FSOT_CACHE), 
                 UnSupportedException);

}

void DiskStorageTest::TestGlobalBlockFileReader()
{
    DiskStoragePtr diskStorage = PrepareDiskStorage();
    
    FileNodeCachePtr fileNodeCache = diskStorage->mFileNodeCache;
    string blockFilePath = mRootDir + "test_global_cache_path_match_with_pattern";
    FileSystemTestUtil::CreateDiskFile(blockFilePath, "abcd");

    FileReaderPtr reader =
        diskStorage->CreateFileReader(blockFilePath, FSOT_CACHE);
    reader->Open();
    FileNodePtr fileNode = fileNodeCache->Find(blockFilePath);
    ASSERT_TRUE(fileNode);
    ASSERT_EQ(3, fileNode.use_count());
    ASSERT_EQ(FSOT_CACHE, reader->GetOpenType());


    char buffer[32];
    reader->Read(buffer, 4);
    ASSERT_EQ("abcd", string(buffer, 4));

}

void DiskStorageTest::TestInMemFileReader()
{
    DiskStoragePtr diskStorage = PrepareDiskStorage();
    
    FileNodeCachePtr fileNodeCache = diskStorage->mFileNodeCache;
    string inMemPath = mRootDir + "in_mem_file";
    FileSystemTestUtil::CreateDiskFile(inMemPath, "abcd");

    FileReaderPtr reader0 =
        diskStorage->CreateFileReader(inMemPath, FSOT_IN_MEM);
    reader0->Open();
    FileNodePtr fileNode = fileNodeCache->Find(inMemPath);
    ASSERT_EQ(3, fileNode.use_count());
    FileReaderPtr reader1 =
        diskStorage->CreateFileReader(inMemPath, FSOT_IN_MEM);
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

void DiskStorageTest::TestBufferedFileReader()
{
    DiskStoragePtr diskStorage = PrepareDiskStorage();

    string bufferedPath = mRootDir + "buffered_file";
    FileSystemTestUtil::CreateDiskFile(bufferedPath, "abcd");
    FileReaderPtr reader = 
        diskStorage->CreateFileReader(bufferedPath, FSOT_BUFFERED);
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

void DiskStorageTest::TestBufferedFileWriter()
{
    InnerTestBufferedFileWriter(true);
    InnerTestBufferedFileWriter(false);
}

void DiskStorageTest::InnerTestBufferedFileWriter(bool atomicWrite)
{
    TearDown();
    SetUp();
    DiskStoragePtr diskStorage = PrepareDiskStorage();
    string filePath = mRootDir + "local/file";
    // ASSERT_THROW(diskStorage->CreateFileWriter(
    //                  filePath, FSWriterParam(atomicWrite)),
    //              NonExistException);
    diskStorage->MakeDirectory(mRootDir + "local");
    FSWriterParam param(atomicWrite);
    param.bufferSize = 2;
    FileWriterPtr fileWriter = diskStorage->CreateFileWriter(filePath, param);
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


void DiskStorageTest::TestUnknownFileReader()
{
    DiskStoragePtr diskStorage = PrepareDiskStorage();

    string filePath = mRootDir + "unknown_file";
    FileSystemTestUtil::CreateDiskFile(filePath, "abcd");
    ASSERT_THROW(diskStorage->CreateFileReader(filePath, FSOT_UNKNOWN),
                 UnSupportedException);
}

void DiskStorageTest::TestNoLoadConfigListWithCacheHit()
{
    DiskStoragePtr diskStorage(new DiskStorage(mRootDir, mMemoryController));
    diskStorage->Init(FileSystemOptions());
    
    string inMemPath = mRootDir + "in_mem";
    string mmapRPath = mRootDir + "mmap_r";
    string mmapRWPath = mRootDir + "mmap_rw";
    string loadConfigPath = mRootDir + "load_config";

    ASSERT_ANY_THROW(CheckFileReader(diskStorage, inMemPath, FSOT_IN_MEM, "in memory"));
    ASSERT_ANY_THROW(CheckFileReader(diskStorage, mmapRPath, FSOT_MMAP, "mmap for r"));
    ASSERT_ANY_THROW(CheckFileReader(diskStorage, mmapRWPath, FSOT_MMAP, "mmap for rw", false, true));

    FileSystemTestUtil::CreateDiskFile(inMemPath, "in memory");
    FileSystemTestUtil::CreateDiskFile(mmapRPath, "mmap for r");
    FileSystemTestUtil::CreateDiskFile(mmapRWPath, "mmap for rw");
    FileSystemTestUtil::CreateDiskFile(loadConfigPath, "load config");

    // test for first create file reader
    CheckFileReader(diskStorage, inMemPath, FSOT_IN_MEM, "in memory");
    CheckFileReader(diskStorage, mmapRPath, FSOT_MMAP, "mmap for r");
    CheckFileReader(diskStorage, mmapRWPath, FSOT_MMAP, "mmap for rw", false, true);
    CheckFileReader(diskStorage, loadConfigPath, FSOT_LOAD_CONFIG, "load config");

    // test for cache hit
    FileSystemWrapper::DeleteFile(inMemPath);
    FileSystemWrapper::DeleteFile(mmapRPath);
    FileSystemWrapper::DeleteFile(mmapRWPath);
    CheckFileReader(diskStorage, inMemPath, FSOT_IN_MEM, "in memory");
    CheckFileReader(diskStorage, mmapRPath, FSOT_MMAP, "mmap for r");
    CheckFileReader(diskStorage, mmapRWPath, FSOT_MMAP, "mmap for rw", false, true);
    CheckFileReader(diskStorage, loadConfigPath, FSOT_LOAD_CONFIG, "load config");

    // cache hit will use same base address
    FileReaderPtr reader0 =
        diskStorage->CreateWritableFileReader(mmapRWPath, FSOT_MMAP);
    reader0->Open();
    FileReaderPtr reader1 =
        diskStorage->CreateWritableFileReader(mmapRWPath, FSOT_MMAP);
    reader1->Open();
    ASSERT_EQ(FSOT_MMAP, reader0->GetOpenType());
    ASSERT_EQ(FSOT_MMAP, reader1->GetOpenType());
    uint8_t* data0 = (uint8_t*) reader0->GetBaseAddress();
    uint8_t* data1 = (uint8_t*) reader1->GetBaseAddress();
    ASSERT_EQ(data0, data1);
    data0[5] = '0';
}

void DiskStorageTest::TestNoLoadConfigListWithCacheMiss()
{
    DiskStoragePtr diskStorage(new DiskStorage(mRootDir, mMemoryController));
    diskStorage->Init(FileSystemOptions());
    
    string inMemPath = mRootDir + "in_mem";
    string mmapRPath = mRootDir + "mmap_r";
    string mmapRWPath = mRootDir + "mmap_rw";
    string loadConfigPath = mRootDir + "load_config";

    FileSystemTestUtil::CreateDiskFile(inMemPath, "in memory");
    FileSystemTestUtil::CreateDiskFile(mmapRPath, "mmap for r");
    FileSystemTestUtil::CreateDiskFile(mmapRWPath, "mmap for rw");

    // test for first create file reader
    CheckFileReader(diskStorage, inMemPath, FSOT_IN_MEM, "in memory");
    CheckFileReader(diskStorage, mmapRPath, FSOT_MMAP, "mmap for r");
    CheckFileReader(diskStorage, mmapRWPath, FSOT_MMAP, "mmap for rw", false, true);

    // test for cache Miss
    CheckFileReader(diskStorage, inMemPath, FSOT_MMAP, "in memory");
    CheckFileReader(diskStorage, mmapRWPath, FSOT_MMAP, "mmap for rw", false, true);
}

void DiskStorageTest::TestLoadConfigListWithNoCache()
{
    DiskStoragePtr diskStorage = PrepareDiskStorage();
    
    string noMatchPath = mRootDir + "miss_file";
    string matchPath = mRootDir + "test_path_match_with_pattern";

    FileSystemTestUtil::CreateDiskFile(noMatchPath, "no match file");
    FileSystemTestUtil::CreateDiskFile(matchPath, "match file");

    // Open type with FSOT_MMAP
    CheckFileReader(diskStorage, noMatchPath, FSOT_MMAP, "no match file");
    CheckFileReader(diskStorage, matchPath, FSOT_MMAP, "match file", true);

    // Open type with FSOT_LOAD_CONFIG
    diskStorage->CleanCache();
    FileNodeCachePtr fileNodeCache = diskStorage->mFileNodeCache;
    ASSERT_EQ((size_t)0, fileNodeCache->GetFileCount());
    CheckFileReader(diskStorage, noMatchPath, FSOT_LOAD_CONFIG, "no match file", true);
    FileReaderPtr reader = 
        diskStorage->CreateFileReader(matchPath, FSOT_LOAD_CONFIG);
    reader->Open();
    ASSERT_EQ(FSOT_LOAD_CONFIG, reader->GetOpenType());
    FileNodePtr fileNode = fileNodeCache->Find(matchPath);
    ASSERT_TRUE(fileNode->GetType() == FSFT_MMAP);
    reader->Close();
}

void DiskStorageTest::TestLoadConfigListWithCacheHit()
{
    DiskStoragePtr diskStorage = PrepareDiskStorage();

    FileNodeCachePtr fileNodeCache = diskStorage->mFileNodeCache;
    
    string inMemPath = mRootDir + "test_path_match_with_pattern_in_mem";
    string mmapReadPath = 
        mRootDir + "test_path_match_with_pattern_mmap_read";

    FileSystemTestUtil::CreateDiskFile(inMemPath, "in mem file");
    FileSystemTestUtil::CreateDiskFile(mmapReadPath, "mmap read file");

    FileReaderPtr readerInMemFirst = 
        diskStorage->CreateFileReader(inMemPath, FSOT_IN_MEM);
    readerInMemFirst->Open();
    ASSERT_EQ(FSOT_IN_MEM, readerInMemFirst->GetOpenType());
    FileReaderPtr readerMmapFirst = 
        diskStorage->CreateFileReader(mmapReadPath, FSOT_LOAD_CONFIG); 
    readerMmapFirst->Open();
    ASSERT_EQ(FSOT_LOAD_CONFIG, readerMmapFirst->GetOpenType());   

    // to test cache hit
    // case 1: IN_MEM(mmap in loadConfig) + IN_MEM -> IN_MEM
    FileSystemWrapper::DeleteFile(inMemPath);
    FileReaderPtr readerInMemSecond = 
        diskStorage->CreateFileReader(inMemPath, FSOT_IN_MEM);
    readerInMemSecond->Open();
    ASSERT_EQ(FSOT_IN_MEM, readerInMemSecond->GetOpenType());
    ASSERT_EQ(readerInMemFirst->GetBaseAddress(), readerInMemSecond->GetBaseAddress());
 
    // case 2: LOAD_CONFIG(mmap in loadConfig) + LOAD_CONFIG -> LOAD_CONFIG
    FileSystemWrapper::DeleteFile(mmapReadPath);
    FileReaderPtr readerMmapSecond = 
        diskStorage->CreateFileReader(mmapReadPath, FSOT_LOAD_CONFIG);
    readerMmapSecond->Open();
    ASSERT_EQ(FSOT_LOAD_CONFIG, readerMmapSecond->GetOpenType());
    ASSERT_EQ(readerMmapFirst->GetBaseAddress(), readerMmapSecond->GetBaseAddress());
}

void DiskStorageTest::TestLoadConfigListWithCacheMiss()
{
    DiskStoragePtr diskStorage = PrepareDiskStorage();

    FileNodeCachePtr fileNodeCache = diskStorage->mFileNodeCache;
    
    string inMemPath = mRootDir + "test_path_match_with_pattern_in_mem";
    string mmapReadPath = 
        mRootDir + "test_path_match_with_pattern_mmap_read";

    FileSystemTestUtil::CreateDiskFile(inMemPath, "in mem file");
    FileSystemTestUtil::CreateDiskFile(mmapReadPath, "mmap read file");
    
    CheckFileReader(diskStorage, inMemPath, FSOT_IN_MEM, "in mem file");
    {
        FileReaderPtr reader = 
            diskStorage->CreateFileReader(mmapReadPath, FSOT_LOAD_CONFIG);
        reader->Open();
        ASSERT_EQ(FSOT_LOAD_CONFIG, reader->GetOpenType());
        FileNodePtr fileNode = fileNodeCache->Find(mmapReadPath);
        ASSERT_TRUE(fileNode->MatchType(FSOT_LOAD_CONFIG, false));
    }
    
    // to test cache miss
    // case1: IN_MEM(mmap in loadConfig) + LOAD_CONFIG -> MMAP_R
    FileReaderPtr reader = 
        diskStorage->CreateFileReader(inMemPath, FSOT_LOAD_CONFIG);
    reader->Open();
    ASSERT_EQ(FSOT_LOAD_CONFIG, reader->GetOpenType());
    FileNodePtr fileNode = fileNodeCache->Find(inMemPath);
    ASSERT_TRUE(fileNode->MatchType(FSOT_LOAD_CONFIG, false));

    // case2: LOAD_CONFIG(mmap in loadConfig) + IN_MEM -> IN_MEM
    CheckFileReader(diskStorage, mmapReadPath, FSOT_IN_MEM, "mmap read file");
}

void DiskStorageTest::TestBlockFileLoadConfigMatch()
{
    DiskStoragePtr diskStorage = PrepareDiskStorage();

    FileNodeCachePtr fileNodeCache = diskStorage->mFileNodeCache;

    string blockMatchPath = mRootDir + "test_cache_path_match_with_pattern";
    FileSystemTestUtil::CreateDiskFile(blockMatchPath, "abcd");

    FileReaderPtr reader =
        diskStorage->CreateFileReader(blockMatchPath, FSOT_CACHE);
    reader->Open();
    FileNodePtr fileNode = fileNodeCache->Find(blockMatchPath);
    ASSERT_TRUE(fileNode->MatchType(FSOT_CACHE, false));
}

void DiskStorageTest::TestLoadWithLifecycleConfig() {
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

    string matchPath = mRootDir + "test_path_match_with_pattern";
    FileSystemTestUtil::CreateDiskFile(matchPath, "value of " + matchPath);

    auto getMmapLockMode = [](FileNodePtr &fileNode) {
                               MmapFileNodePtr mMapFileNode =
                                   DYNAMIC_POINTER_CAST(MmapFileNode, fileNode);
                               assert(mMapFileNode);
                               MmapLoadStrategyPtr loadStrategy = mMapFileNode->mLoadStrategy;
                               return loadStrategy->mIsLock;
    };

    InnerTestLifecycle(jsonStr, matchPath, "cold", FSOT_LOAD_CONFIG, false,
                       getMmapLockMode, nullptr);
    InnerTestLifecycle(jsonStr, matchPath, "cold",
                       FSOT_MMAP, false, getMmapLockMode, nullptr);
    InnerTestLifecycle(jsonStr, matchPath, "hot",
                       FSOT_MMAP, true, getMmapLockMode, nullptr);   

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
    InnerTestLifecycle(jsonStr, matchPath, "cold",
                       FSOT_MMAP, false, getMmapLockMode, nullptr);   
    InnerTestLifecycle(jsonStr, matchPath, "hot",
                       FSOT_MMAP, true, getMmapLockMode, nullptr);   
    InnerTestLifecycle(jsonStr, matchPath, "hot",
                       FSOT_LOAD_CONFIG, true, getMmapLockMode, nullptr);   

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
    auto checkCacheLoadStrategy = [](FileNodePtr& fileNode) {
                                      BlockFileNodePtr blockFileNode = DYNAMIC_POINTER_CAST(BlockFileNode, fileNode);
                                      ASSERT_TRUE(fileNode != NULL);
                                  };
    InnerTestLifecycle(jsonStr, matchPath, "hot", FSOT_LOAD_CONFIG, false,
                       nullptr, checkCacheLoadStrategy);
    InnerTestLifecycle(jsonStr, matchPath, "hot", FSOT_MMAP, false,
                       getMmapLockMode, nullptr);
        
}

void DiskStorageTest::InnerTestLifecycle(
    const string &loadConfigJsonStr, const string& matchPath,
    const string lifecycle, FSOpenType openType, bool expectLock,
    std::function<bool(FileNodePtr &)> getLockModeFunc,
    std::function<void(FileNodePtr &)> checkFunc) {

        LoadConfigList loadConfigList =
            LoadConfigListCreator::CreateLoadConfigListFromJsonString(loadConfigJsonStr);

        FileSystemOptions options;
        options.loadConfigList = loadConfigList;
        options.fileBlockCache = mFileBlockCache;
        options.enablePathMetaContainer = true;
        DiskStoragePtr diskStorage(new DiskStorage(
                                       PathUtil::NormalizePath(GET_TEST_DATA_PATH()), mMemoryController));
        diskStorage->Init(options);
        diskStorage->mPathMetaContainer->AddFileInfo(matchPath, -1, -1, false);
        auto mit = matchPath.rbegin();
        for (; mit != matchPath.rend() && *mit != '/'; mit++);
        diskStorage->SetDirLifecycle(
            matchPath.substr(0, matchPath.rend() - mit), lifecycle);

        FileReaderPtr reader =
            diskStorage->CreateFileReader(matchPath, openType);
        FileNodePtr fileNode = reader->GetFileNode();

        if (getLockModeFunc) {
            bool lockMode = getLockModeFunc(fileNode);
            ASSERT_EQ(expectLock, lockMode);
        }
        if (checkFunc) {
            checkFunc(fileNode);
        }
    }

void DiskStorageTest::TestStorageMetrics() {
    DiskStoragePtr diskStorage = PrepareDiskStorage();

    string text = "disk file";
    FileSystemTestUtil::CreateDiskFiles(mRootDir + "disk/", 5, text);
    FileSystemTestUtil::CreateDiskFile(
            mRootDir + "disk/test_cache_path_match_with_pattern", text);

    diskStorage->CreateFileReader(mRootDir + "disk/0", FSOT_IN_MEM);
    FileReaderPtr mmapRReader = 
        diskStorage->CreateFileReader(mRootDir + "disk/1", FSOT_MMAP);
    mmapRReader->Open();
    FileReaderPtr mmapRWReader = 
    diskStorage->CreateFileReader(mRootDir + "disk/2", FSOT_MMAP);
    diskStorage->CreateFileReader(mRootDir + "disk/test_cache_path_match_with_pattern", FSOT_CACHE);
    diskStorage->CreateFileReader(mRootDir + "disk/4", FSOT_BUFFERED);

    StorageMetrics metrics = diskStorage->GetMetrics();
    ASSERT_EQ((int64_t)text.size() * 5, metrics.GetTotalFileLength());
    ASSERT_EQ(5, metrics.GetTotalFileCount());
    ASSERT_EQ((int64_t)text.size(), metrics.GetFileLength(FSFT_IN_MEM));
    ASSERT_EQ((int64_t)text.size() * 2, metrics.GetFileLength(FSFT_MMAP));
    ASSERT_EQ((int64_t)text.size(), metrics.GetFileLength(FSFT_BLOCK));
    ASSERT_EQ((int64_t)text.size(), metrics.GetFileLength(FSFT_BUFFERED));
    ASSERT_EQ(1, metrics.GetFileCount(FSFT_IN_MEM));
    ASSERT_EQ(2, metrics.GetFileCount(FSFT_MMAP));
    ASSERT_EQ(1, metrics.GetFileCount(FSFT_BLOCK));
    ASSERT_EQ(1, metrics.GetFileCount(FSFT_BUFFERED));

    diskStorage->RemoveFile(mRootDir + "disk/0");
    diskStorage->RemoveFile(mRootDir + "disk/test_cache_path_match_with_pattern");
    
    metrics = diskStorage->GetMetrics();
    ASSERT_EQ((int64_t)text.size() * 3, metrics.GetTotalFileLength());
    ASSERT_EQ(3, metrics.GetTotalFileCount());
    ASSERT_EQ((int64_t)text.size() * 2, metrics.GetFileLength(FSFT_MMAP));
    ASSERT_EQ(( int64_t)text.size(), metrics.GetFileLength(FSFT_BUFFERED));
    ASSERT_EQ(2, metrics.GetFileCount(FSFT_MMAP));
    ASSERT_EQ(1, metrics.GetFileCount(FSFT_BUFFERED));

    diskStorage->CleanCache();
    metrics = diskStorage->GetMetrics();
    ASSERT_EQ((int64_t)text.size() * 2, metrics.GetTotalFileLength());
    ASSERT_EQ(2, metrics.GetTotalFileCount());
    ASSERT_EQ((int64_t)text.size() * 2, metrics.GetFileLength(FSFT_MMAP));
    ASSERT_EQ(2, metrics.GetFileCount(FSFT_MMAP));

    mmapRReader.reset();
    diskStorage->CreateFileReader(mRootDir + "disk/1", FSOT_MMAP);
    metrics = diskStorage->GetMetrics();
    ASSERT_EQ((int64_t)text.size() * 2, metrics.GetFileLength(FSFT_MMAP));
    ASSERT_EQ(2, metrics.GetFileCount(FSFT_MMAP));

    mmapRWReader.reset();
    diskStorage->RemoveDirectory(mRootDir + "disk", false);
    metrics = diskStorage->GetMetrics();
    ASSERT_EQ(0, metrics.GetTotalFileLength());
    ASSERT_EQ(0, metrics.GetTotalFileCount());
    ASSERT_FALSE(diskStorage->IsExist(mRootDir + "disk/5"));
}
void DiskStorageTest::TestStorageMetricsForCacheMiss()
{
    DiskStoragePtr diskStorage = PrepareDiskStorage();

    FileSystemTestUtil::CreateDiskFiles(mRootDir + "disk/", 1, "disk");
    FileReaderPtr mmapRReader = 
        diskStorage->CreateFileReader(mRootDir + "disk/0", FSOT_MMAP);
    mmapRReader->Open();
    
    FileCacheMetrics metrics = diskStorage->GetMetrics().GetFileCacheMetrics();
    ASSERT_EQ(0, metrics.hitCount.getValue());
    ASSERT_EQ(1, metrics.missCount.getValue());
    ASSERT_EQ(0, metrics.replaceCount.getValue());
}

void DiskStorageTest::TestRemoveFile()
{
    DiskStoragePtr diskStorage = PrepareDiskStorage();

    string text = "disk file";
    FileSystemTestUtil::CreateDiskFiles(mRootDir + "disk/", 2, text);

    string path = mRootDir + "disk/0";
    FileReaderPtr reader = diskStorage->CreateFileReader(path, FSOT_BUFFERED);
    reader->Open();
    ASSERT_THROW(diskStorage->RemoveFile(path), MemFileIOException);
    ASSERT_TRUE(diskStorage->IsExist(path));
    reader.reset();
    ASSERT_NO_THROW(diskStorage->RemoveFile(path));
    ASSERT_FALSE(diskStorage->IsExist(path));
    ASSERT_THROW(diskStorage->RemoveFile(path), NonExistException);

    string onDiskOnlyFilePath = mRootDir + "disk/1";
    ASSERT_NO_THROW(diskStorage->RemoveFile(onDiskOnlyFilePath));
    ASSERT_THROW(diskStorage->RemoveFile(onDiskOnlyFilePath),
                 NonExistException);
}

void DiskStorageTest::TestRemoveDirectory()
{
    DiskStoragePtr diskStorage = PrepareDiskStorage();

    string diskDirPath = mRootDir + "disk";
    FileSystemWrapper::MkDir(diskDirPath);
    ASSERT_TRUE(diskStorage->IsExist(diskDirPath));
    ASSERT_NO_THROW(diskStorage->RemoveDirectory(diskDirPath, false));
    ASSERT_FALSE(diskStorage->IsExist(diskDirPath));
    ASSERT_THROW(diskStorage->RemoveDirectory(diskDirPath, false),
                 NonExistException);

    // test rm file node in cache
    FileSystemWrapper::MkDir(diskDirPath);
    string path = mRootDir + "disk/0";
    FileSystemTestUtil::CreateDiskFile(path, "disk file");
    diskStorage->CreateFileReader(path, FSOT_BUFFERED);
    ASSERT_TRUE(diskStorage->IsExist(diskDirPath));
    ASSERT_TRUE(diskStorage->mFileNodeCache->IsExist(path));
    ASSERT_NO_THROW(diskStorage->RemoveDirectory(diskDirPath, false));
    ASSERT_FALSE(diskStorage->mFileNodeCache->IsExist(path));
    ASSERT_FALSE(diskStorage->IsExist(diskDirPath));
}

void DiskStorageTest::TestIsExist()
{
    {
        LoadConfigList loadConfigList;
        CreateLoadConfigList(loadConfigList);
        FileSystemOptions options;
        options.loadConfigList = loadConfigList;
        options.fileBlockCache = mFileBlockCache;
        options.enablePathMetaContainer = true;
        options.isOffline = true;
        DiskStoragePtr diskStorage(
            new DiskStorage(TEST_DATA_PATH, mMemoryController));
        diskStorage->Init(options);
        string dirPath = string(TEST_DATA_PATH"/schema");
        ASSERT_TRUE(diskStorage->IsExist(dirPath));
        // offline fileSystem will update cache
        ASSERT_TRUE(diskStorage->mPathMetaContainer->IsExist(dirPath));
        
        string notExistFile = string(TEST_DATA_PATH"/no-exist-file");
        ASSERT_FALSE(diskStorage->IsExist(notExistFile));
        ASSERT_FALSE(diskStorage->mPathMetaContainer->IsExist(notExistFile));

        string testFile = string(TEST_DATA_PATH"/simple_table_schema.json");
        ASSERT_TRUE(diskStorage->IsExist(testFile));
        // offline fileSystem will update cache
        ASSERT_TRUE(diskStorage->mPathMetaContainer->IsExist(testFile));
    }
    {
        LoadConfigList loadConfigList;
        CreateLoadConfigList(loadConfigList);
        FileSystemOptions options;
        options.loadConfigList = loadConfigList;
        options.fileBlockCache = mFileBlockCache;
        options.enablePathMetaContainer = true;
        options.isOffline = false;
        DiskStoragePtr diskStorage(
            new DiskStorage(TEST_DATA_PATH, mMemoryController));
        diskStorage->Init(options);
        string dirPath = string(TEST_DATA_PATH"/schema");
        ASSERT_TRUE(diskStorage->IsExist(dirPath));
        // online fileSystem will update cache
        ASSERT_FALSE(diskStorage->mPathMetaContainer->IsExist(dirPath));
        
        string notExistFile = string(TEST_DATA_PATH"/no-exist-file");
        ASSERT_FALSE(diskStorage->IsExist(notExistFile));
        ASSERT_FALSE(diskStorage->mPathMetaContainer->IsExist(notExistFile));

        string testFile = string(TEST_DATA_PATH"/simple_table_schema.json");
        ASSERT_TRUE(diskStorage->IsExist(testFile));
        // offline fileSystem will update cache
        ASSERT_FALSE(diskStorage->mPathMetaContainer->IsExist(testFile));
    }
}

void DiskStorageTest::CheckFileReader(DiskStoragePtr storage, string filePath, FSOpenType type,
    string expectContext, bool fromLoadConfig, bool writable)
{
    SCOPED_TRACE(expectContext);
    FileReaderPtr reader;
    if (!writable)
    {
        reader = storage->CreateFileReader(filePath, type);
    }
    else
    {
        reader = storage->CreateWritableFileReader(filePath, type);
    }
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

DiskStoragePtr DiskStorageTest::PrepareDiskStorage()
{
    LoadConfigList loadConfigList;
    CreateLoadConfigList(loadConfigList);
    FileSystemOptions options;
    options.loadConfigList = loadConfigList;
    options.fileBlockCache = mFileBlockCache;
    DiskStoragePtr diskStorage(new DiskStorage(PathUtil::NormalizePath(
                            GET_TEST_DATA_PATH()), mMemoryController));
    diskStorage->Init(options);
    return diskStorage;
}

IE_NAMESPACE_END(file_system);

