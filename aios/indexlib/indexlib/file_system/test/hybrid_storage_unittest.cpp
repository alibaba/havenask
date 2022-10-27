#include "indexlib/file_system/test/hybrid_storage_unittest.h"
#include "indexlib/misc/exception.h"
#include "indexlib/file_system/load_config/load_config_list.h"
#include "indexlib/file_system/load_config/load_strategy.h"
#include "indexlib/util/path_util.h"
#include "indexlib/file_system/file_reader.h"
#include "indexlib/file_system/buffered_file_reader.h"
#include "indexlib/file_system/file_node.h"
#include "indexlib/file_system/buffered_file_node.h"
#include "indexlib/file_system/test/file_system_test_util.h"
#include "indexlib/file_system/test/package_file_util.h"
#include "indexlib/file_system/test/load_config_list_creator.h"
#include "indexlib/util/memory_control/memory_quota_controller_creator.h"
#include "indexlib/file_system/file_block_cache.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/util/cache/block_cache.h"
#include <autil/StringTokenizer.h>
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, HybridStorageTest);

HybridStorageTest::HybridStorageTest()
{
}

HybridStorageTest::~HybridStorageTest()
{
}

void HybridStorageTest::CaseSetUp()
{
    mPrimaryRoot = PathUtil::JoinPath(GET_TEST_DATA_PATH(), "root");
    mSecondaryRoot = PathUtil::JoinPath(GET_TEST_DATA_PATH(), "second_root");

    FileSystemWrapper::MkDir(mPrimaryRoot);
    FileSystemWrapper::MkDir(mSecondaryRoot);

    mMemoryController = MemoryQuotaControllerCreator::CreateBlockMemoryController();
    mFileBlockCache.reset(new FileBlockCache());
    mFileBlockCache->Init(128 * 4 * 1024, 4 * 1024,
                          MemoryQuotaControllerCreator::CreateSimpleMemoryController());
}

void HybridStorageTest::CaseTearDown()
{
    // FileSystemWrapper::DeleteDir(mPrimaryRoot);
    // FileSystemWrapper::DeleteDir(mSecondaryRoot);
}

string HybridStorageTest::GetPrimaryPath(const string& relativePath)
{
    return PathUtil::JoinPath(mPrimaryRoot, relativePath);
}

string HybridStorageTest::GetSecondaryPath(const string& relativePath)
{
    return PathUtil::JoinPath(mSecondaryRoot, relativePath);
}

void HybridStorageTest::TestCreateFileWriter()
{
    auto storage = PrepareHybridStorage("", "");
    auto fileWriter = storage->CreateFileWriter(GetPrimaryPath("testfile"));
    fileWriter->Close();
    ASSERT_TRUE(FileSystemWrapper::IsExist(GetPrimaryPath("testfile")));
    ASSERT_FALSE(FileSystemWrapper::IsExist(GetSecondaryPath("testfile")));
}

void HybridStorageTest::InnerTestCreateFileReader(
        bool primary, bool secondary, bool withCache)
{
    FileSystemWrapper::DeleteDir(mPrimaryRoot);
    FileSystemWrapper::DeleteDir(mSecondaryRoot);
    CaseTearDown();
    CaseSetUp();

    string primaryStr;
    string secondaryStr;
    if (primary)
    {
        primaryStr = mPrimaryRoot + ":" + "testfile";
    }
    if (secondary)
    {
        secondaryStr = mSecondaryRoot + ":" + "testfile";
    }

    auto storage = PrepareHybridStorage(primaryStr, secondaryStr);
    string content = "helloworld";

    if (primary || secondary)
    {
        FileReaderPtr fileReader;
        if (withCache)
        {
            fileReader = storage->CreateFileReader(GetPrimaryPath("testfile"), FSOT_MMAP);
        }
        else
        {
            fileReader = storage->CreateFileReaderWithoutCache(GetPrimaryPath("testfile"), FSOT_MMAP);
        }
        ASSERT_TRUE(fileReader);
        char *addr = static_cast<char*>(fileReader->GetBaseAddress());
        ASSERT_EQ(content, string(addr, fileReader->GetLength()));
        auto fileNode = fileReader->GetFileNode();
    }
    else
    {
        if (withCache)
        {
            ASSERT_ANY_THROW(storage->CreateFileReader(GetPrimaryPath("testfile"), FSOT_MMAP));            
        }
        else
        {
            ASSERT_ANY_THROW(storage->CreateFileReaderWithoutCache(GetPrimaryPath("testfile"), FSOT_MMAP));
        }
    }
}

void HybridStorageTest::TestCreateFileReader()
{
    InnerTestCreateFileReader(true, false, true);
    InnerTestCreateFileReader(true, true, true);
    InnerTestCreateFileReader(false, true, true);
    InnerTestCreateFileReader(false, false, true);

    InnerTestCreateFileReader(true, false, false);
    InnerTestCreateFileReader(true, true, false);
    InnerTestCreateFileReader(false, true, false);
    InnerTestCreateFileReader(false, false, false);
}

void HybridStorageTest::TestDirectory()
{
    auto storage = PrepareHybridStorage("", "");
    storage->MakeDirectory(GetPrimaryPath("testdir"));

    ASSERT_TRUE(FileSystemWrapper::IsExist(GetPrimaryPath("testdir")));
    ASSERT_FALSE(FileSystemWrapper::IsExist(GetSecondaryPath("testdir")));

    FileSystemWrapper::MkDir(GetSecondaryPath("testdir"));

    storage->RemoveDirectory(GetPrimaryPath("testdir"), false);

    ASSERT_FALSE(FileSystemWrapper::IsExist(GetPrimaryPath("testdir")));
    ASSERT_TRUE(FileSystemWrapper::IsExist(GetSecondaryPath("testdir")));
}

void HybridStorageTest::TestIsExistAndListFile()
{
    string primaryStr = mPrimaryRoot + ":" + "test1;test2";
    string secondaryStr = mSecondaryRoot + ":" + "test1;test3";
    auto storage = PrepareHybridStorage(primaryStr, secondaryStr);
    string content = "helloworld";

    ASSERT_TRUE(storage->IsExist(GetPrimaryPath("test1")));
    ASSERT_TRUE(storage->IsExist(GetPrimaryPath("test2")));
    ASSERT_TRUE(storage->IsExist(GetPrimaryPath("test3")));
    ASSERT_FALSE(storage->IsExist(GetPrimaryPath("not_exists")));

    ASSERT_TRUE(storage->IsExistOnDisk(GetPrimaryPath("test1")));
    ASSERT_TRUE(storage->IsExistOnDisk(GetPrimaryPath("test2")));
    ASSERT_TRUE(storage->IsExistOnDisk(GetPrimaryPath("test3")));
    ASSERT_FALSE(storage->IsExistOnDisk(GetPrimaryPath("not_exists")));

    fslib::FileList fileList;
    storage->ListFile(mPrimaryRoot, fileList);
    ASSERT_THAT(fileList, ElementsAre(
                    string("test1"), string("test2"), string("test3")));
}

HybridStoragePtr HybridStorageTest::PrepareHybridStorage(
    const string& primaryData, const string& secondaryData, bool isOffline)
{
    PrepareData(primaryData);
    LoadConfigList loadConfigList = PrepareData(secondaryData);
    FileSystemOptions options;
    options.isOffline = isOffline;
    options.loadConfigList = loadConfigList;
    options.fileBlockCache = mFileBlockCache;
    HybridStoragePtr storage(new HybridStorage(mPrimaryRoot, mSecondaryRoot, mMemoryController));
    storage->Init(options);
    return storage;
}

void HybridStorageTest::TestFileInfo()
{
    string primaryStr = mPrimaryRoot + ":" + "test1;test2";
    string secondaryStr = mSecondaryRoot + ":" + "test1;test3;dir/test4";
    auto storage = PrepareHybridStorage(primaryStr, secondaryStr);

    string content = "helloworld";


    ASSERT_EQ(content.size(), storage->GetFileLength(GetPrimaryPath("test1")));
    ASSERT_EQ(content.size(), storage->GetFileLength(GetPrimaryPath("test2")));
    ASSERT_EQ(content.size(), storage->GetFileLength(GetPrimaryPath("test3")));
    ASSERT_EQ(content.size(), storage->GetFileLength(GetPrimaryPath("dir/test4")));


    auto checkFileStat = [this, storage](const std::string& fileName, bool inCache, bool onDisk) {
        FileStat fileStat;
        storage->GetFileStat(GetPrimaryPath(fileName), fileStat);
        ASSERT_EQ(onDisk, fileStat.onDisk);
        ASSERT_EQ(inCache, fileStat.inCache);
    };

    checkFileStat("test1", false, true);
    checkFileStat("test2", false, true);
    checkFileStat("test3", false, true);
}

// fileInfos: "root1:file1;file2;a/b/c/fille3"
LoadConfigList HybridStorageTest::PrepareData(const string& fileInfos)
{
    LoadConfigList loadConfigList;
    if (fileInfos.empty())
    {
        return loadConfigList;
    }
    StringTokenizer st(fileInfos, ":",
                              StringTokenizer::TOKEN_IGNORE_EMPTY
                              | StringTokenizer::TOKEN_TRIM);
    assert(st.getNumTokens() == 2u);
    const string& rootPath = st[0];

    StringTokenizer files(st[1], ";",
                              StringTokenizer::TOKEN_IGNORE_EMPTY
                              | StringTokenizer::TOKEN_TRIM);
    string content = "helloworld";

    for (size_t i = 0; i < files.getNumTokens(); ++i)
    {
        string filePath = PathUtil::JoinPath(rootPath, files[i]);
        string parentDir = PathUtil::GetParentDirPath(filePath);
        FileSystemWrapper::MkDirIfNotExist(parentDir);
        LoadConfig loadConfig;
        loadConfig.SetRemote(true);
        string relativePath;
        PathUtil::GetRelativePath(rootPath, filePath, relativePath);
        loadConfig.SetFilePatternString({relativePath});
        loadConfigList.PushBack(loadConfig);
        if (filePath.back() != '/')
        {
            FileSystemWrapper::AtomicStore(filePath, content);
        }
    }
    return loadConfigList;
}

void HybridStorageTest::TestPackageFile()
{
    auto storage = PrepareHybridStorage("", "");
    MakePackageFile("second_root/s1", "file1:abcd#index/file2:12345");
    MakePackageFile("root/s2", "file3:abce#index/file4:2345");
    MakePackageFile("root/s3", "file3:abce#index/file4:2345");

    ASSERT_TRUE(storage->MountPackageFile(GetPrimaryPath("s1/package_file")));
    ASSERT_TRUE(storage->MountPackageFile(GetPrimaryPath("s2/package_file")));

    CheckPackageFile(storage, "s1/file1", "abcd", FSOT_MMAP);
    CheckPackageFile(storage, "s1/index/file2", "12345", FSOT_MMAP);
    CheckPackageFile(storage, "s2/file3", "abce", FSOT_MMAP);
    CheckPackageFile(storage, "s2/index/file4", "2345", FSOT_MMAP);
}

void HybridStorageTest::TestSepratePackageFile()
{
    string rootPath = GET_TEST_DATA_PATH();
    PackageFileUtil::CreatePackageFile(rootPath, "tmp",
            "file1:abcd#index/file2:12345;summary/file3:xyz", "deletionmap:attribute");
    FileSystemWrapper::Rename(PathUtil::JoinPath(rootPath, "tmp", "package_file.__data__0"),
                              PathUtil::JoinPath(mPrimaryRoot, "s1", "package_file.__data__0"));
    FileSystemWrapper::Rename(PathUtil::JoinPath(rootPath, "tmp", "package_file.__data__1"),
                              PathUtil::JoinPath(mSecondaryRoot, "s1", "package_file.__data__1"));
    FileSystemWrapper::Rename(PathUtil::JoinPath(rootPath, "tmp", "package_file.__meta__"),
                              PathUtil::JoinPath(mSecondaryRoot, "s1", "package_file.__meta__"));

    auto storage = PrepareHybridStorage("", "");
    storage->MountPackageFile(PathUtil::JoinPath(mPrimaryRoot, "s1", "package_file"));

    CheckPackageFile(storage, "s1/file1", "abcd", FSOT_MMAP);
    CheckPackageFile(storage, "s1/index/file2", "12345", FSOT_MMAP);
    CheckPackageFile(storage, "s1/summary/file3", "xyz", FSOT_MMAP);
}

void HybridStorageTest::TestPackageFileAndSharedMMap()
{
    string rootPath = GET_TEST_DATA_PATH();
    PackageFileUtil::CreatePackageFile(rootPath, "tmp",
            "summary/data:xyz:MERGE;"
            "index/str/offset:offset:MERGE#index/str/data:12345:MERGE;"
            "index/aitheta/data:aitheta:AI;"
            "attribute/long/patch.0:patch;",
            "deletionmap:attribute:index");

    string tmpPath = PathUtil::JoinPath(rootPath, "tmp");
    string s1Path = PathUtil::JoinPath(mSecondaryRoot, "s1");
    fslib::FileList fileList;
    FileSystemWrapper::ListDir(tmpPath, fileList);
    for (size_t i = 0; i < fileList.size(); i++)
    {
        fileList[i] = PathUtil::JoinPath(tmpPath, fileList[i]);
    }
    DirectoryMerger::MoveFiles(fileList, s1Path);
    
    string jsonStr = R"({
        "load_config" :
            [{ "file_patterns" : ["package_file.__data__.MERGE.[0-9]+"], "remote": true,
               "load_strategy" : "mmap", "load_strategy_param": { "lock": true } },
             { "file_patterns" : ["package_file.__data__.AI.[0-9]+"], "remote": true,
               "load_strategy" : "mmap", "load_strategy_param": { "lock": false } }
            ]
    })";
    
    FileSystemOptions options;
    options.loadConfigList = LoadConfigListCreator::CreateLoadConfigListFromJsonString(jsonStr);
    options.fileBlockCache = mFileBlockCache;
    HybridStoragePtr storage(new HybridStorage(mPrimaryRoot, mSecondaryRoot, mMemoryController));
    storage->Init(options);
    storage->MountPackageFile(PathUtil::JoinPath(mPrimaryRoot, "s1", "package_file"));
    storage->CleanCache();
    ASSERT_EQ(0, mMemoryController->GetUsedQuota());

    unique_ptr<HybridStorage, void(*)(HybridStorage*)> uniquePtr(storage.get(),
            [](HybridStorage* s){if(s) s->CleanCache();});
    {
        ASSERT_EQ(4096 + 5, FileSystemWrapper::GetFileLength(mSecondaryRoot + "/s1/package_file.__data__.MERGE.1"));
        auto reader1 = CheckPackageFile(storage, "s1/summary/data", "xyz", FSOT_MMAP);
        //size_t usedQuota = mMemoryController->GetUsedQuota();
        auto reader2 = CheckPackageFile(storage, "s1/index/str/offset", "offset", FSOT_BUFFERED);
        // package file length = 6 align to 4096 + tail 5, quota align to 4M
        ASSERT_EQ(4 * 1024 * 1024, mMemoryController->GetUsedQuota());
        auto reader3 = CheckPackageFile(storage, "s1/index/str/data", "12345", FSOT_MMAP);
        ASSERT_EQ(4 * 1024 * 1024, mMemoryController->GetUsedQuota());
        auto reader4 = CheckPackageFile(storage, "s1/index/aitheta/data", "aitheta", FSOT_IN_MEM);
        auto reader5 = CheckPackageFile(storage, "s1/attribute/long/patch.0", "patch", FSOT_BUFFERED);
        ASSERT_EQ(4 * 1024 * 1024, mMemoryController->GetUsedQuota());
        ASSERT_TRUE(storage->IsExist(GetPrimaryPath("s1/attribute")));
        ASSERT_TRUE(storage->IsExist(GetPrimaryPath("s1/index")));
        ASSERT_TRUE(storage->IsExist(GetPrimaryPath("s1/deletionmap")));

        auto fileNode1 = DYNAMIC_POINTER_CAST(MmapFileNode, reader1->GetFileNode());
        ASSERT_TRUE(fileNode1->mDependFileNode);
        auto fileNode2 = DYNAMIC_POINTER_CAST(MmapFileNode, reader2->GetFileNode());
        auto fileNode3 = DYNAMIC_POINTER_CAST(MmapFileNode, reader3->GetFileNode());
        ASSERT_TRUE(fileNode2->mDependFileNode);
        ASSERT_NE(fileNode1->mDependFileNode, fileNode2->mDependFileNode);
        ASSERT_EQ(fileNode2->mDependFileNode, fileNode3->mDependFileNode);
        ASSERT_EQ(3, fileNode2->mDependFileNode.use_count());
        std::tr1::weak_ptr<FileNode> sharedPackageFile = fileNode2->mDependFileNode;
        ASSERT_EQ(3, sharedPackageFile.use_count());
        fileNode2->Close();
        ASSERT_EQ(2, sharedPackageFile.use_count());
        fileNode3->Close();
        ASSERT_EQ(1, sharedPackageFile.use_count());
        storage->CleanCache();
        ASSERT_TRUE(sharedPackageFile.expired());

        ASSERT_EQ(FSOT_IN_MEM, reader4->GetOpenType());
        ASSERT_EQ(FSOT_IN_MEM, reader4->GetFileNode()->GetOpenType());
        ASSERT_EQ(FSOT_BUFFERED, reader5->GetOpenType());
        ASSERT_EQ(FSOT_BUFFERED, reader5->GetFileNode()->GetOpenType());
    }
}

void HybridStorageTest::TestPackageFileAndSharedMMapWithBufferedFile()
{
    string rootPath = GET_TEST_DATA_PATH();
    PackageFileUtil::CreatePackageFile(rootPath, "tmp",
            "index/str/offset:offset:MERGE#index/str/data:12345:MERGE;"
            "summary/data:xyz:MERGE;"
            "attribute/long/patch.0:patch;",
            "deletionmap:attribute:index");

    string tmpPath = PathUtil::JoinPath(rootPath, "tmp");
    string s1Path = PathUtil::JoinPath(mSecondaryRoot, "s1");
    fslib::FileList fileList;
    FileSystemWrapper::ListDir(tmpPath, fileList);
    for (size_t i = 0; i < fileList.size(); i++)
    {
        fileList[i] = PathUtil::JoinPath(tmpPath, fileList[i]);
    }
    DirectoryMerger::MoveFiles(fileList, s1Path);
    
    string jsonStr = R"({
        "load_config" :
            [{ "file_patterns" : ["package_file.__data__.MERGE.[0-9]+"],
               "load_strategy" : "mmap", "load_strategy_param": { "lock": true } }
            ]
    })";
    
    FileSystemOptions options;
    options.loadConfigList = LoadConfigListCreator::CreateLoadConfigListFromJsonString(jsonStr);
    options.fileBlockCache = mFileBlockCache;
    HybridStoragePtr storage(new HybridStorage(mPrimaryRoot, mSecondaryRoot, mMemoryController));
    storage->Init(options);
    storage->MountPackageFile(PathUtil::JoinPath(mPrimaryRoot, "s1", "package_file"));

    {
        CheckPackageFile(storage, "s1/index/str/offset", "offset", FSOT_BUFFERED);
    }
    {
        CheckPackageFile(storage, "s1/index/str/offset", "offset", FSOT_MMAP);
    }
    // storage->CleanCache();
    // {
    //     auto reader1 = CheckPackageFile(storage, "s1/index/str/offset", "offset", FSOT_BUFFERED);
    //     auto reader2 = CheckPackageFile(storage, "s1/index/str/data", "12345", FSOT_MMAP);
    //     auto fileNode1 = DYNAMIC_POINTER_CAST(BufferedFileNode, reader1->GetFileNode());
    //     auto fileNode2 = DYNAMIC_POINTER_CAST(MmapFileNode, reader2->GetFileNode());
    //     ASSERT_TRUE(fileNode1->mDependFileNode);
    //     ASSERT_EQ(fileNode1->mDependFileNode, fileNode2->mDependFileNode);
    //     ASSERT_EQ(2, fileNode1->mDependFileNode.use_count());
    // }
    storage->CleanCache();
}

void HybridStorageTest::TestCleanSharePackageFileWithFileOpened()
{
    PackageFileUtil::CreatePackageFile(mSecondaryRoot, "s1",
            "index/str/offset:offset#index/str/data:12345;", "");
    string jsonStr = R"({ 
            "load_config" :
                [{ "file_patterns" : ["package_file.__data__[0-9]+"], "remote": true,
                   "load_strategy" : "mmap", "load_strategy_param": { "lock": true } }]
        })";
    FileSystemOptions options;
    options.loadConfigList = LoadConfigListCreator::CreateLoadConfigListFromJsonString(jsonStr);

    auto GetStorage = [&]() {
        HybridStoragePtr storage(new HybridStorage(mPrimaryRoot, mSecondaryRoot,
                                                   mMemoryController));
        storage->Init(options);
        storage->MountPackageFile(PathUtil::JoinPath(mPrimaryRoot, "s1", "package_file"));
        return storage;
    };
    auto GetMMapFileNode = [&](const HybridStoragePtr& storage,
                               const string& fileName, const string& content) {
        return DYNAMIC_POINTER_CAST(MmapFileNode,
                CheckPackageFile(storage, fileName, content, FSOT_MMAP)->GetFileNode());
    };
    
    {
        HybridStoragePtr storage = GetStorage();
        auto fileNode1 = GetMMapFileNode(storage, "s1/index/str/offset", "offset");
        ASSERT_TRUE(fileNode1->mDependFileNode);
        {
            auto fileNode2 = GetMMapFileNode(storage, "s1/index/str/data", "12345");
            ASSERT_EQ(fileNode1->mDependFileNode, fileNode2->mDependFileNode);
        }
        ASSERT_EQ(3, fileNode1->mDependFileNode.use_count());
        {
            auto fileNode3 = GetMMapFileNode(storage, "s1/index/str/data", "12345");
            ASSERT_EQ(fileNode1->mDependFileNode, fileNode3->mDependFileNode);
        }
        auto fileNode4 = GetMMapFileNode(storage, "s1/index/str/offset", "offset");
        ASSERT_EQ(fileNode1->mDependFileNode, fileNode4->mDependFileNode);
        
        ASSERT_EQ(3, fileNode1->mDependFileNode.use_count());
        storage->CleanCache();
        {
            auto fileNode5 = GetMMapFileNode(storage, "s1/index/str/data", "12345");
            ASSERT_EQ(fileNode1->mDependFileNode, fileNode5->mDependFileNode);
        }
        auto fileNode6 = GetMMapFileNode(storage, "s1/index/str/offset", "offset");
        ASSERT_EQ(fileNode1->mDependFileNode, fileNode6->mDependFileNode);
    }
    {
        HybridStoragePtr storage = GetStorage();
        tr1::weak_ptr<FileNode> sharedFileNode;
        {
            auto fileNode1 = GetMMapFileNode(storage, "s1/index/str/offset", "offset");
            ASSERT_TRUE(fileNode1->mDependFileNode);
            sharedFileNode = fileNode1->mDependFileNode;
        }
        {
            auto fileNode2 = GetMMapFileNode(storage, "s1/index/str/data", "12345");
            ASSERT_EQ(sharedFileNode.lock(), fileNode2->mDependFileNode);
        }
    }
    {
        HybridStoragePtr storage = GetStorage();
        tr1::weak_ptr<FileNode> sharedFileNode;
        {
            auto fileNode1 = GetMMapFileNode(storage, "s1/index/str/offset", "offset");
            ASSERT_TRUE(fileNode1->mDependFileNode);
            sharedFileNode = fileNode1->mDependFileNode;
        }
        storage->CleanCache();
        ASSERT_TRUE(sharedFileNode.expired());
    }
}

void HybridStorageTest::TestFileNodeCreator()
{
    string primaryStr = mPrimaryRoot + ":" + "common;primary";
    string secondaryStr = mSecondaryRoot + ":" + "common;secondary";

    PrepareData(primaryStr);
    PrepareData(secondaryStr);
    FileSystemOptions options;
    string jsonStr = R"( {
        "load_config" : [
        {
            "file_patterns" : ["secondary"],
            "load_strategy" : "mmap",
            "remote": true
        }
        ]
    } )";
    
    options.loadConfigList =
        LoadConfigListCreator::CreateLoadConfigListFromJsonString(jsonStr);
    options.fileBlockCache = mFileBlockCache;

    {
        SCOPED_TRACE("all in local");
        HybridStoragePtr storage(new HybridStorage(mPrimaryRoot, mSecondaryRoot, mMemoryController));
        storage->Init(options);

        CheckReader(storage, "primary", FSOT_LOAD_CONFIG, FSFT_MMAP);
        CheckReader(storage, "primary", FSOT_MMAP, FSFT_MMAP);
        ASSERT_ANY_THROW(CheckReader(storage, "primary", FSOT_CACHE, FSFT_BLOCK));
        CheckReader(storage, "primary", FSOT_BUFFERED, FSFT_BUFFERED);
        CheckReader(storage, "primary", FSOT_IN_MEM, FSFT_IN_MEM);

        CheckReader(storage, "secondary", FSOT_LOAD_CONFIG, FSFT_MMAP);
        CheckReader(storage, "secondary", FSOT_MMAP, FSFT_MMAP);
        ASSERT_ANY_THROW(CheckReader(storage, "secondary", FSOT_CACHE, FSFT_BLOCK));
        CheckReader(storage, "secondary", FSOT_BUFFERED, FSFT_BUFFERED);
        CheckReader(storage, "secondary", FSOT_IN_MEM, FSFT_IN_MEM);

        CheckReader(storage, "common", FSOT_LOAD_CONFIG, FSFT_MMAP);
        CheckReader(storage, "common", FSOT_MMAP, FSFT_MMAP);
        ASSERT_ANY_THROW(CheckReader(storage, "common", FSOT_CACHE, FSFT_BLOCK));
        CheckReader(storage, "common", FSOT_BUFFERED, FSFT_BUFFERED);
        CheckReader(storage, "common", FSOT_IN_MEM, FSFT_IN_MEM);
    }

    {
        HybridStoragePtr storage(new HybridStorage(mPrimaryRoot, mSecondaryRoot, mMemoryController));
        storage->mSecondarySupportMmap = false;
        storage->Init(options);

        CheckReader(storage, "primary", FSOT_LOAD_CONFIG, FSFT_MMAP);
        CheckReader(storage, "primary", FSOT_MMAP, FSFT_MMAP);
        ASSERT_ANY_THROW(CheckReader(storage, "primary", FSOT_CACHE, FSFT_BLOCK));
        CheckReader(storage, "primary", FSOT_BUFFERED, FSFT_BUFFERED);
        CheckReader(storage, "primary", FSOT_IN_MEM, FSFT_IN_MEM);

        CheckReader(storage, "secondary", FSOT_LOAD_CONFIG, FSFT_IN_MEM);
        CheckReader(storage, "secondary", FSOT_MMAP, FSFT_IN_MEM);
        ASSERT_ANY_THROW(CheckReader(storage, "secondary", FSOT_CACHE, FSFT_BLOCK));
        CheckReader(storage, "secondary", FSOT_BUFFERED, FSFT_BUFFERED);
        CheckReader(storage, "secondary", FSOT_IN_MEM, FSFT_IN_MEM);

        CheckReader(storage, "common", FSOT_LOAD_CONFIG, FSFT_MMAP);
        CheckReader(storage, "common", FSOT_MMAP, FSFT_MMAP);
        ASSERT_ANY_THROW(CheckReader(storage, "common", FSOT_CACHE, FSFT_BLOCK));
        CheckReader(storage, "common", FSOT_BUFFERED, FSFT_BUFFERED);
        CheckReader(storage, "common", FSOT_IN_MEM, FSFT_IN_MEM);

    }

    {
        HybridStoragePtr storage(new HybridStorage(mPrimaryRoot, mSecondaryRoot, mMemoryController));
        storage->mSecondarySupportMmap = false;
        storage->mSupportMmap = false;
        storage->Init(options);

        CheckReader(storage, "primary", FSOT_LOAD_CONFIG, FSFT_IN_MEM);
        CheckReader(storage, "primary", FSOT_MMAP, FSFT_IN_MEM);
        ASSERT_ANY_THROW(CheckReader(storage, "primary", FSOT_CACHE, FSFT_BLOCK));
        CheckReader(storage, "primary", FSOT_BUFFERED, FSFT_BUFFERED);
        CheckReader(storage, "primary", FSOT_IN_MEM, FSFT_IN_MEM);

        CheckReader(storage, "secondary", FSOT_LOAD_CONFIG, FSFT_IN_MEM);
        CheckReader(storage, "secondary", FSOT_MMAP, FSFT_IN_MEM);
        ASSERT_ANY_THROW(CheckReader(storage, "secondary", FSOT_CACHE, FSFT_BLOCK));
        CheckReader(storage, "secondary", FSOT_BUFFERED, FSFT_BUFFERED);
        CheckReader(storage, "secondary", FSOT_IN_MEM, FSFT_IN_MEM);

        CheckReader(storage, "common", FSOT_LOAD_CONFIG, FSFT_IN_MEM);
        CheckReader(storage, "common", FSOT_MMAP, FSFT_IN_MEM);
        ASSERT_ANY_THROW(CheckReader(storage, "common", FSOT_CACHE, FSFT_BLOCK));
        CheckReader(storage, "common", FSOT_BUFFERED, FSFT_BUFFERED);
        CheckReader(storage, "common", FSOT_IN_MEM, FSFT_IN_MEM);
    }
}

void HybridStorageTest::CheckReader(const StoragePtr& storage, const string& path,
                                    FSOpenType openType, FSFileType fileType)
{
    auto reader = storage->CreateFileReader(
            PathUtil::JoinPath(mPrimaryRoot, path), openType);
    ASSERT_EQ(openType, reader->GetOpenType())
        << "Path:" << path 
        << ", OpenType: " << openType
        << ", FileType: " << fileType;
    ASSERT_EQ(fileType, reader->GetFileNode()->GetType())
        << "Path:" << path 
        << ", OpenType: " << openType
        << ", FileType: " << fileType;
}

void HybridStorageTest::MakePackageFile(const string& dirName,
                                        const string& packageFileInfoStr)
{
    auto packageFileName = "package_file";
    DirectoryPtr partDirectory = GET_PARTITION_DIRECTORY();
    string tempDirName = dirName + "_tmp_";
    DirectoryPtr tempDirectory = 
        partDirectory->MakeInMemDirectory(tempDirName);
    
    PackageFileUtil::CreatePackageFile(tempDirectory,
            packageFileName, packageFileInfoStr, "");
    tempDirectory->Sync(true);

    string srcPackageFileName = PathUtil::JoinPath(tempDirectory->GetPath(),
            packageFileName);
    string dstPackageFileName = PathUtil::JoinPath(GET_TEST_DATA_PATH() + dirName,
            packageFileName);

    FileSystemWrapper::Rename(srcPackageFileName + PACKAGE_FILE_META_SUFFIX,
                              dstPackageFileName + PACKAGE_FILE_META_SUFFIX);
    FileSystemWrapper::Rename(srcPackageFileName + PACKAGE_FILE_DATA_SUFFIX + "0",
                              dstPackageFileName + PACKAGE_FILE_DATA_SUFFIX + "0");
    partDirectory->RemoveDirectory(tempDirName, false);
}

FileReaderPtr HybridStorageTest::CheckPackageFile(
    const StoragePtr& storage, const string& fileName,
    const string& content, FSOpenType type)
{
    auto reader = storage->CreateFileReader(GetPrimaryPath(fileName), type);
    reader->Open();
    EXPECT_EQ(content.size(), reader->GetLength());
    vector<char> buffer;
    buffer.resize(content.size());
    reader->Read(buffer.data(), buffer.size());
    EXPECT_EQ(content, string(buffer.data(), buffer.size()));
    EXPECT_EQ(reader->GetPath(), GetPrimaryPath(fileName));
    return reader;
};

void HybridStorageTest::TestMmapInDfs()
{
    string jsonStr = R"( {
        "load_config" : [
        {
            "file_patterns" : [".*"],
            "load_strategy" : "mmap",
            "remote": true,
            "load_strategy_param": {
                "slice" : 8192,
                "interval" : 10,
                "lock":true

             }
        }
        ]
    } )";

    FileSystemOptions options;
    options.loadConfigList =
        LoadConfigListCreator::CreateLoadConfigListFromJsonString(jsonStr);
    // options.loadConfigList.DisableLoadSpeedLimit();
    HybridStorage storage(mPrimaryRoot, mSecondaryRoot, mMemoryController);
    storage.mSecondarySupportMmap = false;
    
    storage.Init(options);
    string simpleContent = "helloworld";
    FileSystemWrapper::AtomicStore(FileSystemWrapper::JoinPath(mPrimaryRoot, "file"), simpleContent);
    FileSystemWrapper::AtomicStore(FileSystemWrapper::JoinPath(mSecondaryRoot, "file"), simpleContent);
    auto fileNode = storage.CreateFileNode(
            FileSystemWrapper::JoinPath(mPrimaryRoot, "file"),
            FSOT_LOAD_CONFIG, true);
    ASSERT_TRUE(fileNode);
    EXPECT_EQ(FSFT_IN_MEM, fileNode->GetType());
    auto inMemFileNode = DYNAMIC_POINTER_CAST(InMemFileNode, fileNode);
    ASSERT_TRUE(inMemFileNode);
    auto strategy = inMemFileNode->mLoadStrategy;
    ASSERT_TRUE(strategy);
    ASSERT_TRUE(strategy->mLoadSpeedLimitSwitch);
    EXPECT_TRUE(strategy->mLoadSpeedLimitSwitch->IsSwitchOn());
    EXPECT_EQ(8192u, strategy->GetSlice());
    EXPECT_EQ(10u, strategy->GetInterval());

    string content;
    size_t len = 8192 * 2 + 123;
    for (size_t i = 0; i < len; ++i)
    {
        content += static_cast<char>(i);
    }
    FileSystemWrapper::AtomicStore(FileSystemWrapper::JoinPath(mPrimaryRoot, "largefile"), content);
    FileSystemWrapper::AtomicStore(FileSystemWrapper::JoinPath(mSecondaryRoot, "largefile"), content);    

    auto reader = storage.CreateFileReader(
            PathUtil::JoinPath(mPrimaryRoot, "largefile"), FSOT_LOAD_CONFIG);
    reader->Open();
    EXPECT_EQ(FSOT_IN_MEM, reader->GetFileNode()->GetType());
    EXPECT_EQ(content.size(), reader->GetLength());
    auto base = reader->GetBaseAddress();
    ASSERT_TRUE(base);
    EXPECT_EQ(content.size(), reader->GetLength());
    EXPECT_EQ(content, string(static_cast<const char*>(base), content.size()));
}

void HybridStorageTest::TestMmapInDCache()
{
    string jsonStr = R"( {
        "load_config" : [
        {
            "file_patterns" : [".*"],
            "load_strategy" : "mmap",
            "remote": true,
            "load_strategy_param": {
                "slice" : 4096,
                "interval" : 20,
                 "lock":true
             }
        }
        ]
    } )";

    FileSystemOptions options;
    options.loadConfigList =
        LoadConfigListCreator::CreateLoadConfigListFromJsonString(jsonStr);
    HybridStorage storage(mPrimaryRoot, mSecondaryRoot, mMemoryController);
    storage.mSecondarySupportMmap = true;
    
    storage.Init(options);
    string simpleContent = "helloworld";
    FileSystemWrapper::AtomicStore(FileSystemWrapper::JoinPath(mPrimaryRoot, "file"), simpleContent);
    FileSystemWrapper::AtomicStore(FileSystemWrapper::JoinPath(mSecondaryRoot, "file"), simpleContent);
    auto fileNode = storage.CreateFileNode(
            FileSystemWrapper::JoinPath(mPrimaryRoot, "file"),
            FSOT_LOAD_CONFIG, true);
    ASSERT_TRUE(fileNode);
    auto mmapFileNode = DYNAMIC_POINTER_CAST(MmapFileNode, fileNode);
    ASSERT_TRUE(mmapFileNode);
    auto strategy = mmapFileNode->mLoadStrategy;
    ASSERT_TRUE(strategy);
    ASSERT_TRUE(strategy->mLoadSpeedLimitSwitch);
    EXPECT_TRUE(strategy->mLoadSpeedLimitSwitch->IsSwitchOn());
    EXPECT_EQ(4096u, strategy->GetSlice());
    EXPECT_EQ(20u, strategy->GetInterval());
    EXPECT_TRUE(strategy->IsLock());

    string content;
    size_t len = 8192 * 2 + 123;
    for (size_t i = 0; i < len; ++i)
    {
        content += static_cast<char>(i);
    }
    FileSystemWrapper::AtomicStore(FileSystemWrapper::JoinPath(mPrimaryRoot, "largefile"), content);
    FileSystemWrapper::AtomicStore(FileSystemWrapper::JoinPath(mSecondaryRoot, "largefile"), content);    

    auto reader = storage.CreateFileReader(
            PathUtil::JoinPath(mPrimaryRoot, "largefile"), FSOT_LOAD_CONFIG);
    reader->Open();
    EXPECT_EQ(FSFT_MMAP_LOCK, reader->GetFileNode()->GetType());
    EXPECT_EQ(content.size(), reader->GetLength());
    auto base = reader->GetBaseAddress();
    ASSERT_TRUE(base);
    EXPECT_EQ(content.size(), reader->GetLength());
    EXPECT_EQ(content, string(static_cast<const char*>(base), content.size()));
}

void HybridStorageTest::TestUsePathMetaCache()
{
    DoTestUsePathMetaCache(true, true);
    DoTestUsePathMetaCache(true, false);
    DoTestUsePathMetaCache(false, true);
    DoTestUsePathMetaCache(false, false);
}

void HybridStorageTest::DoTestUsePathMetaCache(bool usePathMetaCache, bool isOffline)
{
    auto PPATH = [&](const string& name) { return mPrimaryRoot + "/" + name; };
    auto SPATH = [&](const string& name) { return mSecondaryRoot + "/" + name; };

    auto INIT_PATH_META_CONTAINER = [](const StoragePtr& storage, const string& root, const string& names) {
        for (const string& name : StringUtil::split(names, ";"))
        {
            for (size_t i = 0; i < name.size() - 1; ++i)
            {
                if (name[i] == '/')
                {
                    storage->AddPathFileInfo(root, FileInfo(name.substr(0, i+1), -1, 1));
                }
            }
            storage->AddPathFileInfo(root, FileInfo(name, name.back() == '/' ? -1 : 10, 1));
        }
    };
    auto STORAGE = [&](const string& primaryFileNames, const string& secondaryFileNames) {
        FileSystemWrapper::DeleteDir(mPrimaryRoot );
        FileSystemWrapper::MkDir(mPrimaryRoot);
        FileSystemWrapper::DeleteDir(mSecondaryRoot);
        FileSystemWrapper::MkDir(mSecondaryRoot);
        auto storage = PrepareHybridStorage(
            primaryFileNames.size() == 0 ? "" : mPrimaryRoot + ":" +  primaryFileNames,
            secondaryFileNames.size() == 0 ? "" : mSecondaryRoot + ":" + secondaryFileNames,
            isOffline);
        if (usePathMetaCache)
        {
            storage->InitPathMetaContainer();
            // online use logical path while offline use physical path as key
            INIT_PATH_META_CONTAINER(storage, mPrimaryRoot, primaryFileNames);
            INIT_PATH_META_CONTAINER(storage, isOffline ? mSecondaryRoot : mPrimaryRoot, secondaryFileNames);
        }
        return storage;
    };

    {
        ASSERT_TRUE(STORAGE("a", "")->IsExist(PPATH("a")));
        ASSERT_TRUE(STORAGE("", "a")->IsExist(PPATH("a")));
        ASSERT_TRUE(STORAGE("a", "")->IsExistOnDisk(PPATH("a")));
        ASSERT_TRUE(STORAGE("", "a")->IsExistOnDisk(PPATH("a")));
        
        ASSERT_NO_THROW(STORAGE("a", "")->Validate(PPATH("a"), 10));
        ASSERT_NO_THROW(STORAGE("", "a")->Validate(PPATH("a"), 10));
        ASSERT_EQ(10, STORAGE("a", "")->GetFileLength(PPATH("a")));
        ASSERT_EQ(10, STORAGE("", "a")->GetFileLength(PPATH("a")));
        {
            fslib::FileMeta meta;
            STORAGE("a", "")->GetFileMeta(PPATH("a"), meta);
            ASSERT_EQ(10, meta.fileLength);
        }
        {
            fslib::FileMeta meta;
            STORAGE("", "a")->GetFileMeta(PPATH("a"), meta);
            ASSERT_EQ(10, meta.fileLength);
        }
        
        ASSERT_TRUE(STORAGE("a/b", "")->IsDir(PPATH("a")));
        ASSERT_TRUE(STORAGE("", "a/b")->IsDir(PPATH("a")));
        ASSERT_TRUE(STORAGE("a/b", "a/c")->IsDir(PPATH("a")));
        ASSERT_FALSE(STORAGE("a", "")->IsDir(PPATH("a")));
        ASSERT_FALSE(STORAGE("", "a")->IsDir(PPATH("a")));
        ASSERT_FALSE(STORAGE("a", "a")->IsDir(PPATH("a")));
    }
    {
        ASSERT_NO_THROW(STORAGE("a", "a")->RemoveFile(PPATH("a"), false));
        ASSERT_FALSE(FileSystemWrapper::IsExist(PPATH("a")));
        ASSERT_TRUE(FileSystemWrapper::IsExist(SPATH("a")));
    
        ASSERT_NO_THROW(STORAGE("a", "")->RemoveFile(PPATH("a"), false));
        ASSERT_FALSE(FileSystemWrapper::IsExist(SPATH("a")));

        ASSERT_NO_THROW(STORAGE("", "a")->RemoveFile(PPATH("a"), false));
        ASSERT_TRUE(FileSystemWrapper::IsExist(SPATH("a")));
    }
    {
        ASSERT_NO_THROW(STORAGE("a/b", "a/c")->RemoveDirectory(PPATH("a/"), false));
        ASSERT_FALSE(FileSystemWrapper::IsExist(PPATH("a/")));
        ASSERT_TRUE(FileSystemWrapper::IsExist(SPATH("a/")));
    
        ASSERT_NO_THROW(STORAGE("a/b", "")->RemoveDirectory(PPATH("a/"), false));
        ASSERT_FALSE(FileSystemWrapper::IsExist(SPATH("a/")));

        ASSERT_NO_THROW(STORAGE("", "a/;a/b")->RemoveDirectory(PPATH("a/"), false));
        ASSERT_TRUE(FileSystemWrapper::IsExist(SPATH("a/")));
    }
    {
        ASSERT_NO_THROW(STORAGE("", "a/b")->MakeDirectory(PPATH("a/"), false));
        ASSERT_TRUE(FileSystemWrapper::IsExist(PPATH("a/")));
        ASSERT_TRUE(FileSystemWrapper::IsExist(SPATH("a/b")));
    }
    {
        fslib::FileList fileList;
        STORAGE("a/b", "a/c")->ListFile(PPATH(""), fileList, true, true, true);
        EXPECT_THAT(fileList, UnorderedElementsAre("a/", "a/b"));
        
        fileList.clear();
        STORAGE("a/b", "a/c")->ListFile(PPATH(""), fileList, true, true, false);
        EXPECT_THAT(fileList, UnorderedElementsAre("a/", "a/b", "a/c"));
        
        fileList.clear();
        STORAGE("a", "")->ListFile(PPATH(""), fileList, true, true, false);
        EXPECT_THAT(fileList, UnorderedElementsAre("a"));
        
        fileList.clear();
        STORAGE("", "a")->ListFile(PPATH(""), fileList, true, true, false);
        EXPECT_THAT(fileList, UnorderedElementsAre("a"));

        fileList.clear();
        STORAGE("a", "a")->ListFile(PPATH(""), fileList, true, true, false);
        EXPECT_THAT(fileList, UnorderedElementsAre("a"));
    }
    {
        ASSERT_TRUE(STORAGE("a", "")->CreateFileNode(PPATH("a"), FSOT_BUFFERED, true));
        ASSERT_TRUE(STORAGE("", "a")->CreateFileNode(PPATH("a"), FSOT_BUFFERED, true));
        ASSERT_TRUE(STORAGE("a", "")->CreateFileNode(PPATH("a"), FSOT_IN_MEM, true));
        ASSERT_TRUE(STORAGE("", "a")->CreateFileNode(PPATH("a"), FSOT_IN_MEM, true));
        ASSERT_TRUE(STORAGE("a", "")->CreateFileNode(PPATH("a"), FSOT_MMAP, true));
        ASSERT_TRUE(STORAGE("", "a")->CreateFileNode(PPATH("a"), FSOT_MMAP, true));
        ASSERT_TRUE(STORAGE("a", "")->CreateFileNode(PPATH("a"), FSOT_LOAD_CONFIG, true));
        ASSERT_TRUE(STORAGE("", "a")->CreateFileNode(PPATH("a"), FSOT_LOAD_CONFIG, true));
    }
}

IE_NAMESPACE_END(file_system);

