#include "indexlib/file_system/test/directory_unittest.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/storage/archive_folder.h"
#include "indexlib/file_system/load_config/mmap_load_strategy.h"
#include "indexlib/file_system/load_config/cache_load_strategy.h"
#include "indexlib/file_system/normal_compress_file_reader.h"
#include "indexlib/file_system/integrated_compress_file_reader.h"
#include "indexlib/file_system/block_cache_compress_file_reader.h"
#include "indexlib/file_system/compress_file_writer.h"
#include "indexlib/file_system/test/load_config_list_creator.h"
#include "indexlib/common/file_system_factory.h"

using namespace std;
using namespace fslib;
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, DirectoryTest);

DirectoryTest::DirectoryTest()
{
}

DirectoryTest::~DirectoryTest()
{
}

void DirectoryTest::CaseSetUp()
{
    mDirectory = GET_PARTITION_DIRECTORY();
    mRootDir = GET_TEST_DATA_PATH();
}

void DirectoryTest::CaseTearDown()
{
}

void DirectoryTest::TestStoreAndLoad()
{
    string testStr = "hello, directory";
    string fileName = "testdir.temp";
    ASSERT_NO_THROW(mDirectory->Store(fileName, testStr));
    string compStr;

    ASSERT_NO_THROW(mDirectory->Load(fileName, compStr));
    ASSERT_EQ(testStr, compStr);
}

void DirectoryTest::TestCreateIntegratedFileReader()
{
    DoTestCreateIntegratedFileReader("mmap", FSOT_LOAD_CONFIG);
    DoTestCreateIntegratedFileReader("cache", FSOT_IN_MEM);
}

void DirectoryTest::TestCreateCompressFileReader()
{
    MakeCompressFile("mmap_compress_file", 4096);
    MakeCompressFile("cache_decompress_file", 8192);
    MakeCompressFile("cache_decompress_file1", 4096);
    MakeCompressFile("cache_compress_file", 4096);
    
    string loadConfigStr = R"(
    {
        "load_config" :
        [{ "file_patterns" : ["mmap_compress_file"], "load_strategy" : "mmap" },
         { "file_patterns" : ["cache_decompress_file*"], "load_strategy" : "cache",
               "load_strategy_param" : { "global_cache" : false, "cache_decompress_file" : true }},
         { "file_patterns" : ["cache_compress_file"], "load_strategy" : "cache",
               "load_strategy_param" : { "global_cache" : false }}]
    })";

    IndexPartitionOptions options;
    options.GetOnlineConfig().loadConfigList =
        LoadConfigListCreator::CreateLoadConfigListFromJsonString(loadConfigStr);
    IndexlibFileSystemPtr fileSystem = FileSystemFactory::Create(
            GET_TEST_DATA_PATH(), "", options, NULL);
    DirectoryPtr rootDirectory = DirectoryCreator::Get(fileSystem, GET_TEST_DATA_PATH(), true);

    CheckReaderType(rootDirectory, "mmap_compress_file", COMP_READER_INTE);
    CheckReaderType(rootDirectory, "cache_decompress_file", COMP_READER_NORMAL);
    CheckReaderType(rootDirectory, "cache_decompress_file1", COMP_READER_CACHE);
    CheckReaderType(rootDirectory, "cache_compress_file", COMP_READER_NORMAL);
}

void DirectoryTest::TestProhibitInMemDump()
{
    IndexPartitionOptions options;
    options.GetOnlineConfig().prohibitInMemDump = true;
    IndexlibFileSystemPtr fileSystem = FileSystemFactory::Create(
            GET_TEST_DATA_PATH(), "", options, NULL);
    DirectoryPtr rootDirectory = DirectoryCreator::Get(fileSystem, GET_TEST_DATA_PATH(), true);
    DirectoryPtr inMemDir = rootDirectory->MakeInMemDirectory("in_mem_dir");
    inMemDir->Store("file", "abc", true);

    ASSERT_TRUE(FileSystemWrapper::IsExist(GET_TEST_DATA_PATH() + "/in_mem_dir/file"));

    FileStat fileStat;
    inMemDir->GetFileStat("file", fileStat);
    ASSERT_FALSE(fileStat.inCache);
}

void DirectoryTest::DoTestCreateIntegratedFileReader(string loadType, 
        FSOpenType expectOpenType)
{
    string caseDir = FileSystemWrapper::JoinPath(mRootDir, 
            loadType);
    FileSystemWrapper::MkDir(caseDir);
    config::IndexPartitionOptions option = CreateOptionWithLoadConfig(loadType);
    IndexlibFileSystemPtr fileSystem =
        FileSystemFactory::Create(caseDir, "", option, NULL);
    DirectoryPtr directory = DirectoryCreator::Get(fileSystem, caseDir, true);
    directory->Store("file_name", "file_content");
    FileReaderPtr fileReader = directory->CreateIntegratedFileReader(
            "file_name");
    ASSERT_EQ(expectOpenType, fileReader->GetOpenType());
    ASSERT_TRUE(fileReader->GetBaseAddress());
}

void DirectoryTest::TestPatialLock()
{
    LoadStrategy* strategy1 = new MmapLoadStrategy(true, true, false, 4 * 1024 * 1024, 0);
    LoadStrategy* strategy2 = new MmapLoadStrategy(false, true, false, 4 * 1024 * 1024, 0);
    LoadStrategy* strategy3 = new MmapLoadStrategy(false, false, false, 4 * 1024 * 1024, 0);
    LoadStrategy* strategy4 = new CacheLoadStrategy;
    config::IndexPartitionOptions option = CreateOptionWithLoadConfig({
                std::make_pair(".*\\.mmap_lock", strategy1),
                std::make_pair(".*\\.mmap_patial", strategy2),
                std::make_pair(".*\\.mmap", strategy3),
                std::make_pair(".*\\.cache", strategy4),
                    });
    string caseDir = FileSystemWrapper::JoinPath(mRootDir, 
            "patiallock");
    FileSystemWrapper::MkDir(caseDir);
    IndexlibFileSystemPtr fileSystem = FileSystemFactory::Create(caseDir, "", option, NULL);
    DirectoryPtr directory = DirectoryCreator::Get(fileSystem, caseDir, true);
    directory->Store("file.mmap", "file_content");
    directory->Store("file.mmap_lock", "file_content");
    directory->Store("file.mmap_patial", "file_content");
    directory->Store("file.cache", "file_content");
    ASSERT_FALSE(directory->OnlyPatialLock("file.mmap_lock"));
    ASSERT_FALSE(directory->OnlyPatialLock("file.mmap"));
    ASSERT_TRUE(directory->OnlyPatialLock("file.mmap_patial"));
    ASSERT_TRUE(directory->OnlyPatialLock("file.cache"));
}

void DirectoryTest::TestCreateIntegratedFileReaderNoReplace()
{
    string loadType = "cache";
    string caseDir = FileSystemWrapper::JoinPath(mRootDir, loadType);
    FileSystemWrapper::MkDir(caseDir);
    config::IndexPartitionOptions option = CreateOptionWithLoadConfig(loadType);
    IndexlibFileSystemPtr fileSystem =
        FileSystemFactory::Create(caseDir, "", option, NULL);
    DirectoryPtr directory = DirectoryCreator::Get(fileSystem, caseDir, true);
    directory->Store("file_name", "file_content");
    directory->CreateIntegratedFileReader("file_name");
    directory->CreateIntegratedFileReader("file_name");
    IndexlibFileSystemMetrics metrics = fileSystem->GetFileSystemMetrics();
    const StorageMetrics& localStorageMetrics = 
        metrics.GetLocalStorageMetrics();
    const FileCacheMetrics& fileCacheMetrics = 
        localStorageMetrics.GetFileCacheMetrics();
    ASSERT_EQ(0, fileCacheMetrics.replaceCount.getValue());
    ASSERT_EQ(1, fileCacheMetrics.hitCount.getValue());
    ASSERT_EQ(1, fileCacheMetrics.missCount.getValue());
    ASSERT_EQ(0, fileCacheMetrics.removeCount.getValue());
}

void DirectoryTest::TestAddSolidPathFileInfos()
{
    string caseDir = FileSystemWrapper::JoinPath(mRootDir, "solidPath");
    FileSystemWrapper::MkDir(caseDir);
    config::IndexPartitionOptions option = CreateOptionWithLoadConfig("mmap");
    option.SetIsOnline(false);

    IndexlibFileSystemPtr fileSystem =
        FileSystemFactory::Create(caseDir, "", option, NULL);
    DirectoryPtr directory = DirectoryCreator::Get(fileSystem, caseDir, true);

    directory->MakeDirectory("dir");
    directory->MakeDirectory("dir/sub_dir");
    directory->Store("dir/file1", "abc");
    // this will trigger add file2 to metaCache
    directory->Store("dir/file2", "efg");
    directory->Store("dir/sub_dir/file3", "efg");

    vector<FileInfo> fileInfos = {
        FileInfo("dir/"),
        FileInfo("dir/file1", 3),
        FileInfo("dir/sub_dir/"),
        FileInfo("dir/sub_dir/file3", 3)
    };

    // test exist when not add solid path
    ASSERT_TRUE(directory->IsExist("dir/file1"));
    ASSERT_TRUE(directory->IsExist("dir/file2"));
    ASSERT_TRUE(directory->IsExist("dir/sub_dir/file3"));

    // test exist after add solid path
    directory->AddSolidPathFileInfos(fileInfos.begin(), fileInfos.end());
    ASSERT_TRUE(directory->IsExist("dir/file1"));
    ASSERT_TRUE(directory->IsExist("dir/file2"));
    ASSERT_FALSE(directory->IsExist("dir/not-exist-file"));
    ASSERT_TRUE(directory->IsExist("dir/sub_dir/file3"));

    // test exist when solid path change
    FileSystemWrapper::DeleteFile(FileSystemWrapper::JoinPath(caseDir, "dir/file1"));
    ASSERT_TRUE(directory->IsExist("dir/file1"));

    FileList files;
    directory->ListFile("dir", files, true);
    ASSERT_EQ(4, files.size());

    // test remove data in solid path
    directory->RemoveDirectory("dir/sub_dir");
    ASSERT_TRUE(directory->IsExist("dir/file1"));
    ASSERT_FALSE(directory->IsExist("dir/sub_dir/file3"));

    // test remove solid path
    directory->RemoveDirectory("dir");
    ASSERT_FALSE(directory->IsExist("dir/file1"));
}

config::IndexPartitionOptions DirectoryTest::CreateOptionWithLoadConfig(
        string loadType)
{
    std::pair<std::string, LoadStrategy*> strategy(".*", nullptr);
    if (loadType == "mmap")
    {
        strategy.second = new MmapLoadStrategy;
    }
    else
    {
        strategy.second = new CacheLoadStrategy;
    }
    return CreateOptionWithLoadConfig({strategy});
}

config::IndexPartitionOptions DirectoryTest::CreateOptionWithLoadConfig(
        const std::vector<std::pair<std::string, LoadStrategy*>>& configs)
{
    LoadConfigList loadConfigList;
    for (auto& p: configs)
    {
        LoadConfig::FilePatternStringVector pattern;
        pattern.push_back(p.first);
        LoadConfig loadConfig;
        loadConfig.SetFilePatternString(pattern);
        LoadStrategyPtr loadStrategy(p.second);
        loadConfig.SetLoadStrategyPtr(loadStrategy);
        loadConfigList.PushBack(loadConfig);
    }
    config::IndexPartitionOptions option;
    option.GetOnlineConfig().loadConfigList = loadConfigList;
    return option;
}

void DirectoryTest::MakeCompressFile(const string& fileName, size_t bufferSize)
{
    string testStr = "hello, directory";
    CompressFileWriterPtr compWriter = GET_PARTITION_DIRECTORY()->CreateCompressFileWriter(
            fileName, "zstd", bufferSize);
    compWriter->Write(testStr.c_str(), testStr.size());
    compWriter->Close();
}

void DirectoryTest::CheckReaderType(const DirectoryPtr& dir, const string& fileName,
                                    CompressReaderType readerType)
{
    CompressFileReaderPtr compReader = dir->CreateCompressFileReader(
            fileName, FSOT_LOAD_CONFIG);
    switch (readerType)
    {
    case COMP_READER_NORMAL:
    {
        NormalCompressFileReaderPtr typedReader = DYNAMIC_POINTER_CAST(
                NormalCompressFileReader, compReader);
        ASSERT_TRUE(typedReader);
        break;
    }
    case COMP_READER_INTE:
    {
        IntegratedCompressFileReaderPtr typedReader = DYNAMIC_POINTER_CAST(
                IntegratedCompressFileReader, compReader);
        ASSERT_TRUE(typedReader);
        break;
    }
    default:
    {
        assert(readerType == COMP_READER_CACHE);
        BlockCacheCompressFileReaderPtr typedReader = DYNAMIC_POINTER_CAST(
                BlockCacheCompressFileReader, compReader);
        ASSERT_TRUE(typedReader);
    }
    }
}

void DirectoryTest::TestRename()
{
    IndexPartitionOptions options;
    IndexlibFileSystemPtr fileSystem = FileSystemFactory::Create(
            GET_TEST_DATA_PATH(), "", options, NULL);
    DirectoryPtr rootDirectory = DirectoryCreator::Get(fileSystem, GET_TEST_DATA_PATH(), true);
    rootDirectory->Store("f1", "abc", true);
    ASSERT_TRUE(FileSystemWrapper::IsExist(GET_TEST_DATA_PATH() + "/f1"));
    DirectoryPtr localDir1 = rootDirectory->MakeDirectory("local1");
    DirectoryPtr localDir2 = rootDirectory->MakeDirectory("local2");
    rootDirectory->Rename("f1", localDir1);
    ASSERT_FALSE(FileSystemWrapper::IsExist(GET_TEST_DATA_PATH() + "/file"));
    ASSERT_TRUE(FileSystemWrapper::IsExist(GET_TEST_DATA_PATH() + "/local1/f1"));

    localDir1->Rename("f1", localDir2, "f2.tmp");
    ASSERT_FALSE(FileSystemWrapper::IsExist(GET_TEST_DATA_PATH() + "/local1/f1"));
    ASSERT_TRUE(FileSystemWrapper::IsExist(GET_TEST_DATA_PATH() + "/local2/f2.tmp"));

    // DirectoryPtr inMemDir = rootDirectory->MakeInMemDirectory("in_mem_dir");
    // inMemDir->Store("file", "abc", true);
    // ASSERT_THROW(localDir2->Rename("f2.tmp", inMemDir), misc::UnSupportedException);
}

void DirectoryTest::TestGetArchiveFolder()
{
    const string content = "abcdef";
    auto PrepareArchiveFolder = [&content](const string& path) {
        if (!FileSystemWrapper::IsExist(path)) {
            FileSystemWrapper::MkDir(path);
        }
        ArchiveFolder archiveFolderWrite(false);
        archiveFolderWrite.Open(path, "i");
        FileWrapperPtr archiveFile = archiveFolderWrite.GetInnerFile("test1", fslib::WRITE);

        archiveFile->Write(content.c_str(), content.length());
        archiveFile->Close();
        archiveFolderWrite.Close();
    };
    auto CheckArchiveFolder = [&content](const ArchiveFolderPtr& folder) {
        FileWrapperPtr archiveFileToRead = folder->GetInnerFile("test1", fslib::READ);
        char buffer[10];
        archiveFileToRead->Read(buffer, content.length());
        string result(buffer, content.length());
        ASSERT_EQ(content, result);
        archiveFileToRead->Close();
        folder->Close();
    };
    
    IndexPartitionOptions options;
    options.SetIsOnline(true);
    
    {
        string primaryRoot = GET_PARTITION_DIRECTORY()->MakeDirectory("pri")->GetPath();
        string secondaryRoot = GET_PARTITION_DIRECTORY()->MakeDirectory("sec")->GetPath();
        PrepareArchiveFolder(primaryRoot + "/" + ADAPTIVE_DICT_DIR_NAME);
        IndexlibFileSystemPtr fs = FileSystemFactory::Create(
            primaryRoot, secondaryRoot, options, NULL);
        auto dir = DirectoryCreator::Get(fs, primaryRoot + "/" + ADAPTIVE_DICT_DIR_NAME, true);
        CheckArchiveFolder(dir->GetArchiveFolder());
    }
    TearDown();
    SetUp();
    {
        string primaryRoot = GET_PARTITION_DIRECTORY()->MakeDirectory("pri")->GetPath();
        string secondaryRoot = GET_PARTITION_DIRECTORY()->MakeDirectory("sec")->GetPath();

        string jsonStr = R"( {
            "need_read_remote_index" : true,
            "load_config" : [
                { "file_patterns" : [".*"], "remote" : true, "deploy": false }
            ]
        } )";
        OnlineConfig onlineConfig;
        FromJsonString(onlineConfig, jsonStr);
        options.SetOnlineConfig(onlineConfig);

        PrepareArchiveFolder(secondaryRoot + "/" + ADAPTIVE_DICT_DIR_NAME);
        IndexlibFileSystemPtr fs = FileSystemFactory::Create(
            primaryRoot, secondaryRoot, options, NULL);
        auto dir = DirectoryCreator::Get(fs, primaryRoot + "/" + ADAPTIVE_DICT_DIR_NAME, true);
        CheckArchiveFolder(dir->GetArchiveFolder());
    }
}

IE_NAMESPACE_END(file_system);

