#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/index_define.h"
#include "indexlib/file_system/test/local_directory_unittest.h"
#include "indexlib/file_system/test/package_file_util.h"
#include "indexlib/file_system/test/load_config_list_creator.h"

using namespace std;
using namespace autil;
using namespace fslib;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(index);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, LocalDirectoryTest);

LocalDirectoryTest::LocalDirectoryTest()
{
}

LocalDirectoryTest::~LocalDirectoryTest()
{
}

void LocalDirectoryTest::CaseSetUp()
{
}

void LocalDirectoryTest::CaseTearDown()
{
}

void LocalDirectoryTest::TestSimpleProcessForMountPackageFile()
{
    MakePackageFile("segment_0", "package_file", "file1:abcd#index/file2:12345");
    DirectoryPtr segDirectory = 
        GET_PARTITION_DIRECTORY()->GetDirectory("segment_0", true);

    ASSERT_FALSE(segDirectory->MountPackageFile("not_exist_package_file"));
    ASSERT_TRUE(segDirectory->MountPackageFile("package_file"));

    ASSERT_TRUE(segDirectory->IsExist("file1"));
    ASSERT_TRUE(segDirectory->IsExist("index/file2"));
    ASSERT_TRUE(segDirectory->IsExist("index"));
    ASSERT_TRUE(segDirectory->IsExist("index/"));

    ASSERT_EQ((size_t)4, segDirectory->GetFileLength("file1"));
    ASSERT_EQ((size_t)5, segDirectory->GetFileLength("index/file2"));

    fslib::FileList fileList;
    segDirectory->ListFile("", fileList, true);

    // meta + data, file1, file2, index
    ASSERT_EQ((size_t)5, fileList.size());
    ASSERT_EQ("file1", fileList[0]);
    ASSERT_EQ("index/", fileList[1]);
    ASSERT_EQ("index/file2", fileList[2]);
    ASSERT_EQ("package_file.__data__0", fileList[3]);
    ASSERT_EQ("package_file.__meta__", fileList[4]);

    CheckFileStat(segDirectory, "file1", "false,false,true,false,4");
    CheckFileStat(segDirectory, "index", "false,false,true,true,0");
    CheckFileStat(segDirectory, "index/file2", "false,false,true,false,5");

    fslib::FileList innerFileList;
    segDirectory->ListFile("index", innerFileList, false);
    ASSERT_EQ((size_t)1, innerFileList.size());
    ASSERT_EQ("file2", innerFileList[0]);
}

void LocalDirectoryTest::TestRemoveFromPackageFile()
{
    MakePackageFile("segment_0", "package_file", "file1:abcd#index/file2:12345");
    DirectoryPtr segDirectory = 
        GET_PARTITION_DIRECTORY()->GetDirectory("segment_0", true);

    ASSERT_TRUE(segDirectory->MountPackageFile("package_file"));
    segDirectory->RemoveFile("file1");
    ASSERT_FALSE(segDirectory->IsExist("file1"));
    ASSERT_ANY_THROW(segDirectory->RemoveFile("file_nonexist"));

    ASSERT_TRUE(segDirectory->IsExist("package_file.__meta__"));
    ASSERT_TRUE(segDirectory->IsExist("package_file.__data__0"));

    segDirectory->RemoveDirectory("index/");
    segDirectory->GetFileSystem()->CleanCache();
    ASSERT_FALSE(segDirectory->IsExist("index/"));
    ASSERT_FALSE(segDirectory->IsExist("index/file2"));
    ASSERT_FALSE(segDirectory->IsExist("package_file.__meta__"));
    ASSERT_FALSE(segDirectory->IsExist("package_file.__data__0"));
}

void LocalDirectoryTest::TestCreateFileReader()
{
    InnerCheckCreateFileReader(FSOT_IN_MEM);
    InnerCheckCreateFileReader(FSOT_MMAP);
    InnerCheckCreateFileReader(FSOT_CACHE);
    InnerCheckCreateFileReader(FSOT_BUFFERED);
    InnerCheckCreateFileReader(FSOT_LOAD_CONFIG);
}

void LocalDirectoryTest::TestListFile()
{
    MakePackageFile("segment_0", "package_file", 
                    "file1:abcd#index/file2:12345");
    DirectoryPtr segDirectory = 
        GET_PARTITION_DIRECTORY()->GetDirectory("segment_0", true);
    
    MakeFiles(segDirectory, "file3:file4");
    ASSERT_TRUE(segDirectory->MountPackageFile("package_file"));

    FileList filelist;
    // recursive = true, physical = false
    segDirectory->ListFile("", filelist, true, false);
    CheckFileList(filelist, "file1:file3:file4:index/:index/file2:"
                  "package_file.__data__0:package_file.__meta__");

    filelist.clear();
    // recursive = true, physical = true
    segDirectory->ListFile("", filelist, true, true);
    CheckFileList(filelist, "file3:file4:package_file.__data__0"
                  ":package_file.__meta__");

    filelist.clear();
    // recursive = false, physical = true
    segDirectory->ListFile("", filelist, true, true);
    CheckFileList(filelist, "file3:file4:package_file.__data__0"
                  ":package_file.__meta__");

    FileList innerFileList;
    // recursive = true, physical = true
    segDirectory->ListFile("index", innerFileList, true, true);
    CheckFileList(innerFileList, "");

    // recursive = false, physical = false
    segDirectory->ListFile("index", innerFileList, false, false);
    CheckFileList(innerFileList, "file2");
}

void LocalDirectoryTest::TestLoadInsertToCache()
{
    LoadConfigList loadConfigList;
    RESET_FILE_SYSTEM(loadConfigList, false, false);

    string fileName = "test_file";
    GET_PARTITION_DIRECTORY()->Store(fileName, "abc");

    string content;
    GET_PARTITION_DIRECTORY()->Load(fileName, content);
    ASSERT_FALSE(GET_PARTITION_DIRECTORY()->IsExistInCache(fileName));
    
    GET_PARTITION_DIRECTORY()->Load(fileName, content, true);
    ASSERT_TRUE(GET_PARTITION_DIRECTORY()->IsExistInCache(fileName));
}

void LocalDirectoryTest::CheckFileList(
        const FileList& filelist, const string& fileStr)
{
    vector<string> fileNames;
    StringUtil::fromString(fileStr, fileNames, ":");
    ASSERT_EQ(fileNames.size(), filelist.size());
    for (size_t i = 0; i < filelist.size(); i++)
    {
        ASSERT_EQ(fileNames[i], filelist[i]);
    }
}

void LocalDirectoryTest::MakeFiles(
        DirectoryPtr directoryPtr, const string& fileStr)
{
    vector<string> fileNames;
    StringUtil::fromString(fileStr, fileNames, ":");
    for (size_t i = 0; i < fileNames.size(); i++)
    {
        directoryPtr->Store(fileNames[i], "");
    }
}

void LocalDirectoryTest::InnerCheckCreateFileReader(FSOpenType openType)
{
    TearDown();
    SetUp();
    
    if (openType == FSOT_CACHE)
    {
        LoadConfigList loadConfigList = 
            LoadConfigListCreator::CreateLoadConfigList(READ_MODE_CACHE);
        RESET_FILE_SYSTEM(loadConfigList, false);
    }

    MakePackageFile("segment_0", "package_file", "file1:abcd#index/file2:12345");
    DirectoryPtr segDirectory = 
        GET_PARTITION_DIRECTORY()->GetDirectory("segment_0", true);
    ASSERT_TRUE(segDirectory->MountPackageFile("package_file"));

    CheckReader(segDirectory, "file1", openType, "abcd");
    CheckReader(segDirectory, "index/file2", openType, "12345");

    DirectoryPtr indexDirectory = segDirectory->GetDirectory("index", true);
    ASSERT_TRUE(indexDirectory);
    CheckReader(indexDirectory, "file2", openType, "12345");
}

void LocalDirectoryTest::MakePackageFile(const string& dirName,
        const string& packageFileName,
        const string& packageFileInfoStr)
{
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
    partDirectory->RemoveDirectory(tempDirName);
}

void LocalDirectoryTest::CheckReader(
        const DirectoryPtr& directory,
        const string& filePath,
        FSOpenType openType,
        const string& expectValue)
{
    FileReaderPtr fileReader = directory->CreateFileReader(filePath, openType);
    ASSERT_TRUE(fileReader);
    ASSERT_EQ(expectValue.size(), fileReader->GetLength());
    ASSERT_EQ(openType, fileReader->GetOpenType());

    string value;
    value.resize(expectValue.size());
    fileReader->Read((void*)value.data(), expectValue.size());
    ASSERT_EQ(value, expectValue);

    string expectPath = PathUtil::JoinPath(directory->GetPath(), filePath);
    ASSERT_EQ(expectPath, fileReader->GetPath());
}

// expectFileStatStr: isInCache,isOnDisk,isPackage,isDir,fileLen
void LocalDirectoryTest::CheckFileStat(
        const DirectoryPtr& directory,
        const string& path,
        const string& expectFileStatStr)
{
    vector<string> statInfo;
    StringUtil::fromString(expectFileStatStr, statInfo, ",");
    assert(statInfo.size() == 5);

    FileStat fileStat;
    directory->GetFileStat(path, fileStat);

    ASSERT_EQ(fileStat.inCache, statInfo[0] == string("true"));
    ASSERT_EQ(fileStat.onDisk, statInfo[1] == string("true"));
    ASSERT_EQ(fileStat.inPackage, statInfo[2] == string("true"));
    ASSERT_EQ(fileStat.isDirectory, statInfo[3] == string("true"));
    ASSERT_EQ(fileStat.fileLength, StringUtil::numberFromString<size_t>(statInfo[4]));
}


IE_NAMESPACE_END(file_system);

