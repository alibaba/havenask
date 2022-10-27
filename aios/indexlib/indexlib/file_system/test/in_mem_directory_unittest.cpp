#include "indexlib/file_system/test/in_mem_directory_unittest.h"
#include "indexlib/file_system/test/package_file_util.h"
#include "indexlib/file_system/link_directory.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/util/path_util.h"

using namespace std;
using namespace autil;
using namespace fslib;

IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, InMemDirectoryTest);

InMemDirectoryTest::InMemDirectoryTest()
{
}

InMemDirectoryTest::~InMemDirectoryTest()
{
}

void InMemDirectoryTest::CaseSetUp()
{
}

void InMemDirectoryTest::CaseTearDown()
{
}

void InMemDirectoryTest::TestSimpleProcessForPackageFile()
{
    DirectoryPtr partDirectory = GET_PARTITION_DIRECTORY();
    DirectoryPtr inMemDirectory = partDirectory->MakeInMemDirectory("dump_dir");

    PackageFileWriterPtr packageFileWriter = 
        inMemDirectory->GetPackageFileWriter("package_file");
    ASSERT_FALSE(packageFileWriter);

    packageFileWriter = inMemDirectory->CreatePackageFileWriter("package_file");
    ASSERT_TRUE(packageFileWriter);

    PackageFileWriterPtr reUsePackFileWriter = 
        inMemDirectory->GetPackageFileWriter("package_file");
    ASSERT_TRUE(reUsePackFileWriter);
    ASSERT_EQ(reUsePackFileWriter.get(), packageFileWriter.get());
    
    FileWriterPtr innerFileWriter1 = 
        packageFileWriter->CreateInnerFileWriter("file_1");
    ASSERT_TRUE(innerFileWriter1);

    string value1 = "abc";
    innerFileWriter1->Write(value1.c_str(), value1.size());
    innerFileWriter1->Close();

    FileWriterPtr innerFileWriter2 = 
        packageFileWriter->CreateInnerFileWriter("file_2");
    ASSERT_TRUE(innerFileWriter2);

    string value2 = "efg";
    innerFileWriter2->Write(value2.c_str(), value2.size());
    innerFileWriter2->Close();

    packageFileWriter->Close();
    inMemDirectory->Sync(true);

    string packageFilePath = GET_TEST_DATA_PATH() + "dump_dir/package_file";
    string packageFileDataPath = packageFilePath + 
                                 PACKAGE_FILE_DATA_SUFFIX + "0";
    string packageFileMetaPath = packageFilePath + PACKAGE_FILE_META_SUFFIX;

    FileSystemWrapper::IsExist(packageFileDataPath);
    FileSystemWrapper::IsExist(packageFileMetaPath);

    // TODO : check meta file
    ASSERT_EQ((size_t)4099, 
              FileSystemWrapper::GetFileLength(packageFileDataPath));
}

void InMemDirectoryTest::TestListFileForPackageFile()
{
    InnerTestListFileForPackageFile(true, true);
    InnerTestListFileForPackageFile(true, false);
    InnerTestListFileForPackageFile(false, true);
    InnerTestListFileForPackageFile(false, false);
}

void InMemDirectoryTest::TestRemoveForPackageFile()
{
    InnerTestRemoveForPackageFile(true);
    InnerTestRemoveForPackageFile(false);
}

void InMemDirectoryTest::TestCreateFileReader()
{
    MakePackageFile("segment_0", "package_file", 
                    "file1:abcd#index/file2:12345", false);
    DirectoryPtr segDirectory = 
        GET_PARTITION_DIRECTORY()->GetDirectory("segment_0", true);

    CheckReader(segDirectory, "file1", "abcd");
    CheckReader(segDirectory, "index/file2", "12345");
    segDirectory->Sync(true);

    CheckReader(segDirectory, "file1", "abcd");
    CheckReader(segDirectory, "index/file2", "12345");

    GET_FILE_SYSTEM()->CleanCache();
    ASSERT_TRUE(segDirectory->IsExist("file1"));
    ASSERT_TRUE(segDirectory->IsExist("index/file2"));
    ASSERT_TRUE(segDirectory->MountPackageFile("package_file"));

    // first load to cache
    CheckReader(segDirectory, "file1", "abcd");
    CheckReader(segDirectory, "index/file2", "12345");

    // reopen from file node cache
    CheckReader(segDirectory, "file1", "abcd");
    CheckReader(segDirectory, "index/file2", "12345");
}

void InMemDirectoryTest::TestCreateFileWriterCopyOnDump()
{
    LoadConfigList loadConfigList;
    RESET_FILE_SYSTEM(loadConfigList, true, true);
    DirectoryPtr partDirectory = GET_PARTITION_DIRECTORY();
    DirectoryPtr inMemDirectory = partDirectory->MakeInMemDirectory("dump_dir");
    FSWriterParam param;
    param.copyOnDump = true;
    string filename = "dumpfile";
    FileWriterPtr fileWriter = inMemDirectory->CreateFileWriter(filename, param);
    fileWriter->Write("12345", 6);
    fileWriter->Close();
    FileReaderPtr fileReader = inMemDirectory->CreateFileReader(filename, FSOT_IN_MEM);
    string absPath = fileReader->GetPath();
    char* content = (char*)(fileReader->GetBaseAddress());
    assert(content);
    content[0] = 'a';
    inMemDirectory->Sync(true);

    string fileContent;
    FileSystemWrapper::AtomicLoad(absPath, fileContent);
    ASSERT_EQ(string("12345", 6), fileContent);
}

void InMemDirectoryTest::TestCreateFileWriterAtomicDump()
{
    LoadConfigList loadConfigList;
    RESET_FILE_SYSTEM(loadConfigList, true, true);
    // test exception scenario
    DirectoryPtr partDirectory = GET_PARTITION_DIRECTORY();
    DirectoryPtr inMemDirectory = partDirectory->MakeInMemDirectory("dump_dir");
    FSWriterParam param;
    param.atomicDump = true;
    string filename = "dumpfile";
    FileWriterPtr fileWriter = inMemDirectory->CreateFileWriter(filename, param);
    fileWriter->Write("12345", 6);
    fileWriter->Close();
    FileSystemWrapper::AtomicStore(fileWriter->GetPath(), "random content");
    ASSERT_THROW(inMemDirectory->Sync(true), misc::ExistException);
    // reset file_system to catch exception thrown by sync
    RESET_FILE_SYSTEM(loadConfigList, true, true);
    
    // test normal scenario
    TearDown();
    SetUp();
    partDirectory = GET_PARTITION_DIRECTORY();
    inMemDirectory = partDirectory->MakeInMemDirectory("dump_dir");
    fileWriter = inMemDirectory->CreateFileWriter(filename, param);
    string filePath = fileWriter->GetPath();
    fileWriter->Write("12345", 6);
    fileWriter->Close();
    ASSERT_NO_THROW(inMemDirectory->Sync(true));
    string fileContent;
    FileSystemWrapper::AtomicLoad(filePath, fileContent);
    ASSERT_EQ(string("12345", 6), fileContent);
}

void InMemDirectoryTest::TestCreatePackageFileWriterCopyOnDump()
{
    LoadConfigList loadConfigList;
    RESET_FILE_SYSTEM(loadConfigList, true, true);
    DirectoryPtr partDirectory = GET_PARTITION_DIRECTORY();
    DirectoryPtr inMemDirectory = partDirectory->MakeInMemDirectory("dump_dir");

    FSWriterParam param;
    param.copyOnDump = true;
    string value = "12345";
    
    PackageFileWriterPtr packageFileWriter = 
        inMemDirectory->CreatePackageFileWriter("package_file");
    
    FileWriterPtr innerFileWriter = 
        packageFileWriter->CreateInnerFileWriter("file_1", param);
    innerFileWriter->Write(value.c_str(), value.size());
    innerFileWriter->Close();

    param.copyOnDump = false;
    innerFileWriter = 
        packageFileWriter->CreateInnerFileWriter("file_2", param);
    innerFileWriter->Write(value.c_str(), value.size());
    innerFileWriter->Close();
    packageFileWriter->Close();
    
    inMemDirectory->MountPackageFile("package_file");
    FileReaderPtr reader = inMemDirectory->CreateFileReader("file_1", FSOT_IN_MEM);
    char* content = (char*)(reader->GetBaseAddress());
    content[0] = 'a';
    
    reader = inMemDirectory->CreateFileReader("file_2", FSOT_IN_MEM);
    content = (char*)(reader->GetBaseAddress());
    content[0] = 'a';

    reader.reset();
    inMemDirectory->Sync(true);
    GET_FILE_SYSTEM()->CleanCache();

    CheckReader(inMemDirectory, "file_1", string("12345"));
    CheckReader(inMemDirectory, "file_2", string("a2345"));
}

void InMemDirectoryTest::TestCreatePackageFileWriterAtomicDump()
{
   LoadConfigList loadConfigList;
    RESET_FILE_SYSTEM(loadConfigList, true, true);
    DirectoryPtr partDirectory = GET_PARTITION_DIRECTORY();
    DirectoryPtr inMemDirectory = partDirectory->MakeInMemDirectory("dump_dir");

    // test exception scenario
    FSWriterParam param;
    param.atomicDump = true;
    string value = "12345";
    PackageFileWriterPtr packageFileWriter = 
        inMemDirectory->CreatePackageFileWriter("package_file");
    FileWriterPtr innerFileWriter = 
        packageFileWriter->CreateInnerFileWriter("file_1", param);
    innerFileWriter->Write(value.c_str(), value.size());
    innerFileWriter->Close();
    packageFileWriter->Close();
    FileSystemWrapper::AtomicStore(
        packageFileWriter->GetFilePath() + PACKAGE_FILE_META_SUFFIX,
        "random content");
    ASSERT_THROW(inMemDirectory->Sync(true), misc::ExistException);
    // reset file_system to catch exception thrown by sync
    RESET_FILE_SYSTEM(loadConfigList, true, true);
    
    // test normal scenario
    TearDown();
    SetUp();
    partDirectory = GET_PARTITION_DIRECTORY();
    inMemDirectory = partDirectory->MakeInMemDirectory("dump_dir");
    packageFileWriter = 
        inMemDirectory->CreatePackageFileWriter("package_file");
    innerFileWriter = 
        packageFileWriter->CreateInnerFileWriter("file_1", param);
    innerFileWriter->Write(value.c_str(), value.size());
    innerFileWriter->Close();
    packageFileWriter->Close();
    inMemDirectory->Sync(true);
    GET_FILE_SYSTEM()->CleanCache();
    CheckReader(inMemDirectory, "file_1", string("12345"));
}

void InMemDirectoryTest::InnerTestRemoveForPackageFile(bool sync)
{
    TearDown();
    SetUp();

    MakePackageFile("segment_0", "package_file", "file1:abcd#index/file2:12345", sync);
    DirectoryPtr segDirectory = 
        GET_PARTITION_DIRECTORY()->GetDirectory("segment_0", true);

    ASSERT_TRUE(segDirectory->MountPackageFile("package_file"));
    // package file is already mounted in FileSystem
    if (sync)
    {
        ASSERT_TRUE(segDirectory->IsExist("package_file.__meta__"));
        ASSERT_TRUE(segDirectory->IsExist("package_file.__data__0"));
    }
    else
    {
        ASSERT_FALSE(segDirectory->IsExist("package_file.__meta__"));
        ASSERT_FALSE(segDirectory->IsExist("package_file.__data__0"));        
    }
    segDirectory->RemoveFile("file1");
    ASSERT_FALSE(segDirectory->IsExist("file1"));
    ASSERT_THROW(segDirectory->RemoveFile("file_nonexist"), 
                 misc::NonExistException);

    segDirectory->RemoveDirectory("index/");
    ASSERT_FALSE(segDirectory->IsExist("index/"));
    ASSERT_FALSE(segDirectory->IsExist("index/file2"));

    ASSERT_FALSE(segDirectory->IsExist("package_file.__meta__"));
    ASSERT_FALSE(segDirectory->IsExist("package_file.__data__0"));

    // remove segment trigger remove packageFileWriter in storage
    GET_PARTITION_DIRECTORY()->RemoveDirectory("segment_0");
    ASSERT_NO_THROW(MakePackageFile("segment_0", "package_file", 
                                    "file1:abcd#index/file2:12345", sync));
}

void InMemDirectoryTest::InnerTestListFileForPackageFile(
        bool sync, bool cleanCache)
{
    TearDown();
    SetUp();

    MakePackageFile("segment_0", "package_file", "file1:abcd#index/file2:12345", sync);
    DirectoryPtr segDirectory = 
        GET_PARTITION_DIRECTORY()->GetDirectory("segment_0", true);

    ASSERT_TRUE(segDirectory->IsExist("index"));
    ASSERT_TRUE(segDirectory->IsExist("index/"));

    if (cleanCache)
    {
        GET_FILE_SYSTEM()->CleanCache();
        ASSERT_TRUE(segDirectory->IsExist("file1"));
        ASSERT_TRUE(segDirectory->IsExist("index/file2"));
    }

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

    if (sync)
    {
        // meta + data, file1, file2, index
        ASSERT_EQ((size_t)5, fileList.size());
        ASSERT_EQ("package_file.__data__0", fileList[3]);
        ASSERT_EQ("package_file.__meta__", fileList[4]);
    }
    else
    {
        // file1, file2, index
        ASSERT_EQ((size_t)3, fileList.size());
    }

    ASSERT_EQ("file1", fileList[0]);
    ASSERT_EQ("index/", fileList[1]);
    ASSERT_EQ("index/file2", fileList[2]);
    
    fslib::FileList innerFileList;
    segDirectory->ListFile("index", innerFileList, false);
    ASSERT_EQ((size_t)1, innerFileList.size());
    ASSERT_EQ("file2", innerFileList[0]);
}

void InMemDirectoryTest::TestListFile()
{
    MakePackageFile("segment_0", "package_file",
                    "file1:abcd#index/file2:12345", false);
    DirectoryPtr segDirectory = 
        GET_PARTITION_DIRECTORY()->GetDirectory("segment_0", true);

    MakeFiles(segDirectory, "file3:file4:index/:index/file5");

    FileList filelist;
    // recursive = true, physical = false
    segDirectory->ListFile("", filelist, true, false);
    CheckFileList(filelist, "file1:file3:file4:index/:index/file2:index/file5");

    filelist.clear();
    // recursive = true, physical = true
    segDirectory->ListFile("", filelist, true, true);
    CheckFileList(filelist, "file3:file4:index/:index/file5:package_file.__data__0"
                  ":package_file.__meta__");

    filelist.clear();
    // recursive = false, physical = true
    segDirectory->ListFile("", filelist, false, true);
    CheckFileList(filelist, "file3:file4:index:package_file.__data__0"
                  ":package_file.__meta__");

    FileList innerFileList;
    // recursive = true, physical = true
    segDirectory->ListFile("index", innerFileList, true, true);
    CheckFileList(innerFileList, "file5");

    // recursive = false, physical = false
    segDirectory->ListFile("index", innerFileList, false, false);
    CheckFileList(innerFileList, "file2:file5");
}

void InMemDirectoryTest::TestLinkDirectory()
{
    RESET_FILE_SYSTEM(LoadConfigList(), false, true, true, true);
    DirectoryPtr partDirectory = GET_PARTITION_DIRECTORY();
    DirectoryPtr inMemDirectory = partDirectory->MakeInMemDirectory("dump_dir");
    LinkDirectoryPtr linkDir = inMemDirectory->CreateLinkDirectory();
    ASSERT_FALSE(linkDir);

    // subDir/file1, subDir/file2, file3
    DirectoryPtr subDir = inMemDirectory->MakeDirectory("subDir");
    subDir->Store("file1", "abc");
    subDir->Store("file2", "efg");
    inMemDirectory->Store("file3", "hij");
    inMemDirectory->Sync(true);
    
    linkDir = inMemDirectory->CreateLinkDirectory();
    ASSERT_TRUE(linkDir);
    DirectoryPtr subLinkDir = linkDir->GetDirectory("subDir", true);
    ASSERT_TRUE(subLinkDir);

    FileReaderPtr reader = subLinkDir->CreateFileReader("file1", FSOT_MMAP);
    ASSERT_TRUE(reader);
    ASSERT_EQ(FSOT_MMAP, reader->GetOpenType());
    reader->Close();
    reader.reset();

    ASSERT_TRUE(linkDir->IsExist("file3"));
    ASSERT_TRUE(linkDir->IsExist("subDir/file1"));
    ASSERT_TRUE(linkDir->IsExist("subDir/file2"));
    ASSERT_TRUE(subLinkDir->IsExist("file1"));
    ASSERT_TRUE(subLinkDir->IsExist("file2"));

    // remove file
    inMemDirectory->RemoveFile("file3");
    ASSERT_FALSE(inMemDirectory->IsExist("file3"));    
    ASSERT_FALSE(linkDir->IsExist("file3"));

    // remove directory
    inMemDirectory->RemoveDirectory("subDir");
    ASSERT_ANY_THROW(linkDir->GetDirectory("subDir", true));
    ASSERT_FALSE(linkDir->IsExist("subDir/file1"));
    ASSERT_FALSE(linkDir->IsExist("subDir/file2"));
}

void InMemDirectoryTest::CheckFileList(
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

void InMemDirectoryTest::MakeFiles(
        DirectoryPtr directoryPtr, const string& fileStr)
{
    vector<string> fileNames;
    StringUtil::fromString(fileStr, fileNames, ":");
    for (size_t i = 0; i < fileNames.size(); i++)
    {
        if (*(fileNames[i].rbegin()) == '/')
        {
            directoryPtr->MakeDirectory(fileNames[i]);
        }
        else
        {
            directoryPtr->Store(fileNames[i], "");
        }
    }
}

void InMemDirectoryTest::MakePackageFile(const string& dirName,
        const string& packageFileName,
        const string& packageFileInfoStr,
        bool needSync)
{
    DirectoryPtr partDirectory = GET_PARTITION_DIRECTORY();
    DirectoryPtr inMemDirectory = 
        partDirectory->MakeInMemDirectory(dirName);
    
    PackageFileUtil::CreatePackageFile(inMemDirectory,
            packageFileName, packageFileInfoStr, "");
    if (needSync)
    {
        inMemDirectory->Sync(true);
    }
}

void InMemDirectoryTest::CheckReader(
        const DirectoryPtr& directory,
        const string& filePath,
        const string& expectValue)
{
    FileReaderPtr fileReader = directory->CreateFileReader(filePath, FSOT_IN_MEM);
    ASSERT_TRUE(fileReader);
    ASSERT_EQ(expectValue.size(), fileReader->GetLength());
    ASSERT_EQ(FSOT_IN_MEM, fileReader->GetOpenType());

    string value;
    value.resize(expectValue.size());
    fileReader->Read((void*)value.data(), expectValue.size());
    ASSERT_EQ(value, expectValue);

    string expectPath = PathUtil::JoinPath(directory->GetPath(), filePath);
    ASSERT_EQ(expectPath, fileReader->GetPath());
}

IE_NAMESPACE_END(file_system);

