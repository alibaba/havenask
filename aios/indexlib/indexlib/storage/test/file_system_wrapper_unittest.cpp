#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/storage/common_file_wrapper.h"
#include "indexlib/storage/test/file_system_wrapper_unittest.h"

using namespace std;
using namespace fslib;
using namespace fslib::fs;

IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(storage);

void FileSystemWrapperTest::CaseSetUp()
{
    FileSystemWrapper::Reset();
    mRootDir = GET_TEST_DATA_PATH() + "/root/";

    mExistFile = mRootDir + "exist_file";
    File* file = FileSystem::openFile(mExistFile, WRITE);
    delete file;

    mNotExistFile = mRootDir + "not_exist_file";
}

void FileSystemWrapperTest::CaseTearDown()
{
    FileSystemWrapper::Reset();
}

void FileSystemWrapperTest::TestCaseForOpen4Read()
{
    FileWrapper *file = FileSystemWrapper::OpenFile(mExistFile, fslib::READ);
    INDEXLIB_TEST_TRUE(file != NULL);
    CommonFileWrapper *commonFileWrapper = dynamic_cast<CommonFileWrapper *>(file);
    INDEXLIB_TEST_TRUE(commonFileWrapper != NULL);
    file->Close();
    delete file;
}
    
void FileSystemWrapperTest::TestCaseForOpen4Write()
{
    FileWrapper* file = FileSystemWrapper::OpenFile(mNotExistFile, fslib::WRITE);
    INDEXLIB_TEST_TRUE(file != NULL);
    CommonFileWrapper *commonFileWrapper = dynamic_cast<CommonFileWrapper *>(file);
    INDEXLIB_TEST_TRUE(commonFileWrapper != NULL);
    file->Close();
    delete file;
}

void FileSystemWrapperTest::TestCaseForOpen4Append()
{
    FileWrapper* file = FileSystemWrapper::OpenFile(mNotExistFile, fslib::APPEND);
    INDEXLIB_TEST_TRUE(file != NULL);
    CommonFileWrapper *commonFileWrapper = dynamic_cast<CommonFileWrapper *>(file);
    INDEXLIB_TEST_TRUE(commonFileWrapper != NULL);
    file->Close();
    delete file;
}

void FileSystemWrapperTest::TestCaseForOpen4WriteExist()
{
    FileWrapper* file = FileSystemWrapper::OpenFile(mExistFile, fslib::WRITE);
    INDEXLIB_TEST_TRUE(file != NULL);
    file->Close();
    delete file;
}

void FileSystemWrapperTest::TestCaseForOpenFail()
{
    ASSERT_THROW(FileSystemWrapper::OpenFile(mNotExistFile,fslib::READ), 
                 NonExistException);
}

void FileSystemWrapperTest::TestCaseForLoadAndStore()
{
    string filePath = "./file_system_wrapper_atomic";
    string fileContent = "content test";

    if (FileSystemWrapper::IsExist(filePath))
    {
        FileSystemWrapper::DeleteFile(filePath);
    }
    FileSystemWrapper::AtomicStore(filePath, fileContent);

    string loadedFileCont;
    FileSystemWrapper::AtomicLoad(filePath, loadedFileCont);
    INDEXLIB_TEST_EQUAL(fileContent, loadedFileCont);

    FileSystemWrapper::DeleteFile(filePath);
    INDEXLIB_TEST_TRUE(!FileSystemWrapper::IsExist(filePath));

    size_t len = 193000;
    char buff[len];
    for (size_t i = 0; i < len; i++)
    {
        buff[i] = (char)(i % 126 + 1);
    }

    FileSystemWrapper::AtomicStore(filePath, string(buff, len));
    FileSystemWrapper::AtomicLoad(filePath, loadedFileCont);
    INDEXLIB_TEST_EQUAL(string(buff, len), loadedFileCont);

    FileSystemWrapper::DeleteFile(filePath);
    INDEXLIB_TEST_TRUE(!FileSystemWrapper::IsExist(filePath));
}

void FileSystemWrapperTest::TestCaseForRemoveNotExistFile()
{
    ASSERT_THROW(FileSystemWrapper::DeleteFile(mNotExistFile),
                 NonExistException);
}

void FileSystemWrapperTest::TestCaseForIsExist()
{
    INDEXLIB_TEST_TRUE(FileSystemWrapper::IsExist(mExistFile));
    INDEXLIB_TEST_TRUE(!FileSystemWrapper::IsExist(mNotExistFile));
}

void FileSystemWrapperTest::TestCaseForListDir()
{
    FileList fileList;
    FileSystemWrapper::ListDir(mRootDir, fileList);
    INDEXLIB_TEST_EQUAL((size_t)1, fileList.size());
    INDEXLIB_TEST_EQUAL(mExistFile, mRootDir + fileList[0]);
}
    
void FileSystemWrapperTest::TestCaseForListDirRecursive()
{
    {
        string dir = mRootDir + "testListDirRecursive";
        FileSystemWrapper::MkDir(dir);
        string subDir1 = FileSystemWrapper::JoinPath(dir, "subDir1");
        FileSystemWrapper::MkDir(subDir1);
        string subDir2 = FileSystemWrapper::JoinPath(subDir1, "subDir2");
        FileSystemWrapper::MkDir(subDir2);
        string subFile = FileSystemWrapper::JoinPath(subDir2, "subFile");
        FileSystemWrapper::AtomicStore(subFile, "");
        string tempFile = FileSystemWrapper::JoinPath(dir, "file.__tmp__");
        FileSystemWrapper::AtomicStore(tempFile, "");
        FileList fileList;
        FileSystemWrapper::ListDirRecursive(dir, fileList);
        INDEXLIB_TEST_EQUAL((size_t)3, fileList.size());
        INDEXLIB_TEST_EQUAL(string("subDir1/"), fileList[0]);
        INDEXLIB_TEST_EQUAL(string("subDir1/subDir2/"), fileList[1]);
        INDEXLIB_TEST_EQUAL(string("subDir1/subDir2/subFile"), fileList[2]);
        FileSystemWrapper::DeleteDir(dir);
    }
    {
        FileList fileList;
        ASSERT_THROW(FileSystemWrapper::ListDirRecursive(mNotExistFile, fileList),
                     NonExistException);
    }
    {
        string dir = mRootDir + "testListDirRecursive";
        FileSystemWrapper::MkDir(dir);
        CreateFile(dir, "1.__tmp__");
        CreateFile(dir, "2.__tmp__.");
        CreateFile(dir, "3");
        CreateFile(dir, "__tmp__");
        CreateFile(dir, ".__tmp__");

        FileList fileList;
        FileSystemWrapper::ListDirRecursive(dir, fileList);

        EXPECT_THAT(fileList, UnorderedElementsAre(
                        string("2.__tmp__."),
                        string("__tmp__"),
                        string("3")));
        FileSystemWrapper::DeleteDir(dir);
    }
}
    
void FileSystemWrapperTest::CreateFile(const string& dir, const string& fileName)
{
    string filePath = FileSystemWrapper::JoinPath(dir, fileName);
    FileSystemWrapper::AtomicStore(filePath, "");
}

void FileSystemWrapperTest::TestCaseForDeleteDir()
{
    string dir = mRootDir + "testdir";
    FileSystemWrapper::MkDir(dir);
    FileSystemWrapper::DeleteDir(dir);
    INDEXLIB_TEST_TRUE(!FileSystemWrapper::IsExist(dir));
}

void FileSystemWrapperTest::TestCaseForDeleteDirFail()
{
    ASSERT_THROW(FileSystemWrapper::DeleteDir(mNotExistFile + "/"),
                 NonExistException);
}

void FileSystemWrapperTest::TestCaseForCopy()
{
    ASSERT_THROW(FileSystemWrapper::Copy(mNotExistFile, mExistFile),
                 NonExistException);

    ASSERT_THROW(FileSystemWrapper::Copy(mExistFile, mExistFile),
                 FileIOException);

    FileSystemWrapper::Copy(mExistFile, mNotExistFile);
}

void FileSystemWrapperTest::TestCaseForRename()
{
    ASSERT_THROW(FileSystemWrapper::Rename(mNotExistFile, mExistFile),
                 NonExistException);

    FileSystemWrapper::Rename(mExistFile, mExistFile);

    FileSystemWrapper::Rename(mExistFile, mNotExistFile);
}

void FileSystemWrapperTest::TestCaseForMkDir()
{
    ASSERT_THROW(FileSystemWrapper::MkDir(mExistFile),
                 ExistException);
}

void FileSystemWrapperTest::TestCaseForGetFileMeta()
{
    FileMeta fileMeta;
    ASSERT_THROW(FileSystemWrapper::GetFileMeta(mNotExistFile, fileMeta), 
                 NonExistException);

    FileMeta fileMeta2;
    FileSystemWrapper::GetFileMeta(mExistFile, fileMeta2);
    INDEXLIB_TEST_TRUE(fileMeta2.fileLength == 0);
}

void FileSystemWrapperTest::TestCaseForGetFileLength()
{
    ASSERT_THROW(FileSystemWrapper::GetFileLength(mNotExistFile), 
                 NonExistException);

    size_t size = FileSystemWrapper::GetFileLength(mExistFile);
    INDEXLIB_TEST_EQUAL((size_t)0, size);
}

void FileSystemWrapperTest::TestCaseForIsDir()
{
    INDEXLIB_TEST_TRUE(FileSystemWrapper::IsDir(mRootDir));
    INDEXLIB_TEST_TRUE(!FileSystemWrapper::IsDir(mExistFile));
    INDEXLIB_TEST_TRUE(!FileSystemWrapper::IsDir(mNotExistFile));
}

void FileSystemWrapperTest::TestCaseForDeleteIfExist()
{
    ASSERT_NO_THROW(FileSystemWrapper::DeleteIfExist(mNotExistFile));
    ASSERT_NO_THROW(FileSystemWrapper::DeleteIfExist(mExistFile));
    ASSERT_FALSE(FileSystemWrapper::IsExist(mExistFile));
}

void FileSystemWrapperTest::TestCaseForNeedMkParentDirBeforeOpen()
{
    EXPECT_FALSE(FileSystemWrapper::NeedMkParentDirBeforeOpen("pangu://abc"));
    EXPECT_FALSE(FileSystemWrapper::NeedMkParentDirBeforeOpen("hdfs://abc"));
    EXPECT_FALSE(FileSystemWrapper::NeedMkParentDirBeforeOpen("unknown://abc"));
    EXPECT_TRUE(FileSystemWrapper::NeedMkParentDirBeforeOpen("//abc"));
    EXPECT_TRUE(FileSystemWrapper::NeedMkParentDirBeforeOpen("./abc"));
}

void FileSystemWrapperTest::TestCaseForMkDirIfNotExist()
{
    string notExistDir = GET_TEST_DATA_PATH() + "/not/exist/dir";
    FileSystemWrapper::MkDirIfNotExist(notExistDir);
    EXPECT_TRUE(FileSystemWrapper::IsExist(notExistDir));
    FileSystemWrapper::MkDirIfNotExist(notExistDir);
    EXPECT_TRUE(FileSystemWrapper::IsExist(notExistDir));
    // fslib return EC_EXIST when it's file
    FileSystemWrapper::MkDirIfNotExist(mExistFile);
}

void FileSystemWrapperTest::TestCaseForSimpleMmapFile()
{
    string simpleFilePath = mRootDir + "simple_mmap_file";
    string content = "hello mmap file";
    FileSystemWrapper::AtomicStore(simpleFilePath, content);

    MMapFilePtr mmapFile(FileSystemWrapper::MmapFile(simpleFilePath, fslib::READ,
            NULL, content.size(), PROT_READ, MAP_PRIVATE, 0, -1));
    ASSERT_TRUE(mmapFile.get());
    ASSERT_EQ((int64_t)content.size(), mmapFile->getLength());
    char* base = mmapFile->getBaseAddress();
    ASSERT_EQ(string(base, mmapFile->getLength()), content);
}

void FileSystemWrapperTest::TestCaseForSymLink()
{
    string symlinkPath = mRootDir + "/link_path";

    ASSERT_FALSE(FileSystemWrapper::SymLink("hdfs://abc", symlinkPath));
    ASSERT_TRUE(FileSystemWrapper::SymLink(mRootDir, symlinkPath));
    ASSERT_TRUE(FileSystemWrapper::IsExist(symlinkPath));

    string filePath = mRootDir + "/hello_file";
    string symFilePath = symlinkPath + "/hello_file";
    string content = "hello file";
    FileSystemWrapper::AtomicStore(filePath, content);

    string tmp;
    FileSystemWrapper::AtomicLoad(symFilePath, tmp);
    ASSERT_EQ(tmp, content);

    // list link
    FileList fileList;
    FileSystemWrapper::ListDir(mRootDir, fileList);
    ASSERT_EQ(3, fileList.size());

    // remove link
    FileSystemWrapper::DeleteFile(symlinkPath);
    ASSERT_FALSE(FileSystemWrapper::IsExist(symlinkPath));
    ASSERT_FALSE(FileSystemWrapper::IsExist(symFilePath));
    ASSERT_TRUE(FileSystemWrapper::IsExist(filePath));
}

IE_NAMESPACE_END(storage);
