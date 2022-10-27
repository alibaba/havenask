#include "indexlib/storage/test/archive_folder_unittest.h"
#include "indexlib/storage/archive_file.h"
#include "indexlib/misc/exception.h"
#include <thread>

using namespace std;

IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(storage);
IE_LOG_SETUP(storage, ArchiveFolderTest);

ArchiveFolderTest::ArchiveFolderTest()
{
}

ArchiveFolderTest::~ArchiveFolderTest()
{
}

void ArchiveFolderTest::CaseSetUp()
{
}

void ArchiveFolderTest::CaseTearDown()
{
}

void ArchiveFolderTest::TestSimpleProcess()
{
    ArchiveFolder archiveFolderWrite(false);
    archiveFolderWrite.Open(GET_TEST_DATA_PATH(), "i");
    FileWrapperPtr archiveFile = archiveFolderWrite.GetInnerFile("test1", fslib::WRITE);
    string content = "abcdef";
    archiveFile->Write(content.c_str(), content.length());
    archiveFile->Close();
    archiveFolderWrite.Close();
    ArchiveFolder archiveFolderRead(false);
    archiveFolderRead.Open(GET_TEST_DATA_PATH());
    FileWrapperPtr archiveFileToRead = archiveFolderRead.GetInnerFile("test1", fslib::READ);
    char buffer[10];
    archiveFileToRead->Read(buffer, content.length());
    string result(buffer, content.length());
    ASSERT_EQ(content, result);
    archiveFileToRead->Close();
    archiveFolderRead.Close();
}

void ArchiveFolderTest::TestReadFromMultiLogFiles()
{
    ArchiveFolder folderForWrite1(false);
    folderForWrite1.Open(GET_TEST_DATA_PATH(), "1");
    FileWrapperPtr archiveFile = folderForWrite1.GetInnerFile("test1", fslib::WRITE);
    string content1 = "abcdef";
    archiveFile->Write(content1.c_str(), content1.length());
    archiveFile->Close();
    folderForWrite1.Close();
    
    ArchiveFolder folderForWrite2(false);
    folderForWrite2.Open(GET_TEST_DATA_PATH(),"2");
    archiveFile = folderForWrite2.GetInnerFile("test2", fslib::WRITE);
    string content2 = "hello world";
    archiveFile->Write(content2.c_str(), content2.length());
    archiveFile->Close();

    ArchiveFolder archiveFolderRead(false);
    archiveFolderRead.Open(GET_TEST_DATA_PATH());
    archiveFile = archiveFolderRead.GetInnerFile("test1", fslib::READ);
    char buffer[20];
    int64_t readLen = archiveFile->Read(buffer, 20);
    string result(buffer, readLen);
    ASSERT_EQ(content1, result);
    archiveFile->Close();
    
    archiveFile = archiveFolderRead.GetInnerFile("test2", fslib::READ);
    readLen = archiveFile->Read(buffer, 20);
    result = string(buffer, readLen);
    ASSERT_EQ(content2, result);
    archiveFile->Close();

    archiveFolderRead.Close();
    folderForWrite2.Close();
}

void ArchiveFolderTest::TestReadFromLogTmpFile()
{
    ArchiveFolder archiveFolderWrite(false);
    archiveFolderWrite.Open(GET_TEST_DATA_PATH(),"i");
    FileWrapperPtr archiveFile = archiveFolderWrite.GetInnerFile("test1", fslib::WRITE);
    string content = "abcdef";
    archiveFile->Write(content.c_str(), content.length());
    archiveFile->Close();
    ArchiveFolder archiveFolderRead(false);
    archiveFolderRead.Open(GET_TEST_DATA_PATH());
    FileWrapperPtr archiveFileToRead = archiveFolderRead.GetInnerFile("test1", fslib::READ);
    char buffer[10];
    archiveFileToRead->Read(buffer, content.length());
    string result(buffer, content.length());
    ASSERT_EQ(content, result);
    archiveFileToRead->Close();
    archiveFolderRead.Close();
    archiveFolderWrite.Close();
}

void ArchiveFolderTest::TestMultiBlocks()
{
    ArchiveFolder archiveFolderWrite(false);
    archiveFolderWrite.Open(GET_TEST_DATA_PATH(), "i");
    ArchiveFilePtr archiveFile = 
        DYNAMIC_POINTER_CAST(ArchiveFile, 
                             archiveFolderWrite.GetInnerFile(
                                     "test1", fslib::WRITE));

    archiveFile->ResetBufferSize(2);
    string content = "abcdef";
    archiveFile->Write(content.c_str(), content.length());
    archiveFile->Close();
    ArchiveFolder archiveFolderRead(false);
    archiveFolderRead.Open(GET_TEST_DATA_PATH());
    FileWrapperPtr archiveFileToRead = archiveFolderRead.GetInnerFile("test1", fslib::READ);
    char buffer[10];
    archiveFileToRead->Read(buffer, content.length());
    string result(buffer, content.length());
    ASSERT_EQ(content, result);
    archiveFileToRead->Close();
    archiveFolderRead.Close();
    archiveFolderWrite.Close();
}

void ArchiveFolderTest::TestMultiArchiveFiles()
{
    ArchiveFolder archiveFolderWrite(false);
    archiveFolderWrite.Open(GET_TEST_DATA_PATH(), "i");
    ArchiveFilePtr archiveFile1 = 
        DYNAMIC_POINTER_CAST(ArchiveFile, 
                             archiveFolderWrite.GetInnerFile(
                                     "test1", fslib::WRITE));
    archiveFile1->ResetBufferSize(2);
    ArchiveFilePtr archiveFile2 = 
        DYNAMIC_POINTER_CAST(ArchiveFile, 
                             archiveFolderWrite.GetInnerFile(
                                     "test2", fslib::WRITE));
    archiveFile2->ResetBufferSize(2);

    string content1 = "abcdef";
    string content2 = "hello world";
    archiveFile1->Write(content1.c_str(), content1.length());
    archiveFile2->Write(content2.c_str(), content2.length());
    archiveFile1->Close();
    archiveFile2->Close();

    ArchiveFolder archiveFolderRead(false);
    archiveFolderRead.Open(GET_TEST_DATA_PATH());
    FileWrapperPtr archiveFileToRead = archiveFolderRead.GetInnerFile("test1", fslib::READ);
    char buffer[20];
    archiveFileToRead->Read(buffer, 20);
    string result(buffer, content1.length());
    ASSERT_EQ(content1, result);

    char buffer2[20];
    archiveFileToRead = archiveFolderRead.GetInnerFile("test2", fslib::READ);
    archiveFileToRead->Read(buffer2, 20);
    string result2(buffer2, content2.length());
    ASSERT_EQ(content2, result2);

    archiveFileToRead->Close();
    archiveFolderRead.Close();
    archiveFolderWrite.Close();
}

void ArchiveFolderTest::TestRecoverFailover() 
{
    string logTmpFile = R"(abcdef{
"blocks":
  [
    {
    "block_key":
      "test1_archive_file_block_0",
    "block_length":
      2
    },
    {
    "block_key":
      "test1_archive_file_block_1",
    "block_length":
      2
    },
    {
    "block_key":
      "test1_archive_file_block_2",
    "block_length":
      2
    }
  ],
"max_block_size":
  2
})";
    string errorMetaFile
            = R"(
26	0	2	test1_archive_file_block_0
26	2	2	test1_archive_file_block_1
26  4	2	test1_archive_file_block_2
)";
    string metaFilePath = FileSystemWrapper::JoinPath(
            GET_TEST_DATA_PATH(), "INDEXLIB_LOG_FILE_2.meta");
    string dataTmpFilePath = FileSystemWrapper::JoinPath(
            GET_TEST_DATA_PATH(), "INDEXLIB_LOG_FILE_2.tmp");
    FileWrapperPtr metaFile(FileSystemWrapper::OpenFile(metaFilePath, fslib::WRITE));
    metaFile->Write(errorMetaFile.c_str(), errorMetaFile.length());
    metaFile->Close();

    FileWrapperPtr dataTmpFile(FileSystemWrapper::OpenFile(dataTmpFilePath, fslib::WRITE));
    dataTmpFile->Write(logTmpFile.c_str(), logTmpFile.length());
    dataTmpFile->Close();

    ArchiveFolder archiveFolderRead(false);
    archiveFolderRead.Open(GET_TEST_DATA_PATH(), "2");
    FileWrapperPtr archiveFile2 = archiveFolderRead.GetInnerFile("test1", fslib::READ);
    ASSERT_EQ(NULL, archiveFile2.get());
    string newContent = "test2 test2";
    ArchiveFolder archiveFolderWrite(false);
    archiveFolderWrite.Open(GET_TEST_DATA_PATH(), "2");
    archiveFile2 = archiveFolderWrite.GetInnerFile("test1", fslib::WRITE);
    archiveFile2->Write(newContent.c_str(), newContent.size());
    archiveFile2->Close();

    ArchiveFolder archiveFolder(false);
    archiveFolder.Open(GET_TEST_DATA_PATH(),"2");
    archiveFile2 = archiveFolder.GetInnerFile("test1", fslib::READ);
    char buffer[20];
    archiveFile2->Read(buffer, 20);
    string result(buffer, newContent.size());
    ASSERT_EQ(newContent, result);
    archiveFolder.Close();
    archiveFolderWrite.Close();
    archiveFolderRead.Close();
}

void ArchiveFolderTest::TestRead()
{
    ArchiveFolder archiveFolderWrite1(false);
    archiveFolderWrite1.Open(GET_TEST_DATA_PATH(), "1");

    ArchiveFolder archiveFolderWrite2(false);
    archiveFolderWrite2.Open(GET_TEST_DATA_PATH(), "2");

    ArchiveFilePtr archiveFile1 = 
        DYNAMIC_POINTER_CAST(ArchiveFile, 
                             archiveFolderWrite1.GetInnerFile(
                                     "test1", fslib::WRITE));
    archiveFile1->ResetBufferSize(2);
    ArchiveFilePtr archiveFile2 = 
        DYNAMIC_POINTER_CAST(ArchiveFile, 
                             archiveFolderWrite1.GetInnerFile(
                                     "test2", fslib::WRITE));
    archiveFile2->ResetBufferSize(2);
    ArchiveFilePtr archiveFile3 = 
        DYNAMIC_POINTER_CAST(ArchiveFile, 
                             archiveFolderWrite2.GetInnerFile(
                                     "test3", fslib::WRITE));
    archiveFile3->ResetBufferSize(2);

    string content = "abcdefghijkl";
    archiveFile1->Write(content.c_str(), content.length());
    content = "hello world, hello world";
    archiveFile2->Write(content.c_str(), content.length());
    content = "test3, test3";
    archiveFile3->Write(content.c_str(), content.length());

    archiveFile1->Close();

    ArchiveFolder archiveFolderRead(false);
    archiveFolderRead.Open(GET_TEST_DATA_PATH());

    char buffer[30];
    FileWrapperPtr archiveRead = archiveFolderRead.GetInnerFile("test3", fslib::READ);
    ASSERT_EQ(NULL, archiveRead.get());
    archiveRead = archiveFolderRead.GetInnerFile("test2", fslib::READ);
    ASSERT_EQ(NULL, archiveRead.get());
    
    archiveFile2->Close();
    archiveRead = archiveFolderRead.GetInnerFile("test1", fslib::READ);
    ASSERT_EQ(3, archiveRead->PRead(buffer, 3, 2));
    ASSERT_EQ("cde", string(buffer, 3));

    ASSERT_EQ(1, archiveRead->PRead(buffer, 1, 0));
    ASSERT_EQ("a", string(buffer, 1));

    ASSERT_EQ(1, archiveRead->PRead(buffer, 1, 11));
    ASSERT_EQ("l", string(buffer, 1));

    ASSERT_EQ(1, archiveRead->PRead(buffer, 4, 11));
    ASSERT_EQ("l", string(buffer, 1));

    ASSERT_EQ(2, archiveRead->PRead(buffer, 2, 2));
    ASSERT_EQ("cd", string(buffer, 2));

    ASSERT_EQ(5, archiveRead->PRead(buffer, 5, 2));
    ASSERT_EQ("cdefg", string(buffer, 5));
    
    ASSERT_EQ(3, archiveRead->Read(buffer, 3));
    ASSERT_EQ("hij", string(buffer, 3));

    archiveFile3->Close();
    archiveFolderWrite1.Close();
    archiveFolderWrite2.Close();
    archiveFolderRead.Close();
}

void ArchiveFolderTest::TestMultiThreadWrite()
{
    auto write = [] (FileWrapperPtr archiveFile, int64_t times, 
                     int64_t interval ,string content)
    {
        for (int64_t i = 0; i < times; i++)
        {
            archiveFile->Write(content.c_str(), content.size());
            usleep(interval);
        }
        archiveFile->Close();
    };
    ArchiveFolder archiveFolderWrite(false);
    archiveFolderWrite.Open(GET_TEST_DATA_PATH(), "2");
    ArchiveFilePtr archiveFile1 = 
        DYNAMIC_POINTER_CAST(ArchiveFile, 
                             archiveFolderWrite.GetInnerFile(
                                     "test1", fslib::WRITE));
    archiveFile1->ResetBufferSize(500);
    ArchiveFilePtr archiveFile2 = 
        DYNAMIC_POINTER_CAST(ArchiveFile, 
                             archiveFolderWrite.GetInnerFile(
                                     "test2", fslib::WRITE));
    archiveFile2->ResetBufferSize(500);
    string content1 = "hello world";
    string content2 = "test test";
    thread t1(write, archiveFile1, 500, 10, content1);
    thread t2(write, archiveFile2, 300, 30, content2);
    t1.join();
    t2.join();
    ArchiveFolder archiveFolderRead(false);
    archiveFolderRead.Open(GET_TEST_DATA_PATH(), "2");
    archiveFile1 = 
        DYNAMIC_POINTER_CAST(ArchiveFile, 
                             archiveFolderRead.GetInnerFile("test1", fslib::READ));
    archiveFile2 =
        DYNAMIC_POINTER_CAST(ArchiveFile, 
                             archiveFolderRead.GetInnerFile("test2", fslib::READ));
    char buffer[15];
    ASSERT_EQ(11, archiveFile1->PRead(buffer, 11, 33));
    string result1(buffer, 11);
    ASSERT_EQ(content1, result1);
    ASSERT_EQ(9, archiveFile2->PRead(buffer, 9, 27));
    string result2(buffer, 9);
    ASSERT_EQ(content2, result2);

    archiveFile1->Close();
    archiveFile2->Close();
    archiveFolderWrite.Close();
    archiveFolderRead.Close();
}

void ArchiveFolderTest::TestWriteEmptyFile()
{
    ArchiveFolder archiveFolderWrite(false);
    archiveFolderWrite.Open(GET_TEST_DATA_PATH(), "1");
    FileWrapperPtr archiveFile = archiveFolderWrite.GetInnerFile("test1", fslib::WRITE);
    archiveFile->Close();
    ArchiveFolder archiveFolderRead(false);
    archiveFolderRead.Open(GET_TEST_DATA_PATH());
    archiveFile = archiveFolderRead.GetInnerFile("test1", fslib::READ);
    ASSERT_TRUE(archiveFile);
    char buffer[10];
    ASSERT_EQ(0, archiveFile->Read(buffer, 10));
    archiveFolderWrite.Close();
    archiveFolderRead.Close();
}

void ArchiveFolderTest::TestRewriteFile()
{
    ArchiveFolder archiveFolderWrite(false);
    //write hello world to test1
    archiveFolderWrite.Open(GET_TEST_DATA_PATH(), "1");
    FileWrapperPtr archiveFile = archiveFolderWrite.GetInnerFile("test1", fslib::WRITE);
    string content1 = "hello world";
    archiveFile->Write(content1.c_str(), content1.size());
    archiveFile->Close();
    //write hello ketty to test2
    string content2 = "hello kitty";
    archiveFile = archiveFolderWrite.GetInnerFile("test2", fslib::WRITE);
    archiveFile->Write(content2.c_str(), content2.size());
    archiveFile->Close();
    archiveFolderWrite.Close();
    
    //reopen folder and write nihao to test1
    archiveFolderWrite.Open(GET_TEST_DATA_PATH(), "1");
    string content3 = "nihao";
    archiveFile = archiveFolderWrite.GetInnerFile("test1", fslib::WRITE);
    archiveFile->Write(content3.c_str(), content3.size());
    archiveFile->Close();
    archiveFolderWrite.Close();

    //read test1 = nihao test2 = hello kitty
    ArchiveFolder archiveFolderRead(false);
    archiveFolderRead.Open(GET_TEST_DATA_PATH());
    archiveFile = archiveFolderRead.GetInnerFile("test1", fslib::READ);
    string result;
    FileSystemWrapper::AtomicLoad(archiveFile.get(), result);
    ASSERT_EQ(content3, result);
    archiveFile = archiveFolderRead.GetInnerFile("test2", fslib::READ);
    FileSystemWrapper::AtomicLoad(archiveFile.get(), result);
    ASSERT_EQ(content2, result);
    archiveFolderRead.Close();
}

IE_NAMESPACE_END(storage);

