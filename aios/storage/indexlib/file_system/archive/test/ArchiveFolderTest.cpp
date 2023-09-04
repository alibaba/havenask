#include "indexlib/file_system/archive/ArchiveFolder.h"

#include "gtest/gtest.h"
#include <cstddef>
#include <memory>
#include <stdint.h>
#include <string>
#include <thread>
#include <unistd.h>

#include "autil/Log.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/archive/ArchiveFile.h"
#include "indexlib/file_system/archive/ArchiveFileReader.h"
#include "indexlib/file_system/archive/ArchiveFileWriter.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/file_system/fslib/FslibFileWrapper.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;

using namespace indexlib::util;

namespace indexlib { namespace file_system {
class ArchiveFolderTest : public INDEXLIB_TESTBASE
{
public:
    ArchiveFolderTest();
    ~ArchiveFolderTest();

    DECLARE_CLASS_NAME(ArchiveFolderTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestReadFromLogTmpFile();
    void TestReadFromMultiLogFiles();
    void TestMultiBlocks();
    void TestMultiArchiveFiles();
    void TestRecoverFailover();
    void TestRead();
    void TestMultiThreadWrite();
    void TestWriteEmptyFile();
    void TestRewriteFile();
    void TestReadOtherInstanceFile();

private:
    std::shared_ptr<IDirectory> _directory;

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP(indexlib.file_system, ArchiveFolderTest);

INDEXLIB_UNIT_TEST_CASE(ArchiveFolderTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(ArchiveFolderTest, TestReadFromLogTmpFile);
INDEXLIB_UNIT_TEST_CASE(ArchiveFolderTest, TestReadFromMultiLogFiles);
INDEXLIB_UNIT_TEST_CASE(ArchiveFolderTest, TestMultiBlocks);
INDEXLIB_UNIT_TEST_CASE(ArchiveFolderTest, TestMultiArchiveFiles);
INDEXLIB_UNIT_TEST_CASE(ArchiveFolderTest, TestRecoverFailover);
INDEXLIB_UNIT_TEST_CASE(ArchiveFolderTest, TestRead);
INDEXLIB_UNIT_TEST_CASE(ArchiveFolderTest, TestMultiThreadWrite);
INDEXLIB_UNIT_TEST_CASE(ArchiveFolderTest, TestWriteEmptyFile);
INDEXLIB_UNIT_TEST_CASE(ArchiveFolderTest, TestRewriteFile);
INDEXLIB_UNIT_TEST_CASE(ArchiveFolderTest, TestReadOtherInstanceFile);
//////////////////////////////////////////////////////////////////////

ArchiveFolderTest::ArchiveFolderTest() {}

ArchiveFolderTest::~ArchiveFolderTest() {}

void ArchiveFolderTest::CaseSetUp()
{
    _directory = IDirectory::Get(FileSystemCreator::Create("", GET_TEMP_DATA_PATH()).GetOrThrow());
}

void ArchiveFolderTest::CaseTearDown() {}

void ArchiveFolderTest::TestSimpleProcess()
{
    ArchiveFolder archiveFolderWrite(false);
    ASSERT_EQ(FSEC_OK, archiveFolderWrite.Open(_directory, "i"));
    auto archiveFile = archiveFolderWrite.CreateFileWriter("test1").GetOrThrow();
    string content = "abcdef";
    ASSERT_EQ(FSEC_OK, archiveFile->Write(content.c_str(), content.length()).Code());
    ASSERT_EQ(FSEC_OK, archiveFile->Close());
    ASSERT_EQ(FSEC_OK, archiveFolderWrite.Close());
    ArchiveFolder archiveFolderRead(false);
    ASSERT_EQ(FSEC_OK, archiveFolderRead.Open(_directory, ""));
    auto archiveFileToRead = archiveFolderRead.CreateFileReader("test1").GetOrThrow();
    char buffer[10];
    ASSERT_EQ(FSEC_OK, archiveFileToRead->Read(buffer, content.length()).Code());
    string result(buffer, content.length());
    ASSERT_EQ(content, result);
    ASSERT_EQ(FSEC_OK, archiveFileToRead->Close());
    ASSERT_EQ(FSEC_OK, archiveFolderRead.Close());
}

void ArchiveFolderTest::TestReadFromMultiLogFiles()
{
    ArchiveFolder folderForWrite1(false);
    ASSERT_EQ(FSEC_OK, folderForWrite1.Open(_directory, "1"));
    auto archiveFile = folderForWrite1.CreateFileWriter("test1").GetOrThrow();
    string content1 = "abcdef";
    ASSERT_EQ(FSEC_OK, archiveFile->Write(content1.c_str(), content1.length()).Code());
    ASSERT_EQ(FSEC_OK, archiveFile->Close());
    ASSERT_EQ(FSEC_OK, folderForWrite1.Close());

    ArchiveFolder folderForWrite2(false);
    ASSERT_EQ(FSEC_OK, folderForWrite2.Open(_directory, "2"));
    archiveFile = folderForWrite2.CreateFileWriter("test2").GetOrThrow();
    string content2 = "hello world";
    ASSERT_EQ(FSEC_OK, archiveFile->Write(content2.c_str(), content2.length()).Code());
    ASSERT_EQ(FSEC_OK, archiveFile->Close());

    ArchiveFolder archiveFolderRead(false);
    ASSERT_EQ(FSEC_OK, archiveFolderRead.Open(_directory, ""));
    auto archiveFileReader = archiveFolderRead.CreateFileReader("test1").GetOrThrow();
    char buffer[20];
    int64_t readLen = archiveFileReader->Read(buffer, 20).GetOrThrow();
    string result(buffer, readLen);
    ASSERT_EQ(content1, result);
    ASSERT_EQ(FSEC_OK, archiveFileReader->Close());

    archiveFileReader = archiveFolderRead.CreateFileReader("test2").GetOrThrow();
    readLen = archiveFileReader->Read(buffer, 20).GetOrThrow();
    result = string(buffer, readLen);
    ASSERT_EQ(content2, result);
    ASSERT_EQ(FSEC_OK, archiveFileReader->Close());

    ASSERT_EQ(FSEC_OK, archiveFolderRead.Close());
    ASSERT_EQ(FSEC_OK, folderForWrite2.Close());
}

void ArchiveFolderTest::TestReadFromLogTmpFile()
{
    ArchiveFolder archiveFolderWrite(false);
    ASSERT_EQ(FSEC_OK, archiveFolderWrite.Open(_directory, "i"));
    auto archiveFile = archiveFolderWrite.CreateFileWriter("test1").GetOrThrow();
    string content = "abcdef";
    ASSERT_EQ(FSEC_OK, archiveFile->Write(content.c_str(), content.length()).Code());
    ASSERT_EQ(FSEC_OK, archiveFile->Close());
    ArchiveFolder archiveFolderRead(false);
    ASSERT_EQ(FSEC_OK, archiveFolderRead.Open(_directory));
    auto archiveFileToRead = archiveFolderRead.CreateFileReader("test1").GetOrThrow();
    char buffer[10];
    ASSERT_EQ(FSEC_OK, archiveFileToRead->Read(buffer, content.length()).Code());
    string result(buffer, content.length());
    ASSERT_EQ(content, result);
    ASSERT_EQ(FSEC_OK, archiveFileToRead->Close());
    ASSERT_EQ(FSEC_OK, archiveFolderRead.Close());
    ASSERT_EQ(FSEC_OK, archiveFolderWrite.Close());
}

void ArchiveFolderTest::TestMultiBlocks()
{
    ArchiveFolder archiveFolderWrite(false);
    ASSERT_EQ(FSEC_OK, archiveFolderWrite.Open(_directory, "i"));
    ArchiveFilePtr archiveFile =
        std::dynamic_pointer_cast<ArchiveFileWriter>(archiveFolderWrite.CreateFileWriter("test1").GetOrThrow())
            ->_archiveFile;

    archiveFile->ResetBufferSize(2);
    string content = "abcdef";
    ASSERT_EQ(FSEC_OK, archiveFile->Write(content.c_str(), content.length()).Code());
    ASSERT_EQ(FSEC_OK, archiveFile->Close());
    ArchiveFolder archiveFolderRead(false);
    ASSERT_EQ(FSEC_OK, archiveFolderRead.Open(_directory));
    auto archiveFileToRead = archiveFolderRead.CreateFileReader("test1").GetOrThrow();
    char buffer[10];
    ASSERT_EQ(FSEC_OK, archiveFileToRead->Read(buffer, content.length()).Code());
    string result(buffer, content.length());
    ASSERT_EQ(content, result);
    ASSERT_EQ(FSEC_OK, archiveFileToRead->Close());
    ASSERT_EQ(FSEC_OK, archiveFolderRead.Close());
    ASSERT_EQ(FSEC_OK, archiveFolderWrite.Close());
}

void ArchiveFolderTest::TestMultiArchiveFiles()
{
    ArchiveFolder archiveFolderWrite(false);
    ASSERT_EQ(FSEC_OK, archiveFolderWrite.Open(_directory, "i"));
    ArchiveFilePtr archiveFile1 =
        std::dynamic_pointer_cast<ArchiveFileWriter>(archiveFolderWrite.CreateFileWriter("test1").GetOrThrow())
            ->_archiveFile;
    archiveFile1->ResetBufferSize(2);
    ArchiveFilePtr archiveFile2 =
        std::dynamic_pointer_cast<ArchiveFileWriter>(archiveFolderWrite.CreateFileWriter("test2").GetOrThrow())
            ->_archiveFile;
    archiveFile2->ResetBufferSize(2);

    string content1 = "abcdef";
    string content2 = "hello world";
    ASSERT_EQ(FSEC_OK, archiveFile1->Write(content1.c_str(), content1.length()).Code());
    ASSERT_EQ(FSEC_OK, archiveFile2->Write(content2.c_str(), content2.length()).Code());
    ASSERT_EQ(FSEC_OK, archiveFile1->Close());
    ASSERT_EQ(FSEC_OK, archiveFile2->Close());

    ArchiveFolder archiveFolderRead(false);
    ASSERT_EQ(FSEC_OK, archiveFolderRead.Open(_directory));
    auto archiveFileToRead = archiveFolderRead.CreateFileReader("test1").GetOrThrow();
    char buffer[20];
    ASSERT_EQ(FSEC_OK, archiveFileToRead->Read(buffer, 20).Code());
    string result(buffer, content1.length());
    ASSERT_EQ(content1, result);

    char buffer2[20];
    archiveFileToRead = archiveFolderRead.CreateFileReader("test2").GetOrThrow();
    ASSERT_EQ(FSEC_OK, archiveFileToRead->Read(buffer2, 20).Code());
    string result2(buffer2, content2.length());
    ASSERT_EQ(content2, result2);

    ASSERT_EQ(FSEC_OK, archiveFileToRead->Close());
    ASSERT_EQ(FSEC_OK, archiveFolderRead.Close());
    ASSERT_EQ(FSEC_OK, archiveFolderWrite.Close());
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
    string errorMetaFile = R"(
26	0	2	test1_archive_file_block_0
26	2	2	test1_archive_file_block_1
26  4	2	test1_archive_file_block_2
)";
    string metaFilePath = util::PathUtil::JoinPath(GET_TEMP_DATA_PATH(), "INDEXLIB_LOG_FILE_2.meta");
    string dataTmpFilePath = util::PathUtil::JoinPath(GET_TEMP_DATA_PATH(), "INDEXLIB_LOG_FILE_2.tmp");
    FslibFileWrapperPtr metaFile(FslibWrapper::OpenFile(metaFilePath, fslib::WRITE).GetOrThrow());
    ASSERT_EQ(FSEC_OK, metaFile->Write(errorMetaFile.c_str(), errorMetaFile.length()).Code());
    ASSERT_EQ(FSEC_OK, metaFile->Close());

    FslibFileWrapperPtr dataTmpFile(FslibWrapper::OpenFile(dataTmpFilePath, fslib::WRITE).GetOrThrow());
    ASSERT_EQ(FSEC_OK, dataTmpFile->Write(logTmpFile.c_str(), logTmpFile.length()).Code());
    ASSERT_EQ(FSEC_OK, dataTmpFile->Close());

    ArchiveFolder archiveFolderRead(false);
    ASSERT_EQ(FSEC_OK, archiveFolderRead.Open(_directory, "2"));
    auto archiveFile2 = archiveFolderRead.CreateFileReader("test1").GetOrThrow();
    ASSERT_EQ(NULL, archiveFile2.get());
    string newContent = "test2 test2";
    ArchiveFolder archiveFolderWrite(false);
    ASSERT_EQ(FSEC_OK, archiveFolderWrite.Open(_directory, "2"));
    auto archiveFile3 = archiveFolderWrite.CreateFileWriter("test1").GetOrThrow();
    ASSERT_EQ(FSEC_OK, archiveFile3->Write(newContent.c_str(), newContent.size()).Code());
    ASSERT_EQ(FSEC_OK, archiveFile3->Close());

    ArchiveFolder archiveFolder(false);
    ASSERT_EQ(FSEC_OK, archiveFolder.Open(_directory, "2"));
    archiveFile2 = archiveFolder.CreateFileReader("test1").GetOrThrow();
    char buffer[20];
    ASSERT_EQ(FSEC_OK, archiveFile2->Read(buffer, 20).Code());
    string result(buffer, newContent.size());
    ASSERT_EQ(newContent, result);
    ASSERT_EQ(FSEC_OK, archiveFolder.Close());
    ASSERT_EQ(FSEC_OK, archiveFolderWrite.Close());
    ASSERT_EQ(FSEC_OK, archiveFolderRead.Close());
}

void ArchiveFolderTest::TestRead()
{
    ArchiveFolder archiveFolderWrite1(false);
    ASSERT_EQ(FSEC_OK, archiveFolderWrite1.Open(_directory, "1"));

    ArchiveFolder archiveFolderWrite2(false);
    ASSERT_EQ(FSEC_OK, archiveFolderWrite2.Open(_directory, "2"));

    ArchiveFilePtr archiveFile1 =
        std::dynamic_pointer_cast<ArchiveFileWriter>(archiveFolderWrite1.CreateFileWriter("test1").GetOrThrow())
            ->_archiveFile;
    archiveFile1->ResetBufferSize(2);
    ArchiveFilePtr archiveFile2 =
        std::dynamic_pointer_cast<ArchiveFileWriter>(archiveFolderWrite1.CreateFileWriter("test2").GetOrThrow())
            ->_archiveFile;
    archiveFile2->ResetBufferSize(2);
    ArchiveFilePtr archiveFile3 =
        std::dynamic_pointer_cast<ArchiveFileWriter>(archiveFolderWrite1.CreateFileWriter("test3").GetOrThrow())
            ->_archiveFile;
    archiveFile3->ResetBufferSize(2);

    string content = "abcdefghijkl";
    ASSERT_EQ(FSEC_OK, archiveFile1->Write(content.c_str(), content.length()).Code());
    content = "hello world, hello world";
    ASSERT_EQ(FSEC_OK, archiveFile2->Write(content.c_str(), content.length()).Code());
    content = "test3, test3";
    ASSERT_EQ(FSEC_OK, archiveFile3->Write(content.c_str(), content.length()).Code());

    ASSERT_EQ(FSEC_OK, archiveFile1->Close());

    ArchiveFolder archiveFolderRead(false);
    ASSERT_EQ(FSEC_OK, archiveFolderRead.Open(_directory));

    char buffer[30];
    auto archiveRead = archiveFolderRead.CreateFileReader("test3").GetOrThrow();
    ASSERT_EQ(NULL, archiveRead.get());
    archiveRead = archiveFolderRead.CreateFileReader("test2").GetOrThrow();
    ASSERT_EQ(NULL, archiveRead.get());

    ASSERT_EQ(FSEC_OK, archiveFile2->Close());
    archiveRead = archiveFolderRead.CreateFileReader("test1").GetOrThrow();
    ASSERT_EQ(3, archiveRead->Read(buffer, 3, 2).GetOrThrow());
    ASSERT_EQ("cde", string(buffer, 3));

    ASSERT_EQ(1, archiveRead->Read(buffer, 1, 0).GetOrThrow());
    ASSERT_EQ("a", string(buffer, 1));

    ASSERT_EQ(1, archiveRead->Read(buffer, 1, 11).GetOrThrow());
    ASSERT_EQ("l", string(buffer, 1));

    ASSERT_EQ(1, archiveRead->Read(buffer, 4, 11).GetOrThrow());
    ASSERT_EQ("l", string(buffer, 1));

    ASSERT_EQ(2, archiveRead->Read(buffer, 2, 2).GetOrThrow());
    ASSERT_EQ("cd", string(buffer, 2));

    ASSERT_EQ(5, archiveRead->Read(buffer, 5, 2).GetOrThrow());
    ASSERT_EQ("cdefg", string(buffer, 5));

    ASSERT_EQ(3, archiveRead->Read(buffer, 3).GetOrThrow());
    ASSERT_EQ("hij", string(buffer, 3));

    ASSERT_EQ(FSEC_OK, archiveFile3->Close());
    ASSERT_EQ(FSEC_OK, archiveFolderWrite1.Close());
    ASSERT_EQ(FSEC_OK, archiveFolderWrite2.Close());
    ASSERT_EQ(FSEC_OK, archiveFolderRead.Close());
}

void ArchiveFolderTest::TestMultiThreadWrite()
{
    auto write = [](ArchiveFilePtr archiveFile, int64_t times, int64_t interval, string content) {
        for (int64_t i = 0; i < times; i++) {
            ASSERT_EQ(FSEC_OK, archiveFile->Write(content.c_str(), content.size()).Code());
            usleep(interval);
        }
        ASSERT_EQ(FSEC_OK, archiveFile->Close());
    };
    ArchiveFolder archiveFolderWrite(false);
    ASSERT_EQ(FSEC_OK, archiveFolderWrite.Open(_directory, "2"));
    ArchiveFilePtr archiveFile1 =
        std::dynamic_pointer_cast<ArchiveFileWriter>(archiveFolderWrite.CreateFileWriter("test1").GetOrThrow())
            ->_archiveFile;
    archiveFile1->ResetBufferSize(500);
    ArchiveFilePtr archiveFile2 =
        std::dynamic_pointer_cast<ArchiveFileWriter>(archiveFolderWrite.CreateFileWriter("test2").GetOrThrow())
            ->_archiveFile;
    archiveFile2->ResetBufferSize(500);
    string content1 = "hello world";
    string content2 = "test test";
    thread t1(write, archiveFile1, 500, 10, content1);
    thread t2(write, archiveFile2, 300, 30, content2);
    t1.join();
    t2.join();
    ArchiveFolder archiveFolderRead(false);
    ASSERT_EQ(FSEC_OK, archiveFolderRead.Open(_directory, "2"));
    archiveFile1 =
        std::dynamic_pointer_cast<ArchiveFileReader>(archiveFolderRead.CreateFileReader("test1").GetOrThrow())
            ->_archiveFile;
    archiveFile2 =
        std::dynamic_pointer_cast<ArchiveFileReader>(archiveFolderRead.CreateFileReader("test2").GetOrThrow())
            ->_archiveFile;
    char buffer[15];
    ASSERT_EQ(11, archiveFile1->PRead(buffer, 11, 33).GetOrThrow());
    string result1(buffer, 11);
    ASSERT_EQ(content1, result1);
    ASSERT_EQ(9, archiveFile2->PRead(buffer, 9, 27).GetOrThrow());
    string result2(buffer, 9);
    ASSERT_EQ(content2, result2);

    ASSERT_EQ(FSEC_OK, archiveFile1->Close());
    ASSERT_EQ(FSEC_OK, archiveFile2->Close());
    ASSERT_EQ(FSEC_OK, archiveFolderWrite.Close());
    ASSERT_EQ(FSEC_OK, archiveFolderRead.Close());
}

void ArchiveFolderTest::TestWriteEmptyFile()
{
    ArchiveFolder archiveFolderWrite(false);
    ASSERT_EQ(FSEC_OK, archiveFolderWrite.Open(_directory, "1"));
    auto archiveFile = archiveFolderWrite.CreateFileWriter("test1").GetOrThrow();
    ASSERT_EQ(FSEC_OK, archiveFile->Close());
    ArchiveFolder archiveFolderRead(false);
    ASSERT_EQ(FSEC_OK, archiveFolderRead.Open(_directory));
    auto archiveFileReader = archiveFolderRead.CreateFileReader("test1").GetOrThrow();
    char buffer[10];
    ASSERT_EQ(0, archiveFileReader->Read(buffer, 10).GetOrThrow());
    ASSERT_EQ(FSEC_OK, archiveFolderWrite.Close());
    ASSERT_EQ(FSEC_OK, archiveFolderRead.Close());
}

void ArchiveFolderTest::TestReadOtherInstanceFile()
{
    ArchiveFolder archiveFolderWrite(false);
    // write hello world to test1
    ASSERT_EQ(FSEC_OK, archiveFolderWrite.Open(_directory, "1"));
    auto archiveFile = archiveFolderWrite.CreateFileWriter("test1").GetOrThrow();
    string content1 = "hello world";
    ASSERT_EQ(FSEC_OK, archiveFile->Write(content1.c_str(), content1.size()).Code());
    ASSERT_EQ(FSEC_OK, archiveFile->Close());

    ArchiveFolder archiveFolderRead(false);
    ASSERT_EQ(FSEC_OK, archiveFolderRead.Open(_directory, "2"));
    auto archiveFileReader = archiveFolderRead.CreateFileReader("test1").GetOrThrow();
    ASSERT_EQ(content1, archiveFileReader->TEST_Load());
    ASSERT_EQ(FSEC_OK, archiveFolderRead.Close());
    ASSERT_EQ(FSEC_OK, archiveFolderWrite.Close());
}

void ArchiveFolderTest::TestRewriteFile()
{
    ArchiveFolder archiveFolderWrite(false);
    // write hello world to test1
    ASSERT_EQ(FSEC_OK, archiveFolderWrite.Open(_directory, "1"));
    auto archiveFile = archiveFolderWrite.CreateFileWriter("test1").GetOrThrow();
    string content1 = "hello world";
    ASSERT_EQ(FSEC_OK, archiveFile->Write(content1.c_str(), content1.size()).Code());
    ASSERT_EQ(FSEC_OK, archiveFile->Close());
    // write hello ketty to test2
    string content2 = "hello kitty";
    archiveFile = archiveFolderWrite.CreateFileWriter("test2").GetOrThrow();
    ASSERT_EQ(FSEC_OK, archiveFile->Write(content2.c_str(), content2.size()).Code());
    ASSERT_EQ(FSEC_OK, archiveFile->Close());
    ASSERT_EQ(FSEC_OK, archiveFolderWrite.Close());

    // reopen folder and write nihao to test1
    ASSERT_EQ(FSEC_OK, archiveFolderWrite.Open(_directory, "1"));
    string content3 = "nihao";
    archiveFile = archiveFolderWrite.CreateFileWriter("test1").GetOrThrow();
    ASSERT_EQ(FSEC_OK, archiveFile->Write(content3.c_str(), content3.size()).Code());
    ASSERT_EQ(FSEC_OK, archiveFile->Close());
    ASSERT_EQ(FSEC_OK, archiveFolderWrite.Close());

    // read test1 = nihao test2 = hello kitty
    ArchiveFolder archiveFolderRead(false);
    ASSERT_EQ(FSEC_OK, archiveFolderRead.Open(_directory));
    auto archiveFileReader = archiveFolderRead.CreateFileReader("test1").GetOrThrow();
    ASSERT_EQ(content3, archiveFileReader->TEST_Load());
    archiveFileReader = archiveFolderRead.CreateFileReader("test2").GetOrThrow();
    ASSERT_EQ(content2, archiveFileReader->TEST_Load());
    ASSERT_EQ(FSEC_OK, archiveFolderRead.Close());
}
}} // namespace indexlib::file_system
