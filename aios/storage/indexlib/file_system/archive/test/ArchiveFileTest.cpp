#include "indexlib/file_system/archive/ArchiveFile.h"

#include "gtest/gtest.h"
#include <cstddef>
#include <memory>

#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/archive/ArchiveFileReader.h"
#include "indexlib/file_system/archive/ArchiveFileWriter.h"
#include "indexlib/file_system/archive/ArchiveFolder.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/file_system/fslib/FslibFileWrapper.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;

namespace indexlib { namespace file_system {
class ArchiveFileTest : public INDEXLIB_TESTBASE
{
public:
    ArchiveFileTest();
    ~ArchiveFileTest();

    DECLARE_CLASS_NAME(ArchiveFileTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestRewrite();
    void TestRead();
    void TestGetFileLength();
    void TestFlush();

private:
    void InnerTestRead(size_t writeBufferSize);
    void CheckResult(const std::string& content, size_t beginPos, size_t readLen, size_t actualReadLen,
                     char* readResult);

private:
    std::shared_ptr<IDirectory> _directory;

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP(indexlib.file_system, ArchiveFileTest);

INDEXLIB_UNIT_TEST_CASE(ArchiveFileTest, TestRewrite);
INDEXLIB_UNIT_TEST_CASE(ArchiveFileTest, TestRead);
INDEXLIB_UNIT_TEST_CASE(ArchiveFileTest, TestGetFileLength);
INDEXLIB_UNIT_TEST_CASE(ArchiveFileTest, TestFlush);
//////////////////////////////////////////////////////////////////////

ArchiveFileTest::ArchiveFileTest() {}

ArchiveFileTest::~ArchiveFileTest() {}

void ArchiveFileTest::CaseSetUp()
{
    _directory = IDirectory::Get(FileSystemCreator::Create("", GET_TEMP_DATA_PATH()).GetOrThrow());
}

void ArchiveFileTest::CaseTearDown() {}

void ArchiveFileTest::TestRead()
{
    for (size_t i = 1; i < 12; i++) {
        InnerTestRead(i);
    }
}

void ArchiveFileTest::InnerTestRead(size_t writeBufferSize)
{
    tearDown();
    setUp();
    ArchiveFolder folder(false);
    ASSERT_EQ(FSEC_OK, folder.Open(_directory, "1"));
    ArchiveFilePtr archiveFile =
        std::dynamic_pointer_cast<ArchiveFileWriter>(folder.CreateFileWriter("test1").GetOrThrow())->_archiveFile;
    archiveFile->ResetBufferSize(writeBufferSize);
    string content = "0123456789";
    ASSERT_EQ(FSEC_OK, archiveFile->Write(content.c_str(), content.size()).Code());
    ASSERT_EQ(FSEC_OK, archiveFile->Close());
    ASSERT_EQ(FSEC_OK, folder.Close());

    // test read
    ArchiveFolder archiveFolder(false);
    ASSERT_EQ(FSEC_OK, archiveFolder.Open(_directory, ""));
    ArchiveFilePtr readFile =
        std::dynamic_pointer_cast<ArchiveFileReader>(archiveFolder.CreateFileReader("test1").GetOrThrow())
            ->_archiveFile;
    ASSERT_EQ(writeBufferSize, readFile->GetBufferSize());
    char buffer[10];
    for (size_t readLen = 1; readLen < 12; readLen++) {
        for (size_t beginPos = 0; beginPos < 12; beginPos++) {
            size_t len = readFile->PRead(buffer, readLen, beginPos).GetOrThrow();
            CheckResult(content, beginPos, readLen, len, buffer);
        }
    }
    ASSERT_EQ(FSEC_OK, archiveFolder.Close());
}

void ArchiveFileTest::CheckResult(const string& content, size_t beginPos, size_t readLen, size_t actualReadLen,
                                  char* readResult)
{
    if (beginPos >= content.size()) {
        ASSERT_EQ(0u, actualReadLen);
        return;
    }
    size_t leftLen = content.size() - beginPos;
    size_t expectReadLen = leftLen > readLen ? readLen : leftLen;
    ASSERT_EQ(expectReadLen, actualReadLen);
    for (size_t i = 0; i < expectReadLen; i++) {
        ASSERT_EQ(readResult[i], content[beginPos + i]);
    }
}

void ArchiveFileTest::TestRewrite()
{
    ArchiveFolder archiveFolderWrite(false);
    ASSERT_EQ(FSEC_OK, archiveFolderWrite.Open(_directory, "1"));
    // write hello world and close
    auto archiveFile = archiveFolderWrite.CreateFileWriter("test1").GetOrThrow();
    string content1 = "hello world";
    ASSERT_EQ(FSEC_OK, archiveFile->Write(content1.c_str(), content1.size()).Code());
    ASSERT_EQ(FSEC_OK, archiveFile->Close());
    // write nihao and not close
    archiveFile = archiveFolderWrite.CreateFileWriter("test1").GetOrThrow();
    string content2 = "nihao";
    ASSERT_EQ(FSEC_OK, archiveFile->Write(content2.c_str(), content2.size()).Code());
    ASSERT_EQ(FSEC_OK, std::dynamic_pointer_cast<ArchiveFileWriter>(archiveFile)->_archiveFile->Flush());
    // check read file get "hello world" not "nihao"
    ArchiveFolder archiveFolderRead(false);
    ASSERT_EQ(FSEC_OK, archiveFolderRead.Open(_directory, "1"));
    auto archiveFileReader = archiveFolderRead.CreateFileReader("test1").GetOrThrow();
    ASSERT_EQ(content1, archiveFileReader->TEST_Load());
    // write hello kitty and close
    archiveFile = archiveFolderWrite.CreateFileWriter("test1").GetOrThrow();
    string content3 = "hello kitty";
    ASSERT_EQ(FSEC_OK, archiveFile->Write(content3.c_str(), content3.size()).Code());
    ASSERT_EQ(FSEC_OK, archiveFile->Close());
    // last read folder can not read new file
    archiveFileReader = archiveFolderRead.CreateFileReader("test1").GetOrThrow();
    ASSERT_EQ(content1, archiveFileReader->TEST_Load());
    ASSERT_EQ(FSEC_OK, archiveFolderRead.Close());
    // prepare new read folder
    ASSERT_EQ(FSEC_OK, archiveFolderRead.Open(_directory, "1"));
    archiveFileReader = archiveFolderRead.CreateFileReader("test1").GetOrThrow();
    ASSERT_EQ(content3, archiveFileReader->TEST_Load());
    ASSERT_EQ(FSEC_OK, archiveFolderRead.Close());
    ASSERT_EQ(FSEC_OK, archiveFolderWrite.Close());
    ASSERT_EQ(FSEC_OK, archiveFileReader->Close());
}

void ArchiveFileTest::TestGetFileLength()
{
    ArchiveFolder archiveFolderWrite(false);
    ASSERT_EQ(FSEC_OK, archiveFolderWrite.Open(_directory, "1"));
    ArchiveFilePtr archiveFile =
        std::dynamic_pointer_cast<ArchiveFileWriter>(archiveFolderWrite.CreateFileWriter("test1").GetOrThrow())
            ->_archiveFile;
    string content1 = "hello world";
    ASSERT_EQ(FSEC_OK, archiveFile->Write(content1.c_str(), content1.size()).Code());
    ASSERT_EQ(FSEC_OK, archiveFile->Close());
    ASSERT_EQ(content1.size(), archiveFile->GetFileLength());

    // empty file
    content1 = "";
    archiveFile.reset();
    archiveFile =
        std::dynamic_pointer_cast<ArchiveFileWriter>(archiveFolderWrite.CreateFileWriter("test1").GetOrThrow())
            ->_archiveFile;
    ASSERT_EQ(FSEC_OK, archiveFile->Write(content1.c_str(), content1.size()).Code());
    ASSERT_EQ(FSEC_OK, archiveFile->Close());
    ASSERT_EQ(content1.size(), archiveFile->GetFileLength());
    ASSERT_EQ(FSEC_OK, archiveFolderWrite.Close());
}

void ArchiveFileTest::TestFlush()
{
    ArchiveFolder folder(false);
    ASSERT_EQ(FSEC_OK, folder.Open(_directory, "1"));
    ArchiveFilePtr archiveFile1 =
        std::dynamic_pointer_cast<ArchiveFileWriter>(folder.CreateFileWriter("test1").GetOrThrow())->_archiveFile;
    ArchiveFilePtr archiveFile2 =
        std::dynamic_pointer_cast<ArchiveFileWriter>(folder.CreateFileWriter("test2").GetOrThrow())->_archiveFile;

    string content1 = "hello world1";
    string content2 = "hello World222";
    ASSERT_EQ(FSEC_OK, archiveFile2->Write(content2.c_str(), content2.size()).Code());
    ASSERT_EQ(FSEC_OK, archiveFile1->Write(content1.c_str(), content1.size()).Code());
    ASSERT_EQ(FSEC_OK, archiveFile1->Flush());

    string metaFileContent = R"(
30	0	12	test1_0_0_archive_file_block_0
)";

    string dataFilePath = util::PathUtil::JoinPath(GET_TEMP_DATA_PATH(), "INDEXLIB_ARCHIVE_FILES_1_0.lfd");
    string metaFilePath = util::PathUtil::JoinPath(GET_TEMP_DATA_PATH(), "INDEXLIB_ARCHIVE_FILES_1_0.lfm");
    string realContent;
    FslibFileWrapperPtr file(FslibWrapper::OpenFile(dataFilePath, fslib::READ).GetOrThrow());
    FslibFileWrapperPtr metaFile(FslibWrapper::OpenFile(metaFilePath, fslib::READ).GetOrThrow());
    ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicLoad(file.get(), realContent).Code());
    ASSERT_EQ(content1, realContent);
    realContent.clear();
    ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicLoad(metaFile.get(), realContent).Code());
    ASSERT_EQ(metaFileContent, realContent);
    ASSERT_EQ(FSEC_OK, archiveFile2->Flush());
    realContent.clear();
    ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicLoad(file.get(), realContent).Code());
    ASSERT_EQ(content1 + content2, realContent);
    metaFileContent = R"(
30	0	12	test1_0_0_archive_file_block_0
30	12	14	test2_0_1_archive_file_block_0
)";
    realContent.clear();
    ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicLoad(metaFile.get(), realContent).Code());
    ASSERT_EQ(metaFileContent, realContent);

    ASSERT_EQ(FSEC_OK, file->Close());
    ASSERT_EQ(FSEC_OK, metaFile->Close());
    ASSERT_EQ(FSEC_OK, folder.Close());
}
}} // namespace indexlib::file_system
