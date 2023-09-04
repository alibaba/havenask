#include "gtest/gtest.h"
#include <cstddef>
#include <memory>

#include "autil/CommonMacros.h"
#include "autil/StringUtil.h"
#include "autil/legacy/exception.h"
#include "fslib/fs/ExceptionTrigger.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/archive/ArchiveFile.h"
#include "indexlib/file_system/archive/ArchiveFileReader.h"
#include "indexlib/file_system/archive/ArchiveFileWriter.h"
#include "indexlib/file_system/archive/ArchiveFolder.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;
using namespace autil;
using fslib::fs::ExceptionTrigger;

namespace indexlib { namespace file_system {

class ArchiveFolderExceptionTest : public INDEXLIB_TESTBASE
{
public:
    ArchiveFolderExceptionTest();
    ~ArchiveFolderExceptionTest();

    DECLARE_CLASS_NAME(ArchiveFolderExceptionTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    void WriteFile(ArchiveFolder& folder, int64_t fileIdx, const std::string& fileContentPrefix);
    void CheckFiles(int64_t maxFileIdx, const std::string& fileContent);
    void WriteFiles(const std::string& content, int64_t& writeSuccessIdx);

private:
    std::shared_ptr<IDirectory> _directory;

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP(indexlib.file_system, ArchiveFolderExceptionTest);

INDEXLIB_UNIT_TEST_CASE(ArchiveFolderExceptionTest, TestSimpleProcess);
//////////////////////////////////////////////////////////////////////

ArchiveFolderExceptionTest::ArchiveFolderExceptionTest() {}

ArchiveFolderExceptionTest::~ArchiveFolderExceptionTest() {}

void ArchiveFolderExceptionTest::CaseSetUp()
{
    _directory = IDirectory::Get(FileSystemCreator::Create("", GET_TEMP_DATA_PATH()).GetOrThrow());
    FslibWrapper::Reset();
    ExceptionTrigger::InitTrigger(ExceptionTrigger::NO_EXCEPTION_COUNT);
}

void ArchiveFolderExceptionTest::CaseTearDown()
{
    FslibWrapper::Reset();
    ExceptionTrigger::InitTrigger(ExceptionTrigger::NO_EXCEPTION_COUNT);
}

void ArchiveFolderExceptionTest::TestSimpleProcess()
{
    for (size_t i = 0; i < 200; i++) {
        FslibWrapper::Reset();
        ExceptionTrigger::InitTrigger(i);
        int64_t writeSuccessIdx = -1;
        string content = string("hello world") + StringUtil::toString(i);
        WriteFiles(content, writeSuccessIdx);
        CheckFiles(writeSuccessIdx, content);
        writeSuccessIdx = -1;
        content = string("hello kitty") + StringUtil::toString(i);
        WriteFiles(content, writeSuccessIdx);
        CheckFiles(writeSuccessIdx, content);
    }
}

void ArchiveFolderExceptionTest::WriteFiles(const std::string& content, int64_t& writeSuccessIdx)
{
    ArchiveFolder archiveFolderWrite(false);
    try {
        archiveFolderWrite.Open(_directory, "").GetOrThrow();
        for (size_t i = 0; i < 12; i++) {
            WriteFile(archiveFolderWrite, i, content);
            writeSuccessIdx = i;
        }
        ASSERT_EQ(FSEC_OK, archiveFolderWrite.Close());
    } catch (const autil::legacy::ExceptionBase& e) {
        FslibWrapper::Reset();
        ExceptionTrigger::InitTrigger(ExceptionTrigger::NO_EXCEPTION_COUNT);
    }
}

void ArchiveFolderExceptionTest::WriteFile(ArchiveFolder& folder, int64_t fileIdx, const string& fileContentPrefix)
{
    string fileName = string("file_") + StringUtil::toString(fileIdx);
    string fileContent = fileName + "_" + fileContentPrefix;
    // ArchiveFilePtr archiveFile = std::dynamic_pointer_cast<ArchiveFile>(folder.CreateFileWriter(fileName));
    ArchiveFilePtr archiveFile =
        std::dynamic_pointer_cast<ArchiveFileWriter>(folder.CreateFileWriter(fileName).GetOrThrow())->_archiveFile;
    archiveFile->ResetBufferSize(2);
    ASSERT_EQ(FSEC_OK, archiveFile->Write(fileContent.c_str(), fileContent.length()).Code());
    ASSERT_EQ(FSEC_OK, archiveFile->Close());
}

void ArchiveFolderExceptionTest::CheckFiles(int64_t maxFileIdx, const std::string& content)
{
    if (maxFileIdx < 0) {
        return;
    }
    ArchiveFolder archiveFolder(false);
    ASSERT_EQ(FSEC_OK, archiveFolder.Open(_directory, ""));
    char buffer[30];
    for (size_t i = 0; i <= (size_t)maxFileIdx; i++) {
        string fileName = string("file_") + StringUtil::toString(i);
        string fileContent = fileName + "_" + content;
        auto archiveFile = archiveFolder.CreateFileReader(fileName).GetOrThrow();
        ASSERT_EQ(FSEC_OK, archiveFile->Read(buffer, fileContent.length()).Code());
        ASSERT_EQ(fileContent, string(buffer, fileContent.length()));
        ASSERT_EQ(FSEC_OK, archiveFile->Close());
    }
    ASSERT_EQ(FSEC_OK, archiveFolder.Close());
}
}} // namespace indexlib::file_system
