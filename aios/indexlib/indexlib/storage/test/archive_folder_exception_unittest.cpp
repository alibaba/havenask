#include "indexlib/storage/test/archive_folder_exception_unittest.h"
#include "indexlib/storage/exception_trigger.h"
#include <autil/StringUtil.h>
using namespace std;
using namespace autil;
//IE_NAMESPACE_USE(test);

IE_NAMESPACE_BEGIN(storage);
IE_LOG_SETUP(storage, ArchiveFolderExceptionTest);

ArchiveFolderExceptionTest::ArchiveFolderExceptionTest()
{
}

ArchiveFolderExceptionTest::~ArchiveFolderExceptionTest()
{
}

void ArchiveFolderExceptionTest::CaseSetUp()
{
    FileSystemWrapper::Reset();
    ExceptionTrigger::InitTrigger(ExceptionTrigger::NO_EXCEPTION_COUNT);
}

void ArchiveFolderExceptionTest::CaseTearDown()
{
    FileSystemWrapper::Reset();
    ExceptionTrigger::InitTrigger(ExceptionTrigger::NO_EXCEPTION_COUNT);
}

void ArchiveFolderExceptionTest::TestSimpleProcess()
{
    for (size_t i = 0; i < 200; i++)
    {
        FileSystemWrapper::Reset();
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

void ArchiveFolderExceptionTest::WriteFiles(
        const std::string& content,
        int64_t& writeSuccessIdx)
{
    ArchiveFolder archiveFolderWrite(false);
    try {
        archiveFolderWrite.Open(GET_TEST_DATA_PATH());
        for (size_t i = 0; i < 12; i++)
        {
            WriteFile(archiveFolderWrite, i, content);
            writeSuccessIdx = i;
        }
        archiveFolderWrite.Close();
    } catch(const autil::legacy::ExceptionBase& e)
    {
        FileSystemWrapper::Reset();
        ExceptionTrigger::InitTrigger(ExceptionTrigger::NO_EXCEPTION_COUNT);
        //archiveFolderWrite.Close();
    }    
}

void ArchiveFolderExceptionTest::WriteFile(
        ArchiveFolder& folder, int64_t fileIdx,
        const string& fileContentPrefix)
{
    string fileName = string("file_") + 
                      StringUtil::toString(fileIdx);
    string fileContent = fileName  + "_"
                         + fileContentPrefix;
    ArchiveFilePtr archiveFile = 
        DYNAMIC_POINTER_CAST(
                ArchiveFile, folder.GetInnerFile(fileName, fslib::WRITE));
    archiveFile->ResetBufferSize(2);
    archiveFile->Write(fileContent.c_str(), fileContent.length());
    archiveFile->Close();
}

void ArchiveFolderExceptionTest::CheckFiles(
        int64_t maxFileIdx,const std::string& content)
{
    if (maxFileIdx < 0)
    {
        return;
    }
    ArchiveFolder archiveFolder(false);
    archiveFolder.Open(GET_TEST_DATA_PATH());
    for (size_t i = 0; i <= (size_t)maxFileIdx; i++)
    {
        string fileName = string("file_") + 
                          StringUtil::toString(i);
        string fileContent = fileName  + "_"
                          + content;
        FileWrapperPtr archiveFile = 
            archiveFolder.GetInnerFile(fileName, fslib::READ);
        string readResult;
        FileSystemWrapper::AtomicLoad(archiveFile.get(), readResult);
        ASSERT_EQ(readResult, fileContent);
        archiveFile->Close();
    }
    archiveFolder.Close();
}

IE_NAMESPACE_END(storage);

