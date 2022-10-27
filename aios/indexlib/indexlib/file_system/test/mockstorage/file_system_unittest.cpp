#include "indexlib/file_system/test/mockstorage/file_system_unittest.h"
#include "indexlib/file_system/indexlib_file_system.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/file_system/test/test_file_creator.h"
#include "indexlib/util/path_util.h"
#include "indexlib/misc/exception.h"

using namespace std;
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, FileSystemTest);

FileSystemTest::FileSystemTest()
{
}

FileSystemTest::~FileSystemTest()
{
}

void FileSystemTest::CaseSetUp()
{
    mFileSystem = GET_FILE_SYSTEM();
    mRootDir = mFileSystem->GetRootPath();
    FileSystemWrapper::ClearError();
}

void FileSystemTest::CaseTearDown()
{
}

void FileSystemTest::TestAtomicStoreForLocalStorage()
{
    string filePath = PathUtil::JoinPath(mRootDir, "dir", "file");
    FileSystemWrapper::SetError(FileSystemWrapper::RENAME_ERROR,
                                filePath + TEMP_FILE_SUFFIX);
    FSWriterParam writerParam;
    writerParam.atomicDump = true;
    FileWriterPtr fileWriter = TestFileCreator::CreateWriter(
        mFileSystem, FSST_LOCAL, FSOT_BUFFERED, "", filePath, writerParam);
    ASSERT_THROW(fileWriter->Close(), FileIOException);
}

void FileSystemTest::TestAtomicStoreForInMemStorage()
{
    string filePath = PathUtil::JoinPath(mRootDir, "dir", "segment_info");
    FileSystemWrapper::SetError(FileSystemWrapper::RENAME_ERROR,
                                filePath + TEMP_FILE_SUFFIX);
    FSWriterParam writerParam;
    writerParam.atomicDump = true;
    FileWriterPtr fileWriter = TestFileCreator::CreateWriter(
        mFileSystem, FSST_IN_MEM, FSOT_IN_MEM, "", filePath, writerParam);
    ASSERT_NO_THROW(fileWriter->Close());
    ASSERT_THROW(mFileSystem->Sync(true), FileIOException);
}

IE_NAMESPACE_END(file_system);

