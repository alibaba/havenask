#include "indexlib/storage/test/buffered_file_wrapper_perf_unittest.h"
#include "indexlib/storage/buffered_file_wrapper.h"
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;
using namespace fslib::fs;
using namespace fslib;
IE_NAMESPACE_USE(test);

IE_NAMESPACE_BEGIN(storage);
IE_LOG_SETUP(storage, BufferedFileWrapperMultiThreadTest);

BufferedFileWrapperMultiThreadTest::BufferedFileWrapperMultiThreadTest()
{
}

BufferedFileWrapperMultiThreadTest::~BufferedFileWrapperMultiThreadTest()
{
}

void BufferedFileWrapperMultiThreadTest::CaseSetUp()
{
    mDirectory = GET_TEST_DATA_PATH();
    mFileNameForRead = FileSystemWrapper::JoinPath(mDirectory, "read.file");
    FileSystemWrapper::AtomicStore(mFileNameForRead, "hi");
}

void BufferedFileWrapperMultiThreadTest::CaseTearDown()
{
}

void BufferedFileWrapperMultiThreadTest::TestMultiThreadOpenFileForRead()
{
    mOpenModeForReadThread = "read";
    mOpenModeForWriteThread = "read";
    DoMultiThreadTest(10, 1);
}
void BufferedFileWrapperMultiThreadTest::TestMultiThreadOpenFileForWrite()
{
    mOpenModeForReadThread = "write";
    mOpenModeForWriteThread = "write";
    DoMultiThreadTest(10, 1);
}
void BufferedFileWrapperMultiThreadTest::TestMultiThreadOpenFileForReadAndWrite()
{
    mOpenModeForReadThread = "read";
    mOpenModeForWriteThread = "write";
    DoMultiThreadTest(10, 1);
}

void BufferedFileWrapperMultiThreadTest::DoWrite()
{
    if (mOpenModeForWriteThread == "read")
    {
        ReadFile(mFileNameForRead);
    }
    else
    {
        string fileName = StringUtil::toString(pthread_self());
        fileName = FileSystemWrapper::JoinPath(mDirectory, fileName);
        WriteFile(fileName);
    }
}

void BufferedFileWrapperMultiThreadTest::DoRead(int* status)
{
    if (mOpenModeForReadThread == "read")
    {
        ReadFile(mFileNameForRead);
    }
    else
    {
        string fileName = StringUtil::toString(pthread_self());
        fileName = FileSystemWrapper::JoinPath(mDirectory, fileName);
        WriteFile(fileName);
    }
}

void BufferedFileWrapperMultiThreadTest::WriteFile(const std::string& fileName)
{
    BufferedFileWrapperPtr bufferedFile(FileSystemWrapper::OpenBufferedFile(
                    fileName, WRITE, 1024, true));
    string buffer = "hello";
    bufferedFile->Write((const void*)buffer.c_str(), buffer.size());
    bufferedFile->Close();
    string fileContent;
    FileSystemWrapper::AtomicLoad(fileName, fileContent);
    ASSERT_EQ(fileContent, buffer);
}

void BufferedFileWrapperMultiThreadTest::ReadFile(const std::string& fileName)
{
    string fileContent;
    FileSystemWrapper::AtomicLoad(fileName, fileContent);

    BufferedFileWrapperPtr bufferedFile(FileSystemWrapper::OpenBufferedFile(
                    fileName, READ, 1024, true));
    size_t fileLen = bufferedFile->GetFileLength();
    ASSERT_EQ(fileContent.size(), fileLen);

    char buffer[1024];
    ASSERT_EQ(fileLen, bufferedFile->Read((void*)buffer, fileLen));
    bufferedFile->Close();
    ASSERT_EQ(fileContent, string(buffer, fileLen));
}

IE_NAMESPACE_END(storage);

