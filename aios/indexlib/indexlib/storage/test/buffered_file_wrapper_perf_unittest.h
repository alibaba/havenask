#ifndef __INDEXLIB_BUFFEREDFILEWRAPPERMULTITHREADTEST_H
#define __INDEXLIB_BUFFEREDFILEWRAPPERMULTITHREADTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/test/multi_thread_test_base.h"
#include "indexlib/storage/file_system_wrapper.h"

IE_NAMESPACE_BEGIN(storage);

class BufferedFileWrapperMultiThreadTest : public test::MultiThreadTestBase
{
public:
    BufferedFileWrapperMultiThreadTest();
    ~BufferedFileWrapperMultiThreadTest();

    DECLARE_CLASS_NAME(BufferedFileWrapperMultiThreadTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestMultiThreadOpenFileForRead();
    void TestMultiThreadOpenFileForWrite();
    void TestMultiThreadOpenFileForReadAndWrite();

private:
    void DoWrite() override;
    void DoRead(int* status) override;

    void WriteFile(const std::string& fileName);
    void ReadFile(const std::string& fileName);

private:
    std::string mDirectory;
    std::string mFileNameForRead;

    std::string mOpenModeForReadThread;
    std::string mOpenModeForWriteThread;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(BufferedFileWrapperMultiThreadTest, TestMultiThreadOpenFileForRead);
INDEXLIB_UNIT_TEST_CASE(BufferedFileWrapperMultiThreadTest, TestMultiThreadOpenFileForWrite);
INDEXLIB_UNIT_TEST_CASE(BufferedFileWrapperMultiThreadTest, TestMultiThreadOpenFileForReadAndWrite);

IE_NAMESPACE_END(storage);

#endif //__INDEXLIB_BUFFEREDFILEWRAPPERMULTITHREADTEST_H
