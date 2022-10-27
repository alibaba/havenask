#ifndef __INDEXLIB_BUFFEREDFILEWRITERTEST_H
#define __INDEXLIB_BUFFEREDFILEWRITERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/file_system/buffered_file_writer.h"

IE_NAMESPACE_BEGIN(file_system);

class BufferedFileWriterTest : public INDEXLIB_TESTBASE
{
public:
    BufferedFileWriterTest();
    ~BufferedFileWriterTest();

    DECLARE_CLASS_NAME(BufferedFileWriterTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestCloseException();
    void TestBufferedFileException();

private:
    void CheckFile(const std::string& fileName, const std::string& content);
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(BufferedFileWriterTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(BufferedFileWriterTest, TestCloseException);
INDEXLIB_UNIT_TEST_CASE(BufferedFileWriterTest, TestBufferedFileException);
IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_BUFFEREDFILEWRITERTEST_H
