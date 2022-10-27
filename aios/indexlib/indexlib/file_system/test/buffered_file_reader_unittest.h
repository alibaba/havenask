#ifndef __INDEXLIB_BUFFEREDFILEREADERTEST_H
#define __INDEXLIB_BUFFEREDFILEREADERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/file_system/buffered_file_reader.h"

IE_NAMESPACE_BEGIN(file_system);

class BufferedFileReaderTest : public INDEXLIB_TESTBASE
{
public:
    BufferedFileReaderTest();
    ~BufferedFileReaderTest();

    DECLARE_CLASS_NAME(BufferedFileReaderTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestCaseForRead();
    void TestCaseForOffsetWithinOneBuffer();
    void TestCaseForSkipForwardThenBackward();
    void TestCaseForRevertReadAllFileWithOffset();
    void TestCaseForSeqReadAllFileWithOffset();
    void TestCaseForReadBigFile();

private:
    void ForReadCase(uint32_t bufferSize, uint32_t dataLength, bool async);
    void MakeFileData(uint32_t dataLength, std::string& answer);
    void MakeData(uint32_t dataLength, std::string& answer);
    void CheckFileReader(BufferedFileReader& reader, const std::string& answer);

    FileNodePtr PrepareFileNode();
    FileReaderPtr PrepareFileReader(std::string& answer, size_t fileLength);

private:
    std::string mFilePath;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(BufferedFileReaderTest, TestCaseForRead);
INDEXLIB_UNIT_TEST_CASE(BufferedFileReaderTest, TestCaseForOffsetWithinOneBuffer);
INDEXLIB_UNIT_TEST_CASE(BufferedFileReaderTest, TestCaseForSkipForwardThenBackward);
INDEXLIB_UNIT_TEST_CASE(BufferedFileReaderTest, TestCaseForRevertReadAllFileWithOffset);
INDEXLIB_UNIT_TEST_CASE(BufferedFileReaderTest, TestCaseForSeqReadAllFileWithOffset);
INDEXLIB_UNIT_TEST_CASE(BufferedFileReaderTest, TestCaseForReadBigFile);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_BUFFEREDFILEREADERTEST_H
