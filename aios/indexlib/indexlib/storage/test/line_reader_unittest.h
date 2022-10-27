#ifndef __INDEXLIB_LINEREADERTEST_H
#define __INDEXLIB_LINEREADERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/storage/line_reader.h"

IE_NAMESPACE_BEGIN(storage);

class LineReaderTest : public INDEXLIB_TESTBASE
{
public:
    LineReaderTest();
    ~LineReaderTest();

    DECLARE_CLASS_NAME(LineReaderTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestNextLine();
private:
    void InnerTestNextLine(const std::string &content);
    void MakeFileData(const std::string &fileContent);
    void CheckData(const std::string &expectedFileStr);
private:
    std::string mFilePath;
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(LineReaderTest, TestNextLine);

IE_NAMESPACE_END(storage);

#endif //__INDEXLIB_LINEREADERTEST_H
