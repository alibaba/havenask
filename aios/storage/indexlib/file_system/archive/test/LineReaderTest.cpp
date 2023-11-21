#include "indexlib/file_system/archive/LineReader.h"

#include "gtest/gtest.h"
#include <algorithm>
#include <iosfwd>
#include <memory>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;
using namespace autil;

namespace indexlib { namespace file_system {
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
    void TestNextLineForMMapFile();

private:
    void InnerTestNextLine(const std::string& content);
    void MakeFileData(const std::string& fileContent);
    void CheckData(const std::string& expectedFileStr);

private:
    std::string _filePath;
    std::shared_ptr<Directory> _directory;

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP(indexlib.file_system, LineReaderTest);

INDEXLIB_UNIT_TEST_CASE(LineReaderTest, TestNextLine);
INDEXLIB_UNIT_TEST_CASE(LineReaderTest, TestNextLineForMMapFile);
//////////////////////////////////////////////////////////////////////

LineReaderTest::LineReaderTest() {}

LineReaderTest::~LineReaderTest() {}

void LineReaderTest::CaseSetUp()
{
    _directory = Directory::Get(FileSystemCreator::Create("", GET_TEMP_DATA_PATH()).GetOrThrow());
    _filePath = "test_file";
}

void LineReaderTest::CaseTearDown() {}

void LineReaderTest::TestNextLine()
{
    // empty file
    InnerTestNextLine("");

    // one line without '\n'
    InnerTestNextLine("abccd");

    // many lines
    InnerTestNextLine("abcdd\nabawsdf\nabsss\n");

    // many lines but last line without '\n'
    InnerTestNextLine("abcdd\nabawsdf\nabsss");

    // large line
    string largeLine(100000, 'a');
    largeLine += '\n';
    largeLine += "abddfsd";
    InnerTestNextLine(largeLine);
}

void LineReaderTest::InnerTestNextLine(const string& content)
{
    tearDown();
    setUp();
    MakeFileData(content);
    CheckData(content);
}

void LineReaderTest::MakeFileData(const string& fileContent)
{
    std::shared_ptr<FileWriter> file = _directory->CreateFileWriter(_filePath);
    ASSERT_EQ(fileContent.length(), file->Write(fileContent.c_str(), fileContent.length()).GetOrThrow());
    ASSERT_EQ(FSEC_OK, file->Close());
}

void LineReaderTest::CheckData(const string& expectedFileStr)
{
    vector<string> expectedStrVec;
    StringUtil::fromString(expectedFileStr, expectedStrVec, "\n");

    vector<string> actualStrVec;
    LineReader reader;
    reader.Open(_directory->CreateFileReader(_filePath, FSOT_BUFFERED));

    string line;
    ErrorCode ec = FSEC_OK;
    while (reader.NextLine(line, ec)) {
        actualStrVec.push_back(line);
    }
    ASSERT_EQ(FSEC_OK, ec);
    ASSERT_TRUE(expectedStrVec == actualStrVec);
}

void LineReaderTest::TestNextLineForMMapFile()
{
    MakeFileData("abc");
    LineReader reader;
    reader.Open(_directory->CreateFileReader(_filePath, FSOT_MMAP));
    string line;
    ErrorCode ec = FSEC_OK;
    ASSERT_TRUE(reader.NextLine(line, ec));
}

}} // namespace indexlib::file_system
