#include "indexlib/storage/test/line_reader_unittest.h"
#include "indexlib/storage/file_system_wrapper.h"
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;

IE_NAMESPACE_BEGIN(storage);
IE_LOG_SETUP(storage, LineReaderTest);

LineReaderTest::LineReaderTest()
{
}

LineReaderTest::~LineReaderTest()
{
}

void LineReaderTest::CaseSetUp()
{
    string dir = GET_TEST_DATA_PATH();
    mFilePath = dir + "test_file";
}

void LineReaderTest::CaseTearDown()
{
    FileSystemWrapper::DeleteFile(mFilePath);
}

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

void LineReaderTest::InnerTestNextLine(const string &content)
{
    MakeFileData(content);
    CheckData(content);
}

void LineReaderTest::MakeFileData(const string &fileContent)
{
    FileWrapper *file = FileSystemWrapper::OpenFile(mFilePath, fslib::WRITE);
    file->Write(fileContent.c_str(), fileContent.length());
    file->Close();
    delete file;
}

void LineReaderTest::CheckData(const string &expectedFileStr)
{
    vector<string> expectedStrVec;
    StringUtil::fromString(expectedFileStr, expectedStrVec, "\n");

    vector<string> actualStrVec;
    LineReader reader;
    reader.Open(mFilePath);

    string line;
    while (reader.NextLine(line))
    {
        actualStrVec.push_back(line);
    }
    ASSERT_TRUE(expectedStrVec == actualStrVec);
}

IE_NAMESPACE_END(storage);

