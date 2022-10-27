#include "indexlib/common/test/in_mem_file_writer_unittest.h"

using namespace std;
IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(common, InMemFileWriterTest);

InMemFileWriterTest::InMemFileWriterTest()
{
}

InMemFileWriterTest::~InMemFileWriterTest()
{
}

void InMemFileWriterTest::CaseSetUp()
{
}

void InMemFileWriterTest::CaseTearDown()
{
}

void InMemFileWriterTest::TestSimpleProcess()
{
    SCOPED_TRACE("Failed");
    InMemFileWriter writer;
    writer.Init(2);
    writer.Open("test");
    writer.Write("1", 1);
    char* data = (char*)writer.GetBaseAddress();
    ASSERT_EQ('1', data[0]);
    ASSERT_EQ((uint64_t)1, writer.GetLength());

    writer.Write("234", 3);
    char* newData = (char*)writer.GetBaseAddress();
    ASSERT_FALSE(data == newData);
    ASSERT_EQ((uint64_t)4, writer.GetLength());
    for (size_t i = 0; i < 4; i++)
    {
        ASSERT_EQ((char)('1' + i), newData[i]);
    }
}

IE_NAMESPACE_END(common);

