#include "indexlib/file_system/test/in_mem_file_writer_unittest.h"
#include "indexlib/file_system/test/file_system_creator.h"
#include "indexlib/file_system/test/test_file_creator.h"

using namespace std;

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, InMemFileWriterTest);

InMemFileWriterTest::InMemFileWriterTest()
{
}

InMemFileWriterTest::~InMemFileWriterTest()
{
}

void InMemFileWriterTest::CaseSetUp()
{
    mRootDir = GET_TEST_DATA_PATH();
}

void InMemFileWriterTest::CaseTearDown()
{
}

void InMemFileWriterTest::TestSimpleProcess()
{
    const size_t times = 1024 * 1024;
    IndexlibFileSystemPtr ifs = FileSystemCreator::CreateFileSystem(mRootDir);
    FileWriterPtr writer = TestFileCreator::CreateWriter(
            ifs, FSST_IN_MEM, FSOT_IN_MEM);
    for (size_t i = 0; i < times; ++i)
    {
        writer->Write("abcd", 4);
    }
    ASSERT_EQ((size_t)4 * times, writer->GetLength());
    writer->Close();

    FileReaderPtr reader = TestFileCreator::CreateReader(
            ifs, FSST_IN_MEM, FSOT_IN_MEM);

    ASSERT_EQ((size_t)4 * times, reader->GetLength());
    char* addr = (char* )reader->GetBaseAddress();
    for (size_t i = 0; i < 4 * times; i += 4)
    {
        ASSERT_EQ('a', addr[i]);
        ASSERT_EQ('b', addr[i+1]);
        ASSERT_EQ('c', addr[i+2]);
        ASSERT_EQ('d', addr[i+3]);
    }
}

IE_NAMESPACE_END(file_system);

