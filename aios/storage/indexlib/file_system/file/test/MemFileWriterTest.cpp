#include "gtest/gtest.h"
#include <cstddef>
#include <memory>

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/file_system/test/TestFileCreator.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;

namespace indexlib { namespace file_system {

class MemFileWriterTest : public INDEXLIB_TESTBASE
{
public:
    MemFileWriterTest();
    ~MemFileWriterTest();

    DECLARE_CLASS_NAME(MemFileWriterTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    std::string _rootDir;
    std::shared_ptr<IFileSystem> _fileSystem;

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP(indexlib.file_system, MemFileWriterTest);

INDEXLIB_UNIT_TEST_CASE(MemFileWriterTest, TestSimpleProcess);
//////////////////////////////////////////////////////////////////////

MemFileWriterTest::MemFileWriterTest() {}

MemFileWriterTest::~MemFileWriterTest() {}

void MemFileWriterTest::CaseSetUp()
{
    _rootDir = GET_TEMP_DATA_PATH();
    _fileSystem = FileSystemCreator::Create("ut", GET_TEMP_DATA_PATH()).GetOrThrow();
}

void MemFileWriterTest::CaseTearDown() {}

void MemFileWriterTest::TestSimpleProcess()
{
    const size_t times = 1024 * 1024;
    std::shared_ptr<FileWriter> writer = TestFileCreator::CreateWriter(_fileSystem, FSST_MEM, FSOT_MEM);
    for (size_t i = 0; i < times; ++i) {
        ASSERT_EQ(FSEC_OK, writer->Write("abcd", 4).Code());
    }
    ASSERT_EQ((size_t)4 * times, writer->GetLength());
    ASSERT_EQ(FSEC_OK, writer->Close());

    std::shared_ptr<FileReader> reader = TestFileCreator::CreateReader(_fileSystem, FSST_MEM, FSOT_MEM);

    ASSERT_EQ((size_t)4 * times, reader->GetLength());
    char* addr = (char*)reader->GetBaseAddress();
    for (size_t i = 0; i < 4 * times; i += 4) {
        ASSERT_EQ('a', addr[i]);
        ASSERT_EQ('b', addr[i + 1]);
        ASSERT_EQ('c', addr[i + 2]);
        ASSERT_EQ('d', addr[i + 3]);
    }
}
}} // namespace indexlib::file_system
