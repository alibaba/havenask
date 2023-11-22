#include "gtest/gtest.h"
#include <cstddef>
#include <memory>
#include <ostream>
#include <stdint.h>

#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/file/MemFileNode.h"
#include "indexlib/file_system/load_config/LoadConfig.h"
#include "indexlib/util/byte_slice_list/ByteSliceList.h"
#include "indexlib/util/memory_control/MemoryQuotaControllerCreator.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;

using namespace indexlib::util;
namespace indexlib { namespace file_system {

class InMemFileTest : public INDEXLIB_TESTBASE
{
public:
    InMemFileTest();
    ~InMemFileTest();

    DECLARE_CLASS_NAME(InMemFileTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestReadOutOfRange();
    void TestReservedSizeZero();

private:
    std::string _rootDir;
    util::BlockMemoryQuotaControllerPtr _memoryController;

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP(indexlib.file_system, InMemFileTest);

INDEXLIB_UNIT_TEST_CASE(InMemFileTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(InMemFileTest, TestReadOutOfRange);
INDEXLIB_UNIT_TEST_CASE(InMemFileTest, TestReservedSizeZero);
//////////////////////////////////////////////////////////////////////

InMemFileTest::InMemFileTest() {}

InMemFileTest::~InMemFileTest() {}

void InMemFileTest::CaseSetUp()
{
    _rootDir = GET_TEMP_DATA_PATH();
    _memoryController = util::MemoryQuotaControllerCreator::CreateBlockMemoryController();
}

void InMemFileTest::CaseTearDown() {}

void InMemFileTest::TestSimpleProcess()
{
    MemFileNode inMemFileNode(2, false, LoadConfig(), _memoryController);

    string path = _rootDir + "test";
    ASSERT_EQ(FSEC_OK, inMemFileNode.Open("LOGICAL_PATH", path, FSOT_MEM, -1));

    size_t writeSize = inMemFileNode.Write("1", 1).GetOrThrow();
    ASSERT_EQ((size_t)1, writeSize);

    char* data = (char*)inMemFileNode.GetBaseAddress();
    ASSERT_EQ('1', data[0]);
    ASSERT_EQ((size_t)1, inMemFileNode.GetLength());

    ASSERT_EQ(FSEC_OK, inMemFileNode.Write("234", 3).Code());
    char* newData = (char*)inMemFileNode.GetBaseAddress();
    ASSERT_FALSE(data == newData);
    ASSERT_EQ((size_t)4, inMemFileNode.GetLength());
    for (size_t i = 0; i < 4; i++) {
        ASSERT_EQ((char)('1' + i), newData[i]);
    }

    uint8_t readBuffer[10];
    size_t readSize = inMemFileNode.Read(readBuffer, 3, 1, ReadOption()).GetOrThrow();
    ASSERT_EQ((size_t)3, readSize);
    for (size_t i = 0; i < 3; i++) {
        ASSERT_EQ((uint8_t)('2' + i), readBuffer[i]) << "Read Off:" << i;
    }

    ASSERT_EQ(FSEC_OK, inMemFileNode.Close());
}

void InMemFileTest::TestReadOutOfRange()
{
    std::shared_ptr<MemFileNode> file(new MemFileNode(10, false, LoadConfig(), _memoryController));
    ASSERT_EQ(FSEC_OK, file->Open("LOGICAL_PATH", "abc", FSOT_MEM, -1));
    uint8_t buffer[1024];
    ASSERT_EQ(FSEC_OK, file->Write("1234567890", 10).Code());
    ASSERT_EQ(FSEC_OK, file->Read(buffer, 10, 0, ReadOption()).Code());
    ASSERT_EQ(FSEC_BADARGS, file->Read(buffer, 10, 1, ReadOption()).Code());
    ASSERT_EQ(FSEC_BADARGS, file->Read(buffer, 0, 11, ReadOption()).Code());
    ASSERT_EQ(FSEC_BADARGS, file->Read(buffer, 11, 0, ReadOption()).Code());
}

void InMemFileTest::TestReservedSizeZero()
{
    std::shared_ptr<MemFileNode> file(new MemFileNode(0, false, LoadConfig(), _memoryController));
    ASSERT_EQ(FSEC_OK, file->Open("LOGICAL_PATH", "abc", FSOT_MEM, -1));
    ASSERT_EQ(FSEC_OK, file->Write("1234567", 7).Code());

    ASSERT_EQ((size_t)7, file->GetLength());
    ASSERT_EQ((size_t)7, file->_capacity);
}
}} // namespace indexlib::file_system
