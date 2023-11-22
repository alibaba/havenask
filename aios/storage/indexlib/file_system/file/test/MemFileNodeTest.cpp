#include "indexlib/file_system/file/MemFileNode.h"

#include "gtest/gtest.h"
#include <iosfwd>
#include <memory>
#include <stdint.h>
#include <string.h>

#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/load_config/LoadConfig.h"
#include "indexlib/file_system/test/FileSystemTestUtil.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/byte_slice_list/ByteSliceList.h"
#include "indexlib/util/memory_control/BlockMemoryQuotaController.h"
#include "indexlib/util/memory_control/MemoryQuotaControllerCreator.h"
#include "indexlib/util/testutil/unittest.h"

namespace indexlib { namespace util {
class OutOfRangeException;
}} // namespace indexlib::util
using namespace std;
using namespace indexlib::util;

namespace indexlib { namespace file_system {

class MemFileNodeTest : public INDEXLIB_TESTBASE
{
public:
    MemFileNodeTest();
    ~MemFileNodeTest();

    DECLARE_CLASS_NAME(MemFileNodeTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestForByteSliceList();
    void TestInMemFileRead();
    void TestBigData();
    void TestClone();
    void TestReserveSize();

private:
    std::string _rootDir;
    util::BlockMemoryQuotaControllerPtr _memoryController;

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP(indexlib.file_system, MemFileNodeTest);

INDEXLIB_UNIT_TEST_CASE(MemFileNodeTest, TestForByteSliceList);
INDEXLIB_UNIT_TEST_CASE(MemFileNodeTest, TestInMemFileRead);
INDEXLIB_UNIT_TEST_CASE(MemFileNodeTest, TestBigData);
INDEXLIB_UNIT_TEST_CASE(MemFileNodeTest, TestClone);
INDEXLIB_UNIT_TEST_CASE(MemFileNodeTest, TestReserveSize);
//////////////////////////////////////////////////////////////////////

MemFileNodeTest::MemFileNodeTest() {}

MemFileNodeTest::~MemFileNodeTest() {}

void MemFileNodeTest::CaseSetUp()
{
    _rootDir = PathUtil::NormalizePath(GET_TEMP_DATA_PATH());
    _memoryController = MemoryQuotaControllerCreator::CreateBlockMemoryController();
}

void MemFileNodeTest::CaseTearDown() {}

void MemFileNodeTest::TestInMemFileRead()
{
    string filePath = _rootDir + "in_mem_file";
    string fileContext = "in mem file read from disk";
    FileSystemTestUtil::CreateDiskFile(filePath, fileContext);
    MemFileNode fileNode(0, true, LoadConfig(), _memoryController);

    ASSERT_EQ(FSEC_OK, fileNode.Open("LOGICAL_PATH", filePath, FSOT_MEM, -1));
    ASSERT_EQ(FSEC_OK, fileNode.Populate());
    ASSERT_EQ(fileContext.size(), fileNode.GetLength());
    ASSERT_EQ(filePath, fileNode.GetPhysicalPath());

    uint8_t buffer[1024];
    size_t readSize = fileNode.Read(buffer, fileContext.size(), 0, ReadOption()).GetOrThrow();
    ASSERT_EQ(fileContext.size(), readSize);
    ASSERT_TRUE(memcmp(fileContext.data(), buffer, readSize) == 0);
    ASSERT_EQ(FSEC_BADARGS, fileNode.Read(buffer, fileContext.size() + 1, 0, ReadOption()).Code());
}

void MemFileNodeTest::TestForByteSliceList()
{
    MemFileNode fileNode(1024, false, LoadConfig(), _memoryController);
    ASSERT_EQ(FSEC_OK, fileNode.Open("LOGICAL_PATH", "", FSOT_MEM, -1));
    uint8_t buffer[256];
    for (int i = 0; i < 256; i++) {
        buffer[i] = i;
    }

    ASSERT_EQ(FSEC_OK, fileNode.Write(buffer, 256).Code());
    ByteSliceListPtr byteSliceList(fileNode.ReadToByteSliceList(256, 0, ReadOption()));
    ASSERT_TRUE(byteSliceList.get());
    ByteSlice* byteSlice = byteSliceList->GetHead();
    ASSERT_TRUE(byteSlice);
    ASSERT_EQ((uint32_t)256, byteSlice->size);
    ASSERT_EQ(0, memcmp(byteSlice->data, buffer, 256));
    byteSliceList->Clear(NULL);

    ASSERT_EQ(nullptr, fileNode.ReadToByteSliceList(257, 0, ReadOption()));
}

void MemFileNodeTest::TestBigData()
{
    MemFileNode fileNode((size_t)4 * 1024, false, LoadConfig(), _memoryController);
    ASSERT_EQ(FSEC_OK, fileNode.Open("LOGICAL_PATH", "", FSOT_MEM, -1));
    for (size_t i = 0; i < 1024 * 1024; ++i) {
        ASSERT_EQ(FSEC_OK, fileNode.Write("abcd", 4).Code());
    }
    ASSERT_EQ((size_t)4 * 1024 * 1024, fileNode.GetLength());

    char* addr = (char*)fileNode.GetBaseAddress();
    for (size_t i = 0; i < 4 * 1024 * 1024; i += 4) {
        ASSERT_EQ('a', addr[i]);
        ASSERT_EQ('b', addr[i + 1]);
        ASSERT_EQ('c', addr[i + 2]);
        ASSERT_EQ('d', addr[i + 3]);
    }
}

void MemFileNodeTest::TestClone()
{
    MemFileNode fileNode((size_t)10, false, LoadConfig(), _memoryController);

    string value("abcdef", 6);
    string path = "/home/yida/in_mem_file";
    ASSERT_EQ(FSEC_OK, fileNode.Open("LOGICAL_PATH", path, FSOT_MEM, -1));
    ASSERT_EQ(FSEC_OK, fileNode.Write(value.data(), value.length()).Code());
    fileNode.SetDirty(true);
    fileNode.SetInPackage(true);

    std::shared_ptr<MemFileNode> clonedFileNode(fileNode.Clone());
    ASSERT_EQ(value.length(), clonedFileNode->GetLength());

    string cloneValue;
    cloneValue.resize(value.length());
    ASSERT_EQ(FSEC_OK, clonedFileNode->Read((void*)cloneValue.data(), value.length(), 0, ReadOption()).Code());
    ASSERT_EQ(value, cloneValue);

    ASSERT_EQ(FSOT_MEM, clonedFileNode->GetOpenType());
    ASSERT_TRUE(clonedFileNode->IsDirty());
    ASSERT_TRUE(clonedFileNode->IsInPackage());
    ASSERT_EQ(path, clonedFileNode->GetPhysicalPath());
}

void MemFileNodeTest::TestReserveSize()
{
    int64_t totalQuota = 10 * 1024 * 1024;
    _memoryController = MemoryQuotaControllerCreator::CreateBlockMemoryController(totalQuota);
    MemFileNode fileNode((size_t)100, false, LoadConfig(), _memoryController);
    string value("abcdef", 6);
    string path = "/home/yida/in_mem_file";
    ASSERT_EQ(FSEC_OK, fileNode.Open("LOGICAL_PATH", path, FSOT_MEM, -1));

    fileNode.Resize(30);
    int64_t freeQuota = _memoryController->GetFreeQuota();
    while (freeQuota > 0) {
        ASSERT_EQ(FSEC_OK, fileNode.Write(value.data(), value.length()).Code());
        freeQuota = _memoryController->GetFreeQuota();
    }
    ASSERT_EQ(FSEC_OK, fileNode.Close());
    ASSERT_EQ(totalQuota, _memoryController->GetFreeQuota());
}
}} // namespace indexlib::file_system
