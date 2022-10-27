#include "indexlib/util/byte_slice_list/byte_slice_list.h"
#include "indexlib/file_system/test/in_mem_file_node_unittest.h"
#include "indexlib/file_system/in_mem_file_node.h"
#include "indexlib/file_system/test/file_system_test_util.h"
#include "indexlib/util/memory_control/memory_quota_controller_creator.h"

using namespace std;
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, InMemFileNodeTest);

InMemFileNodeTest::InMemFileNodeTest()
{
}

InMemFileNodeTest::~InMemFileNodeTest()
{
}

void InMemFileNodeTest::CaseSetUp()
{
    mRootDir = GET_TEST_DATA_PATH();
    mMemoryController = MemoryQuotaControllerCreator::CreateBlockMemoryController();
}

void InMemFileNodeTest::CaseTearDown()
{
}

void InMemFileNodeTest::TestInMemFileRead()
{
    string filePath = mRootDir + "in_mem_file";
    string fileContext = "in mem file read from disk";
    FileSystemTestUtil::CreateDiskFile(filePath, fileContext);
    InMemFileNode fileNode(0, true, LoadConfig(), mMemoryController);

    fileNode.Open(filePath, FSOT_IN_MEM);
    fileNode.Populate();
    ASSERT_EQ(fileContext.size(), fileNode.GetLength());
    ASSERT_EQ(filePath, fileNode.GetPath());

    uint8_t buffer[1024];
    size_t readSize = fileNode.Read(buffer, fileContext.size(), 0);
    ASSERT_EQ(fileContext.size(), readSize);
    ASSERT_TRUE(memcmp(fileContext.data(), buffer, readSize) == 0);
    ASSERT_THROW(fileNode.Read(buffer, fileContext.size() + 1, 0), 
                 OutOfRangeException);
}

void InMemFileNodeTest::TestForByteSliceList()
{
    InMemFileNode fileNode(1024, false, LoadConfig(), mMemoryController);
    fileNode.Open("", FSOT_IN_MEM);
    uint8_t buffer[256];
    for (int i = 0; i < 256; i++)
    {
        buffer[i] = i;
    }

    fileNode.Write(buffer, 256);
    ByteSliceListPtr byteSliceList(fileNode.Read(256, 0));
    ASSERT_TRUE(byteSliceList.get());
    ByteSlice* byteSlice = byteSliceList->GetHead();
    ASSERT_TRUE(byteSlice);
    ASSERT_EQ((uint32_t)256, byteSlice->size);
    ASSERT_EQ(0, memcmp(byteSlice->data, buffer, 256));
    byteSliceList->Clear(NULL);

    ASSERT_THROW(fileNode.Read(257, 0), OutOfRangeException);
}

void InMemFileNodeTest::TestBigData()
{
    InMemFileNode fileNode((size_t)4 * 1024, false, LoadConfig(), mMemoryController);
    fileNode.Open("", FSOT_IN_MEM);
    for (size_t i = 0; i < 1024 * 1024; ++i)
    {
        fileNode.Write("abcd", 4);
    }
    ASSERT_EQ((size_t)4 * 1024 * 1024, fileNode.GetLength());

    char* addr = (char* )fileNode.GetBaseAddress();
    for (size_t i = 0; i < 4 * 1024 * 1024; i += 4)
    {
        ASSERT_EQ('a', addr[i]);
        ASSERT_EQ('b', addr[i+1]);
        ASSERT_EQ('c', addr[i+2]);
        ASSERT_EQ('d', addr[i+3]);
    }
}

void InMemFileNodeTest::TestClone()
{
    InMemFileNode fileNode((size_t)10, false, LoadConfig(), mMemoryController);

    string value("abcdef", 6);
    string path = "/home/yida/in_mem_file";
    fileNode.Open(path, FSOT_IN_MEM);
    fileNode.Write(value.data(), value.length());
    fileNode.SetDirty(true);
    fileNode.SetInPackage(true);

    InMemFileNodePtr clonedFileNode(fileNode.Clone());
    ASSERT_EQ(value.length(), clonedFileNode->GetLength());

    string cloneValue;
    cloneValue.resize(value.length());
    clonedFileNode->Read((void*)cloneValue.data(), value.length(), 0);
    ASSERT_EQ(value, cloneValue);

    ASSERT_EQ(FSOT_IN_MEM, clonedFileNode->GetOpenType());
    ASSERT_TRUE(clonedFileNode->IsDirty());
    ASSERT_TRUE(clonedFileNode->IsInPackage());
    ASSERT_EQ(path, clonedFileNode->GetPath());
}

void InMemFileNodeTest::TestReserveSize()
{
    int64_t totalQuota = 10 * 1024 * 1024;
    mMemoryController = MemoryQuotaControllerCreator::CreateBlockMemoryController(totalQuota);
    InMemFileNode fileNode((size_t)100, false, LoadConfig(), mMemoryController);
    string value("abcdef", 6);
    string path = "/home/yida/in_mem_file";
    fileNode.Open(path, FSOT_IN_MEM);

    fileNode.Resize(30);
    int64_t freeQuota = mMemoryController->GetFreeQuota();
    while (freeQuota > 0)
    {
        fileNode.Write(value.data(), value.length());
        freeQuota = mMemoryController->GetFreeQuota();
    }
    fileNode.Close();
    ASSERT_EQ(totalQuota, mMemoryController->GetFreeQuota());
}

IE_NAMESPACE_END(file_system);

