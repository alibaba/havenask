#include "indexlib/file_system/test/in_mem_file_unittest.h"
#include "indexlib/util/memory_control/memory_quota_controller_creator.h"

using namespace std;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, InMemFileTest);

InMemFileTest::InMemFileTest()
{
}

InMemFileTest::~InMemFileTest()
{
}

void InMemFileTest::CaseSetUp()
{
    mRootDir = GET_TEST_DATA_PATH();
    mMemoryController = util::MemoryQuotaControllerCreator::CreateBlockMemoryController();
}

void InMemFileTest::CaseTearDown()
{
}

void InMemFileTest::TestSimpleProcess()
{
    InMemFileNode inMemFileNode(2, false, LoadConfig(), mMemoryController);

    string path = mRootDir + "test";
    inMemFileNode.Open(path, FSOT_IN_MEM);
    
    size_t writeSize = inMemFileNode.Write("1", 1);
    ASSERT_EQ((size_t)1, writeSize);

    char* data = (char*)inMemFileNode.GetBaseAddress();
    ASSERT_EQ('1', data[0]);
    ASSERT_EQ((size_t)1, inMemFileNode.GetLength());

    inMemFileNode.Write("234", 3);
    char* newData = (char*)inMemFileNode.GetBaseAddress();
    ASSERT_FALSE(data == newData);
    ASSERT_EQ((size_t)4, inMemFileNode.GetLength());
    for (size_t i = 0; i < 4; i++)
    {
        ASSERT_EQ((char)('1' + i), newData[i]);
    }
    
    uint8_t readBuffer[10];
    size_t readSize = inMemFileNode.Read(readBuffer, 3, 1);
    ASSERT_EQ((size_t)3, readSize);
    for (size_t i = 0; i < 3; i++)
    {
        ASSERT_EQ((uint8_t)('2' + i), readBuffer[i]) << "Read Off:" << i;
    }    

    inMemFileNode.Close();
}

void InMemFileTest::TestReadOutOfRange()
{
    InMemFileNodePtr file(new InMemFileNode(10, false, LoadConfig(), mMemoryController));
    file->Open("abc", FSOT_IN_MEM);
    uint8_t buffer[1024];
    file->Write("1234567890", 10);
    EXPECT_NO_THROW(file->Read(buffer, 10, 0));
    ASSERT_ANY_THROW(file->Read(buffer, 10, 1));
    ASSERT_ANY_THROW(file->Read(buffer, 0, 11));
    ASSERT_ANY_THROW(file->Read(buffer, 11, 0));
}

void InMemFileTest::TestReservedSizeZero()
{
    InMemFileNodePtr file(new InMemFileNode(0, false, LoadConfig(), mMemoryController));
    file->Open("abc", FSOT_IN_MEM);
    file->Write("1234567", 7);
    
    ASSERT_EQ((size_t)7, file->GetLength());
    ASSERT_EQ((size_t)7, file->mCapacity);
}

IE_NAMESPACE_END(file_system);

