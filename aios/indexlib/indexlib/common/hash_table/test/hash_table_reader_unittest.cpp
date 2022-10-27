#include "indexlib/common/hash_table/test/hash_table_reader_unittest.h"
#include "indexlib/common/hash_table/hash_table_writer.h"
#include "indexlib/file_system/file_writer.h"
#include "indexlib/file_system/test/mock_directory.h"

using namespace std;
using namespace testing;
IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(common, HashTableReaderTest);

class MockFileReader : public file_system::FileReader
{
public:
    MOCK_METHOD0(Open, void());
    MOCK_METHOD4(Read, size_t(void*, size_t, size_t, ReadOption));
    MOCK_METHOD3(Read, size_t(void*, size_t, ReadOption));
    MOCK_METHOD3(Read, util::ByteSliceList*(size_t, size_t, ReadOption));
    MOCK_CONST_METHOD0(GetBaseAddress, void*());
    MOCK_CONST_METHOD0(GetLength, size_t());
    MOCK_CONST_METHOD0(GetPath, const std::string&());
    MOCK_METHOD0(Close, void());
    MOCK_METHOD0(GetOpenType, FSOpenType());
    MOCK_CONST_METHOD0(GetFileNode, FileNodePtr());
};

DEFINE_SHARED_PTR(MockFileReader);

HashTableReaderTest::HashTableReaderTest()
{
}

HashTableReaderTest::~HashTableReaderTest()
{
}

void HashTableReaderTest::CaseSetUp()
{
}

void HashTableReaderTest::CaseTearDown()
{
}

ACTION_P(THROW_EXCEPTION, needThrowException)
{
    if (needThrowException)
    {
        INDEXLIB_THROW(misc::IndexCollapsedException, "Mock Throw");
    }
    return NULL;
}

void HashTableReaderTest::TestException()
{
    // fileLength is smaller than sizeof bucketCount + nodeCount
    DirectoryPtr rootDir = GET_PARTITION_DIRECTORY();
    FileWriterPtr fileWriter = rootDir->CreateFileWriter("hash_table");
    uint32_t data = 1;
    fileWriter->Write(&data, sizeof(data));
    fileWriter->Close();
    HashTableReader<uint64_t, uint64_t> hashTableReader;
    ASSERT_ANY_THROW(hashTableReader.Open(rootDir, "hash_table"));

    // fileLength is different from calculated expect length
    fileWriter = rootDir->CreateFileWriter("hash_table2");
    uint32_t nodeCount = 1;
    uint32_t bucketCount = 1;
    fileWriter->Write(&nodeCount, sizeof(nodeCount));
    fileWriter->Write(&bucketCount, sizeof(bucketCount));
    fileWriter->Close();
    HashTableReader<uint64_t, uint64_t> hashTableReader2;
    ASSERT_ANY_THROW(hashTableReader2.Open(rootDir, "hash_table2"));

    // fileReader exception on GetBaseAddress
    MockDirectoryPtr mockDir(new MockDirectory("mockdir"));
    MockFileReaderPtr mockFileReader(new MockFileReader);
    EXPECT_CALL(*mockDir, CreateFileReader(_,_))
        .WillRepeatedly(Return(mockFileReader));
    EXPECT_CALL(*mockFileReader, GetLength())
        .WillRepeatedly(Return(100));
    EXPECT_CALL(*mockFileReader, GetBaseAddress())
        .WillRepeatedly(THROW_EXCEPTION(true));
    EXPECT_CALL(*mockFileReader, GetOpenType())
        .WillRepeatedly(Return(FSOT_UNKNOWN));
    HashTableReader<uint64_t, uint64_t> hashTableReader3;
    ASSERT_ANY_THROW(hashTableReader3.Open(mockDir, "hash_table3"));
}

void HashTableReaderTest::TestSimpleProcess()
{
    HashTableWriter<uint64_t, uint64_t> hashTableWriter;
    const size_t bucketCount = 100;
    DirectoryPtr rootDir = GET_PARTITION_DIRECTORY();
    hashTableWriter.Init(rootDir, "hash_table", bucketCount);
    for (size_t i = 0; i < 200; ++i)
    {
        hashTableWriter.Add(i, i);
    }
    hashTableWriter.Close();

    HashTableReader<uint64_t, uint64_t> hashTableReader;
    hashTableReader.Open(rootDir, "hash_table");
    ASSERT_EQ(bucketCount, hashTableReader.GetBucketCount());
    ASSERT_EQ(200u, hashTableReader.GetNodeCount());
    uint64_t value;
    for (size_t i = 0; i < 200; ++i)
    {
        ASSERT_TRUE(hashTableReader.Find(i, value));
        ASSERT_EQ(i, value);
    }
    ASSERT_FALSE(hashTableReader.Find(332, value));
}

IE_NAMESPACE_END(common);

