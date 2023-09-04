#include "indexlib/common/hash_table/test/hash_table_reader_unittest.h"

#include "indexlib/common/hash_table/hash_table_writer.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/file_system/mock/MockDirectory.h"
#include "indexlib/file_system/mock/MockFileReader.h"

using namespace std;
using namespace testing;
using namespace indexlib::file_system;

namespace indexlib { namespace common {
IE_LOG_SETUP(common, HashTableReaderTest);

HashTableReaderTest::HashTableReaderTest() {}

HashTableReaderTest::~HashTableReaderTest() {}

void HashTableReaderTest::CaseSetUp() {}

void HashTableReaderTest::CaseTearDown() {}

ACTION_P(THROW_EXCEPTION, needThrowException)
{
    if (needThrowException) {
        INDEXLIB_THROW(util::IndexCollapsedException, "Mock Throw");
    }
    return NULL;
}

void HashTableReaderTest::TestException()
{
    // fileLength is smaller than sizeof bucketCount + nodeCount
    DirectoryPtr rootDir = GET_PARTITION_DIRECTORY();
    FileWriterPtr fileWriter = rootDir->CreateFileWriter("hash_table");
    uint32_t data = 1;
    fileWriter->Write(&data, sizeof(data)).GetOrThrow();
    fileWriter->Close().GetOrThrow();
    HashTableReader<uint64_t, uint64_t> hashTableReader;
    ASSERT_ANY_THROW(hashTableReader.Open(rootDir, "hash_table"));

    // fileLength is different from calculated expect length
    fileWriter = rootDir->CreateFileWriter("hash_table2");
    uint32_t nodeCount = 1;
    uint32_t bucketCount = 1;
    fileWriter->Write(&nodeCount, sizeof(nodeCount)).GetOrThrow();
    fileWriter->Write(&bucketCount, sizeof(bucketCount)).GetOrThrow();
    fileWriter->Close().GetOrThrow();
    HashTableReader<uint64_t, uint64_t> hashTableReader2;
    ASSERT_ANY_THROW(hashTableReader2.Open(rootDir, "hash_table2"));

    // fileReader exception on GetBaseAddress
    MockDirectoryPtr mockDir(new MockDirectory("mockdir"));
    std::shared_ptr<MockFileReader> mockFileReader(new NiceMock<MockFileReader>);
    EXPECT_CALL(*mockDir, DoCreateFileReader(_, _))
        .WillRepeatedly(Return(FSResult<std::shared_ptr<FileReader>>(FSEC_OK, mockFileReader)));
    EXPECT_CALL(*mockFileReader, GetLength()).WillRepeatedly(Return(100));
    string path;
    EXPECT_CALL(*mockFileReader, GetLogicalPath()).WillRepeatedly(ReturnRef(path));
    EXPECT_CALL(*mockFileReader, GetBaseAddress()).WillRepeatedly(THROW_EXCEPTION(true));
    EXPECT_CALL(*mockFileReader, GetOpenType()).WillRepeatedly(Return(FSOT_UNKNOWN));
    HashTableReader<uint64_t, uint64_t> hashTableReader3;
    ASSERT_ANY_THROW(hashTableReader3.Open(IDirectory::ToLegacyDirectory(mockDir), "hash_table3"));
}

void HashTableReaderTest::TestSimpleProcess()
{
    HashTableWriter<uint64_t, uint64_t> hashTableWriter;
    const size_t bucketCount = 100;
    DirectoryPtr rootDir = GET_PARTITION_DIRECTORY();
    hashTableWriter.Init(rootDir, "hash_table", bucketCount);
    for (size_t i = 0; i < 200; ++i) {
        hashTableWriter.Add(i, i);
    }
    hashTableWriter.Close();

    HashTableReader<uint64_t, uint64_t> hashTableReader;
    hashTableReader.Open(rootDir, "hash_table");
    ASSERT_EQ(bucketCount, hashTableReader.GetBucketCount());
    ASSERT_EQ(200u, hashTableReader.GetNodeCount());
    uint64_t value;
    for (size_t i = 0; i < 200; ++i) {
        ASSERT_TRUE(hashTableReader.Find(i, value));
        ASSERT_EQ(i, value);
    }
    ASSERT_FALSE(hashTableReader.Find(332, value));
}
}} // namespace indexlib::common
