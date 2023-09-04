#include "indexlib/index/common/hash_table/HashTableReader.h"

#include "gmock/gmock.h"

#include "autil/Log.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/file_system/mock/MockDirectory.h"
#include "indexlib/file_system/mock/MockFileReader.h"
#include "indexlib/index/common/hash_table/HashTableReader.h"
#include "indexlib/index/common/hash_table/HashTableWriter.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;
using namespace testing;
using namespace indexlib::file_system;

namespace indexlibv2::index {

class HashTableReaderTest : public indexlib::INDEXLIB_TESTBASE
{
public:
    HashTableReaderTest();
    ~HashTableReaderTest();

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(indexlib.index, HashTableReaderTest);

HashTableReaderTest::HashTableReaderTest() {}

HashTableReaderTest::~HashTableReaderTest() {}

void HashTableReaderTest::CaseSetUp() {}

void HashTableReaderTest::CaseTearDown() {}

ACTION_P(THROW_EXCEPTION, needThrowException)
{
    if (needThrowException) {
        INDEXLIB_THROW(indexlib::util::IndexCollapsedException, "Mock Throw");
    }
    return NULL;
}

TEST_F(HashTableReaderTest, TestException)
{
    indexlib::file_system::FileSystemOptions fsOptions;
    std::string rootPath = GET_TEMP_DATA_PATH();
    auto fs = indexlib::file_system::FileSystemCreator::Create("ut", rootPath, fsOptions).GetOrThrow();
    auto rootDir = indexlib::file_system::IDirectory::Get(fs);

    // fileLength is smaller than sizeof bucketCount + nodeCount
    FileWriterPtr fileWriter =
        rootDir->CreateFileWriter("hash_table", indexlib::file_system::WriterOption()).GetOrThrow();
    uint32_t data = 1;
    fileWriter->Write(&data, sizeof(data)).GetOrThrow();
    fileWriter->Close().GetOrThrow();
    HashTableReader<uint64_t, uint64_t> hashTableReader;
    ASSERT_ANY_THROW(hashTableReader.Open(rootDir, "hash_table"));

    // fileLength is different from calculated expect length
    fileWriter = rootDir->CreateFileWriter("hash_table2", indexlib::file_system::WriterOption()).GetOrThrow();
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
    ASSERT_ANY_THROW(hashTableReader3.Open(mockDir, "hash_table3"));
}

TEST_F(HashTableReaderTest, TestSimpleProcess)
{
    indexlib::file_system::FileSystemOptions fsOptions;
    std::string rootPath = GET_TEMP_DATA_PATH();
    auto fs = indexlib::file_system::FileSystemCreator::Create("ut", rootPath, fsOptions).GetOrThrow();
    auto rootDir = indexlib::file_system::IDirectory::Get(fs);

    HashTableWriter<uint64_t, uint64_t> hashTableWriter;
    const size_t bucketCount = 100;
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
} // namespace indexlibv2::index
