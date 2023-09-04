#include "indexlib/index/common/hash_table/HashTableWriter.h"

#include "autil/Log.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;
using namespace indexlib::file_system;

namespace indexlibv2::index {

class HashTableWriterTest : public indexlib::INDEXLIB_TESTBASE
{
public:
    HashTableWriterTest();
    ~HashTableWriterTest();

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(indexlib.index, HashTableWriterTest);

HashTableWriterTest::HashTableWriterTest() {}

HashTableWriterTest::~HashTableWriterTest() {}

void HashTableWriterTest::CaseSetUp() {}

void HashTableWriterTest::CaseTearDown() {}

TEST_F(HashTableWriterTest, TestSimpleProcess)
{
    indexlib::file_system::FileSystemOptions fsOptions;
    std::string rootPath = GET_TEMP_DATA_PATH();
    auto fs = indexlib::file_system::FileSystemCreator::Create("ut", rootPath, fsOptions).GetOrThrow();
    auto rootDir = indexlib::file_system::IDirectory::Get(fs);

    HashTableWriter<uint64_t, uint64_t> hashTableWriter;
    const size_t bucketCount = 100;
    hashTableWriter.Init(rootDir, "hash_table", bucketCount);
    FileWriterPtr fileWriter = hashTableWriter.GetFileWriter();
    ASSERT_EQ(indexlib::util::PathUtil::JoinPath(rootDir->GetLogicalPath(), "hash_table"),
              fileWriter->GetLogicalPath());
    for (size_t i = 0; i < 200; ++i) {
        hashTableWriter.Add(i, i);
    }
    hashTableWriter.Close();
    size_t expectSize = bucketCount * sizeof(uint32_t) + 200 * (16 + sizeof(uint32_t)) + sizeof(uint32_t) * 2;
    ASSERT_EQ(expectSize, rootDir->GetFileLength("hash_table").GetOrThrow());
}
} // namespace indexlibv2::index
