#include "indexlib/common/hash_table/test/hash_table_writer_unittest.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace indexlib::file_system;

namespace indexlib { namespace common {
IE_LOG_SETUP(common, HashTableWriterTest);

HashTableWriterTest::HashTableWriterTest() {}

HashTableWriterTest::~HashTableWriterTest() {}

void HashTableWriterTest::CaseSetUp() {}

void HashTableWriterTest::CaseTearDown() {}

void HashTableWriterTest::TestSimpleProcess()
{
    HashTableWriter<uint64_t, uint64_t> hashTableWriter;
    const size_t bucketCount = 100;
    DirectoryPtr rootDir = GET_PARTITION_DIRECTORY();
    hashTableWriter.Init(rootDir, "hash_table", bucketCount);
    FileWriterPtr fileWriter = hashTableWriter.GetFileWriter();
    ASSERT_EQ(util::PathUtil::JoinPath(rootDir->GetLogicalPath(), "hash_table"), fileWriter->GetLogicalPath());
    for (size_t i = 0; i < 200; ++i) {
        hashTableWriter.Add(i, i);
    }
    hashTableWriter.Close();
    size_t expectSize = bucketCount * sizeof(uint32_t) + 200 * (16 + sizeof(uint32_t)) + sizeof(uint32_t) * 2;
    ASSERT_EQ(expectSize, rootDir->GetFileLength("hash_table"));
}
}} // namespace indexlib::common
