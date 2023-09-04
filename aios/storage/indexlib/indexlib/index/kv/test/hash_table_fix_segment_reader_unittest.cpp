#include "indexlib/index/kv/test/hash_table_fix_segment_reader_unittest.h"

#include "future_lite/CoroInterface.h"
#include "future_lite/executors/SimpleExecutor.h"
#include "indexlib/common/hash_table/dense_hash_table_traits.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/schema_maker.h"

using namespace std;

using namespace indexlib::test;
using namespace indexlib::config;
using namespace indexlib::partition;
using namespace indexlib::index_base;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, HashTableFixSegmentReaderTest);

HashTableFixSegmentReaderTest::HashTableFixSegmentReaderTest() {}

HashTableFixSegmentReaderTest::~HashTableFixSegmentReaderTest() {}

void HashTableFixSegmentReaderTest::CaseSetUp()
{
    string field = "key:int32;value:uint64;";
    mSchema = SchemaMaker::MakeKVSchema(field, "key", "value");
    mDocStr = "cmd=add,key=1,value=1,ts=10000000;"
              "cmd=delete,key=2,ts=30000000;";
    mKVConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, mSchema->GetIndexSchema()->GetPrimaryKeyIndexConfig());
    setenv("DISABLE_CODEGEN", "true", 1);
}

void HashTableFixSegmentReaderTest::CaseTearDown() { unsetenv("DISABLE_CODEGEN"); }

void HashTableFixSegmentReaderTest::TestValueWithTs()
{
    mSchema->SetEnableTTL(true);
    IndexPartitionOptions options;
    options.GetBuildConfig(false).buildTotalMemory = 40;
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, options, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, mDocStr, "", ""));
    mKVConfig->SetTTL(100);
    RESET_FILE_SYSTEM();
    OnDiskPartitionDataPtr partitionData = OnDiskPartitionData::CreateOnDiskPartitionData(GET_FILE_SYSTEM());
    HashTableFixSegmentReader reader;
    reader.Open(mKVConfig, *(partitionData->Begin()));
    autil::StringView value;
    uint64_t ts = 0;
    bool isDeleted = false;
    future_lite::executors::SimpleExecutor ex(1);
    KVIndexOptions indexOptions;
    ASSERT_TRUE(future_lite::interface::syncAwait(reader.Get(&indexOptions, 1, value, ts, isDeleted, NULL), &ex));
    ASSERT_EQ((uint64_t)1, *(uint64_t*)value.data());
    ASSERT_FALSE(isDeleted);
    ASSERT_EQ((uint64_t)10, ts);

    ASSERT_TRUE(future_lite::interface::syncAwait(reader.Get(&indexOptions, 2, value, ts, isDeleted, NULL), &ex));
    ASSERT_TRUE(isDeleted);
    ASSERT_EQ((uint64_t)30, ts);

    ASSERT_FALSE(future_lite::interface::syncAwait(reader.Get(&indexOptions, 3, value, ts, isDeleted, NULL), &ex));
}

void HashTableFixSegmentReaderTest::TestValueWithoutTs()
{
    future_lite::executors::SimpleExecutor ex(1);
    KVIndexOptions indexOptions;
    mSchema->SetEnableTTL(false);
    IndexPartitionOptions options;
    options.GetBuildConfig(false).buildTotalMemory = 40;
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, options, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, mDocStr, "", ""));
    RESET_FILE_SYSTEM();
    OnDiskPartitionDataPtr partitionData = OnDiskPartitionData::CreateOnDiskPartitionData(GET_FILE_SYSTEM());
    HashTableFixSegmentReader reader;
    reader.Open(mKVConfig, *(partitionData->Begin()));
    autil::StringView value;
    uint64_t ts = 0;
    bool isDeleted = false;
    ASSERT_TRUE(future_lite::interface::syncAwait(reader.Get(&indexOptions, 1, value, ts, isDeleted, NULL), &ex));
    ASSERT_EQ((uint64_t)1, *(uint64_t*)value.data());
    ASSERT_FALSE(isDeleted);
    ASSERT_EQ((uint64_t)0, ts);

    ASSERT_TRUE(future_lite::interface::syncAwait(reader.Get(&indexOptions, 2, value, ts, isDeleted, NULL), &ex));
    ASSERT_TRUE(isDeleted);
    ASSERT_EQ((uint64_t)0, ts);

    ASSERT_FALSE(future_lite::interface::syncAwait(reader.Get(&indexOptions, 3, value, ts, isDeleted, NULL), &ex));
}
}} // namespace indexlib::index
