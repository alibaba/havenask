#include "indexlib/index/kkv/test/on_disk_separate_chain_hash_iterator_unittest.h"

#include "indexlib/common/hash_table/hash_table_writer.h"
#include "indexlib/config/test/schema_maker.h"
#include "indexlib/index_define.h"

using namespace std;
using namespace indexlib::test;
using namespace indexlib::util;
using namespace indexlib::config;
using namespace indexlib::index_base;
using namespace indexlib::common;
using namespace indexlib::file_system;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, OnDiskSeparateChainHashIteratorTest);

OnDiskSeparateChainHashIteratorTest::OnDiskSeparateChainHashIteratorTest() {}

OnDiskSeparateChainHashIteratorTest::~OnDiskSeparateChainHashIteratorTest() {}

void OnDiskSeparateChainHashIteratorTest::CaseSetUp()
{
    string fields = "single_int8:int8;single_int16:int16;pkey:uint64;skey:uint64";
    mSchema = SchemaMaker::MakeKKVSchema(fields, "pkey", "skey", "single_int8;single_int16;skey");

    const IndexSchemaPtr& indexSchema = mSchema->GetIndexSchema();
    mKKVIndexConfig = DYNAMIC_POINTER_CAST(KKVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
}

void OnDiskSeparateChainHashIteratorTest::CaseTearDown() {}

void OnDiskSeparateChainHashIteratorTest::TestSimpleProcess()
{
    size_t count = 10;
    PrepareData(count);
    CheckData(count);
}

void OnDiskSeparateChainHashIteratorTest::PrepareData(size_t count)
{
    DirectoryPtr segmentDir = GET_SEGMENT_DIRECTORY();
    DirectoryPtr dataDir = segmentDir->MakeDirectory("index/pkey", DirectoryOption::Mem());

    HashTableWriter<uint64_t, OnDiskPKeyOffset> writer;
    writer.Init(dataDir, PREFIX_KEY_FILE_NAME, count * 3);
    for (size_t i = 0; i < count; ++i) {
        uint64_t key = i;
        OnDiskPKeyOffset offset(i, i);
        writer.Add(key, offset);
    }
    writer.Close();
    segmentDir->Sync(true);
}

void OnDiskSeparateChainHashIteratorTest::CheckData(size_t count)
{
    SegmentData segData;
    segData.SetDirectory(GET_SEGMENT_DIRECTORY());

    OnDiskSeparateChainHashIterator iter;
    iter.Open(mKKVIndexConfig, segData);

    size_t totalCount = 0;
    while (iter.IsValid()) {
        uint64_t expectKey = totalCount;
        OnDiskPKeyOffset expectOffset(totalCount, totalCount);
        ASSERT_EQ(expectKey, iter.Key());
        ASSERT_EQ(expectOffset, iter.Value());
        iter.MoveToNext();
        ++totalCount;
    }

    ASSERT_EQ(count, totalCount);
    ASSERT_EQ(count, iter.Size());
}
}} // namespace indexlib::index
