#include "indexlib/index/kkv/test/on_disk_closed_hash_iterator_unittest.h"

#include "indexlib/index/kkv/kkv_define.h"
#include "indexlib/test/schema_maker.h"

using namespace std;
using namespace indexlib::test;
using namespace indexlib::util;
using namespace indexlib::config;
using namespace indexlib::index_base;
using namespace indexlib::file_system;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, OnDiskClosedHashIteratorTest);

OnDiskClosedHashIteratorTest::OnDiskClosedHashIteratorTest() {}

OnDiskClosedHashIteratorTest::~OnDiskClosedHashIteratorTest() {}

void OnDiskClosedHashIteratorTest::CaseSetUp()
{
    string fields = "single_int8:int8;single_int16:int16;pkey:uint64;skey:uint64";
    mSchema = SchemaMaker::MakeKKVSchema(fields, "pkey", "skey", "single_int8;single_int16;skey");

    const IndexSchemaPtr& indexSchema = mSchema->GetIndexSchema();
    mKKVIndexConfig = DYNAMIC_POINTER_CAST(KKVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
}

void OnDiskClosedHashIteratorTest::CaseTearDown() {}

void OnDiskClosedHashIteratorTest::TestDenseProcess()
{
    size_t count = 10;
    PrepareData<PKT_DENSE>(count);
    CheckData<PKT_DENSE>(count);
}

void OnDiskClosedHashIteratorTest::TestCuckooProcess()
{
    size_t count = 10;
    PrepareData<PKT_CUCKOO>(count);
    CheckData<PKT_CUCKOO>(count);
}

template <PKeyTableType Type>
void OnDiskClosedHashIteratorTest::PrepareData(size_t count)
{
    DirectoryPtr segmentDir = GET_SEGMENT_DIRECTORY();
    DirectoryPtr dataDir = segmentDir->MakeDirectory("index/pkey", DirectoryOption::Mem());

    typedef typename ClosedHashPrefixKeyTableTraits<uint64_t, Type>::Traits Traits;
    typedef typename Traits::HashTable ClosedHashTable;

    size_t memorySize = ClosedHashTable::DoCapacityToTableMemory(count, 50);
    vector<char> buffer;
    buffer.resize(memorySize);
    ClosedHashTable table;
    table.MountForWrite(buffer.data(), memorySize, 50);
    EXPECT_LE(count, table.Capacity());

    for (size_t i = 0; i < count; ++i) {
        uint64_t key = i;
        OnDiskPKeyOffset offset(i, i);
        table.Insert(key, offset.ToU64Value());
    }
    FileWriterPtr fileWriter = dataDir->CreateFileWriter(PREFIX_KEY_FILE_NAME);
    fileWriter->Write(buffer.data(), buffer.size()).GetOrThrow();
    ASSERT_EQ(FSEC_OK, fileWriter->Close());
    segmentDir->Sync(true);
}

template <PKeyTableType Type>
void OnDiskClosedHashIteratorTest::CheckData(size_t count)
{
    SegmentData segData;
    segData.SetDirectory(GET_SEGMENT_DIRECTORY());

    OnDiskClosedHashIterator<Type> iter;
    iter.Open(mKKVIndexConfig, segData);
    iter.SortByKey();
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
