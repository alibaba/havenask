#include <autil/LongHashValue.h>
#include "indexlib/index/normal/primarykey/test/on_disk_ordered_primary_key_iterator_unittest.h"
#include "indexlib/index/normal/primarykey/primary_key_index_reader_typed.h"
#include "indexlib/test/single_field_partition_data_provider.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, OnDiskOrderedPrimaryKeyIteratorTest);

OnDiskOrderedPrimaryKeyIteratorTest::OnDiskOrderedPrimaryKeyIteratorTest()
{
}

OnDiskOrderedPrimaryKeyIteratorTest::~OnDiskOrderedPrimaryKeyIteratorTest()
{
}

void OnDiskOrderedPrimaryKeyIteratorTest::CaseSetUp()
{
}

void OnDiskOrderedPrimaryKeyIteratorTest::CaseTearDown()
{
}

void OnDiskOrderedPrimaryKeyIteratorTest::TestUint64SortedPkIterator()
{
    DoTestUint64PkIterator(pk_sort_array);
}
void OnDiskOrderedPrimaryKeyIteratorTest::TestUint128SortedPkIterator()
{
    DoTestUint128PkIterator(pk_sort_array);
}
void OnDiskOrderedPrimaryKeyIteratorTest::DoTestUint64PkIterator(PrimaryKeyIndexType type)
{
    //check delete docs can also get out
    //check empty segment no effect
    SingleFieldPartitionDataProvider provider;
    provider.Init(GET_TEST_DATA_PATH(), "uint64", SFP_PK_INDEX);
    PrimaryKeyIndexConfigPtr config = DYNAMIC_POINTER_CAST(
            PrimaryKeyIndexConfig, provider.GetIndexConfig());
    assert(config);
    config->SetPrimaryKeyIndexType(type);
    string docsStr = "1,2,5#3,4,9#5,7,3#2,6,7#delete:1,delete:5#10,11"; 
    provider.Build(docsStr, SFP_OFFLINE);
    PrimaryKeyIndexReaderTyped<uint64_t> pkReader;
    pkReader.Open(provider.GetIndexConfig(), provider.GetPartitionData());
    OnDiskOrderedPrimaryKeyIterator<uint64_t> iter = 
        pkReader.CreateOnDiskOrderedIterator();
    CheckIterator<uint64_t>(iter, "1,0;2,1;5,2;3,3;4,4;9,5;5,6;7,7;"
                            "3,8;2,9;6,10;7,11;10,12;11,13");
}

void OnDiskOrderedPrimaryKeyIteratorTest::DoTestUint128PkIterator(PrimaryKeyIndexType type)
{
    SingleFieldPartitionDataProvider provider;
    provider.Init(GET_TEST_DATA_PATH(), "uint64", SFP_PK_128_INDEX);
    PrimaryKeyIndexConfigPtr config = DYNAMIC_POINTER_CAST(
            PrimaryKeyIndexConfig, provider.GetIndexConfig());
    assert(config);
    config->SetPrimaryKeyIndexType(type);
    string docsStr = "1,2,5#3,4,9#5,7,3#2,6,7#delete:1,delete:5#10,11"; 
    provider.Build(docsStr, SFP_OFFLINE);
    PrimaryKeyIndexReaderTyped<uint128_t> pkReader;
    pkReader.Open(provider.GetIndexConfig(), provider.GetPartitionData());
    OnDiskOrderedPrimaryKeyIterator<uint128_t> iter = 
        pkReader.CreateOnDiskOrderedIterator();
    CheckIterator<uint128_t>(iter, "1,0;2,1;5,2;3,3;4,4;9,5;5,6;7,7;"
                             "3,8;2,9;6,10;7,11;10,12;11,13");
}

void OnDiskOrderedPrimaryKeyIteratorTest::TestInitWithSortedPkSegmentDatas()
{
    DoTestInitWithSegmentDatas(pk_sort_array);
}

void OnDiskOrderedPrimaryKeyIteratorTest::DoTestInitWithSegmentDatas(PrimaryKeyIndexType type)
{
    SingleFieldPartitionDataProvider provider;
    provider.Init(GET_TEST_DATA_PATH(), "uint64", SFP_PK_INDEX);
    PrimaryKeyIndexConfigPtr config = DYNAMIC_POINTER_CAST(
            PrimaryKeyIndexConfig, provider.GetIndexConfig());
    assert(config);
    config->SetPrimaryKeyIndexType(type);
    string docsStr = "1,2,5#3,4,9#5,7,3#2,6,7#delete:1,delete:5#10,11"; 
    provider.Build(docsStr, SFP_OFFLINE);

    vector<SegmentData> segmentDatas;
    PartitionDataPtr partData = provider.GetPartitionData();

    for (PartitionData::Iterator iter = partData->Begin();
         iter != partData->End(); iter++)
    {
        segmentDatas.push_back(*iter);
    }

    OnDiskOrderedPrimaryKeyIterator<uint64_t> pkIter(provider.GetIndexConfig());
    pkIter.Init(segmentDatas);
    CheckIterator<uint64_t>(pkIter, "1,0;2,1;5,2;3,3;4,4;9,5;5,6;7,7;"
                            "3,8;2,9;6,10;7,11;10,12;11,13");
}

IE_NAMESPACE_END(index);

