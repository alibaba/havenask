#include "indexlib/index/normal/primarykey/test/primary_key_pair_segment_iterator_unittest.h"
#include "indexlib/index/normal/primarykey/sorted_primary_key_pair_segment_iterator.h"
#include "indexlib/index/normal/primarykey/primary_key_pair.h"
#include "indexlib/test/single_field_partition_data_provider.h"
#include "indexlib/index/normal/primarykey/primary_key_index_reader_typed.h"

using namespace std;
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, PrimaryKeyPairSegmentIteratorTest);

PrimaryKeyPairSegmentIteratorTest::PrimaryKeyPairSegmentIteratorTest()
{
}

PrimaryKeyPairSegmentIteratorTest::~PrimaryKeyPairSegmentIteratorTest()
{
}

void PrimaryKeyPairSegmentIteratorTest::CaseSetUp()
{
}

void PrimaryKeyPairSegmentIteratorTest::CaseTearDown()
{
}

void PrimaryKeyPairSegmentIteratorTest::TestSortedPKPairIterator()
{
    SingleFieldPartitionDataProvider provider;
    provider.Init(GET_TEST_DATA_PATH(), "uint64", SFP_PK_INDEX);
    PrimaryKeyIndexConfigPtr config = DYNAMIC_POINTER_CAST(
            PrimaryKeyIndexConfig, provider.GetIndexConfig());
    assert(config);
    config->SetPrimaryKeyIndexType(pk_sort_array);

    string docsStr = "1,3,2";
    provider.Build(docsStr, SFP_OFFLINE);

    PartitionDataPtr partData = provider.GetPartitionData();
    PartitionData::Iterator iter = partData->Begin();
    const DirectoryPtr& dir = iter->GetIndexDirectory(true);
    DirectoryPtr pkDir = dir->GetDirectory(config->GetIndexName(), true);
    FileReaderPtr fileReader = pkDir->CreateFileReader(PRIMARY_KEY_DATA_FILE_NAME, FSOT_BUFFERED);

    PrimaryKeyPairSegmentIteratorPtr pkIter;
    pkIter.reset(new SortedPrimaryKeyPairSegmentIterator<uint64_t>());
    pkIter->Init(fileReader);
    CheckIterator<uint64_t>(pkIter, "1,0;2,2;3,1");
}

IE_NAMESPACE_END(index);

