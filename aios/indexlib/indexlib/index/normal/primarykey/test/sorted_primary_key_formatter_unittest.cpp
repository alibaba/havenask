#include "indexlib/index/normal/primarykey/test/sorted_primary_key_formatter_unittest.h"
#include "indexlib/util/hash_string.h"
#include <autil/StringUtil.h>
#include "indexlib/test/single_field_partition_data_provider.h"
#include "indexlib/config/single_field_index_config.h"
#include "indexlib/config/impl/single_field_index_config_impl.h"
#include "indexlib/index/normal/primarykey/primary_key_iterator_creator.h"
#include "indexlib/index_base/segment/segment_directory.h"

using namespace std;
using std::tr1::shared_ptr;
using namespace autil;
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, SortedPrimaryKeyFormatterTest);

SortedPrimaryKeyFormatterTest::SortedPrimaryKeyFormatterTest()
{
}

SortedPrimaryKeyFormatterTest::~SortedPrimaryKeyFormatterTest()
{
}

void SortedPrimaryKeyFormatterTest::CaseSetUp()
{
}

void SortedPrimaryKeyFormatterTest::CaseTearDown()
{
}

void SortedPrimaryKeyFormatterTest::TestFind()
{
    typedef SortedPrimaryKeyFormatter<uint64_t>::PKPair PK64Pair;
    PK64Pair data64[] = { {1, 11}, {2, 22}};
    ASSERT_EQ((docid_t)INVALID_DOCID, SortedPrimaryKeyFormatter<uint64_t>::Find(data64, 0, 1));
    ASSERT_EQ((docid_t)11, SortedPrimaryKeyFormatter<uint64_t>::Find(data64, 1, 1));
    ASSERT_EQ((docid_t)INVALID_DOCID, SortedPrimaryKeyFormatter<uint64_t>::Find(data64, 1, 2));
    ASSERT_EQ((docid_t)INVALID_DOCID, SortedPrimaryKeyFormatter<uint64_t>::Find(data64, 2, 0));
    ASSERT_EQ((docid_t)11, SortedPrimaryKeyFormatter<uint64_t>::Find(data64, 2, 1));
    ASSERT_EQ((docid_t)22, SortedPrimaryKeyFormatter<uint64_t>::Find(data64, 2, 2));
    ASSERT_EQ((docid_t)INVALID_DOCID, SortedPrimaryKeyFormatter<uint64_t>::Find(data64, 2, 3));

    typedef SortedPrimaryKeyFormatter<uint128_t>::PKPair PK128Pair;
    PK128Pair data128[] = { {1, 11}, {2, 22} };
    ASSERT_EQ((docid_t)INVALID_DOCID, SortedPrimaryKeyFormatter<uint128_t>::Find(data128, 0, 1));
    ASSERT_EQ((docid_t)11, SortedPrimaryKeyFormatter<uint128_t>::Find(data128, 1, 1));
    ASSERT_EQ((docid_t)INVALID_DOCID, SortedPrimaryKeyFormatter<uint128_t>::Find(data128, 1, 2));
    ASSERT_EQ((docid_t)INVALID_DOCID, SortedPrimaryKeyFormatter<uint128_t>::Find(data128, 2, 0));
    ASSERT_EQ((docid_t)11, SortedPrimaryKeyFormatter<uint128_t>::Find(data128, 2, 1));
    ASSERT_EQ((docid_t)22, SortedPrimaryKeyFormatter<uint128_t>::Find(data128, 2, 2));
    ASSERT_EQ((docid_t)INVALID_DOCID, SortedPrimaryKeyFormatter<uint128_t>::Find(data128, 2, 3));
}

void SortedPrimaryKeyFormatterTest::TestSerializeFromHashMap()
{
    SortedPrimaryKeyFormatter<uint64_t>::HashMapTypePtr hashMap(
            new SortedPrimaryKeyFormatter<uint64_t>::HashMapType(100));
    hashMap->FindAndInsert(1,1);
    hashMap->FindAndInsert(3,2);
    hashMap->FindAndInsert(2,3);
    SortedPrimaryKeyFormatter<uint64_t>::PKPair buffer[3];
    SortedPrimaryKeyFormatter<uint64_t> formatter;
    formatter.SerializeToBuffer((char*)buffer, sizeof(buffer), hashMap);
    ASSERT_EQ((docid_t)1, buffer[0].docid);
    ASSERT_EQ((docid_t)3, buffer[1].docid);
    ASSERT_EQ((docid_t)2, buffer[2].docid);
}

void SortedPrimaryKeyFormatterTest::TestDeserializeToSliceFile()
{
    SingleFieldPartitionDataProvider provider;
    provider.Init(GET_TEST_DATA_PATH(), "uint64", SFP_PK_INDEX);
    
    //set pk index sorted or unsorted
    PrimaryKeyIndexConfigPtr pkConfig = DYNAMIC_POINTER_CAST(
            PrimaryKeyIndexConfig, provider.GetIndexConfig());
    pkConfig->SetPrimaryKeyIndexType(pk_sort_array);

    provider.Build("1,2,3,4,5,1,3,5", SFP_OFFLINE);

    //prepare slice file
    PartitionDataPtr partitionData = provider.GetPartitionData();
    DirectoryPtr directory = partitionData->GetRootDirectory();
    DirectoryPtr pkDirectory = directory->GetDirectory("segment_0_level_0/index/pk_index/", true);
    SortedPrimaryKeyFormatter<uint64_t> pkFormatter;
    size_t sliceFileLen = pkFormatter.CalculatePkSliceFileLen(5,8);
    file_system::SliceFilePtr sliceFile 
        = pkDirectory->CreateSliceFile(
                PRIMARY_KEY_DATA_SLICE_FILE_NAME, sliceFileLen, 1);
    
    tr1::shared_ptr<PrimaryKeyIterator<uint64_t> > pkIterator =
        PrimaryKeyIteratorCreator::Create<uint64_t>(pkConfig);

    const vector<SegmentData>& segmentDatas =
        partitionData->GetSegmentDirectory()->GetSegmentDatas();

    assert(segmentDatas.size() == 1);
    pkIterator->Init(segmentDatas);
    
    pkFormatter.DeserializeToSliceFile(
            pkIterator, sliceFile, sliceFileLen);
    CheckSliceFile(sliceFile, "1,2,3,4,5", "5,1,6,3,7");
    SliceFileReaderPtr sliceFileReader = sliceFile->CreateSliceFileReader();
    ASSERT_EQ(sliceFileLen, sliceFileReader->GetLength());
}

void SortedPrimaryKeyFormatterTest::CheckSliceFile(
        const file_system::SliceFilePtr& sliceFile,
        const std::string& docs, const std::string& expectDocids)
{

    SliceFileReaderPtr sliceFileReader = sliceFile->CreateSliceFileReader();
    SortedPrimaryKeyFormatter<uint64_t>::PKPair* baseAddress = 
        (SortedPrimaryKeyFormatter<uint64_t>::PKPair*)sliceFileReader->GetBaseAddress();
    
    vector<string> docsVec;
    StringUtil::fromString(docs, docsVec, ",");

    vector<docid_t> expectDocidVec;
    StringUtil::fromString(expectDocids, expectDocidVec, ",");

    ASSERT_EQ(docsVec.size(), expectDocidVec.size());
    util::HashString hashFunc;
    for (size_t i = 0; i < docsVec.size(); i++)
    {
        uint64_t hashKey;
        hashFunc.Hash(hashKey, docsVec[i].c_str(), docsVec[i].size());
        docid_t docid = SortedPrimaryKeyFormatter<uint64_t>::Find(
                baseAddress, docsVec.size(), hashKey);
        ASSERT_EQ(expectDocidVec[i], docid);
    }
}

IE_NAMESPACE_END(index);

