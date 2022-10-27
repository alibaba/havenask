#include "indexlib/index/normal/primarykey/test/hash_primary_key_formatter_unittest.h"
#include "indexlib/index/normal/primarykey/primary_key_loader.h"
#include "indexlib/test/single_field_partition_data_provider.h"
#include "indexlib/config/single_field_index_config.h"
#include "indexlib/config/impl/single_field_index_config_impl.h"

using namespace std;
using std::tr1::shared_ptr;
using namespace autil;
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, HashPrimaryKeyFormatterTest);

HashPrimaryKeyFormatterTest::HashPrimaryKeyFormatterTest()
{
}

HashPrimaryKeyFormatterTest::~HashPrimaryKeyFormatterTest()
{
}

void HashPrimaryKeyFormatterTest::CaseSetUp()
{
    mRootDirectory = GET_PARTITION_DIRECTORY();
}

void HashPrimaryKeyFormatterTest::CaseTearDown()
{
}

void HashPrimaryKeyFormatterTest::TestDeserializeToSliceFile()
{
    InnerTestDeserializeToSliceFile("1", 1, 1, "1", "0");
    InnerTestDeserializeToSliceFile("1", 1, 1, "1,2", "0,-1");
    InnerTestDeserializeToSliceFile("1,2,1", 2, 3, "1,2,3", "2,1,-1");

    //check multi segment merge, 8=1+4+1+2, 11=2+5+1+3
    InnerTestDeserializeToSliceFile("1,1#2,1,3,2,4#2#3,3,4",
                                    8, 11, "1,2,3,4", "3,7,9,10");

    //check multi segment merge with empty segment 
    InnerTestDeserializeToSliceFile("1,1##2,1,3,2,4##2#3,3,4",
                                    8, 11, "1,2,3,4", "3,7,9,10");
}

void HashPrimaryKeyFormatterTest::TestCalculatePkSliceFileLen()
{
    HashPrimaryKeyFormatter<uint64_t> pk64Formatter;
    HashPrimaryKeyFormatter<uint128_t> pk128Formatter;
    //pkCount = 3 ==> bucketCount = 7;
    //fileLen = header(24) + bucketCount * sizeof(docid) + docCount * sizeof(PKPair)
    ASSERT_EQ((size_t)1252, pk64Formatter.CalculatePkSliceFileLen(3, 100));
    ASSERT_EQ((size_t)2052, pk128Formatter.CalculatePkSliceFileLen(3, 100));
    ASSERT_EQ((size_t)88, pk64Formatter.CalculatePkSliceFileLen(3, 3));
    ASSERT_EQ((size_t)112, pk128Formatter.CalculatePkSliceFileLen(3, 3));
}

void HashPrimaryKeyFormatterTest::TestSerializeFromHashMap()
{
    HashPrimaryKeyFormatter<uint64_t>::HashMapTypePtr hashMap(
            new HashPrimaryKeyFormatter<uint64_t>::HashMapType(100));
    hashMap->FindAndInsert(1,1);
    hashMap->FindAndInsert(2,2);

    size_t serializeBufSize =  2 * sizeof(HashPrimaryKeyFormatter<uint64_t>::PKPair);
    SliceFilePtr sliceFile = 
        GET_PARTITION_DIRECTORY()->CreateSliceFile("tmp", serializeBufSize, 1);
    HashPrimaryKeyFormatter<uint64_t>::ReserveSliceFile(sliceFile, serializeBufSize);
    SliceFileReaderPtr fileReader = sliceFile->CreateSliceFileReader();
    char* buffer = (char*)fileReader->GetBaseAddress();
    
    HashPrimaryKeyFormatter<uint64_t> formatter;
    formatter.SerializeToBuffer(buffer, serializeBufSize, hashMap);

    ASSERT_EQ((docid_t)1, formatter.Find(buffer, 1));
    ASSERT_EQ((docid_t)2, formatter.Find(buffer, 2));
}

void HashPrimaryKeyFormatterTest::InnerTestDeserializeToSliceFile(
        const string& docString, 
        const size_t pkCount, const size_t docCount,
        const string& pkString, const string& docidStr)
{
    InnerTestDeserializeFromPkIndexType(pk_sort_array, true, docString,
                                        pkCount, docCount, pkString, docidStr);
    InnerTestDeserializeFromPkIndexType(pk_sort_array, false, docString,
                                        pkCount, docCount, pkString, docidStr);
    InnerTestDeserializeFromPkIndexType(pk_hash_table, true, docString,
                                        pkCount, docCount, pkString, docidStr);
    InnerTestDeserializeFromPkIndexType(pk_hash_table, false, docString,
                                        pkCount, docCount, pkString, docidStr);
}

void HashPrimaryKeyFormatterTest::InnerTestDeserializeFromPkIndexType(
        PrimaryKeyIndexType pkIndexType, bool isUint128, const string& docString, 
        const size_t pkCount, const size_t docCount,
        const string& pkString, const string& docidStr)
{
    TearDown();
    SetUp();
    SingleFieldPartitionDataProvider provider;
    if (isUint128)
    {
        provider.Init(GET_TEST_DATA_PATH(), "uint64", SFP_PK_128_INDEX);
    }
    else
    {
        provider.Init(GET_TEST_DATA_PATH(), "uint64", SFP_PK_INDEX);
    }
    
    //set pk index sorted or unsorted
    PrimaryKeyIndexConfigPtr pkConfig = DYNAMIC_POINTER_CAST(
            PrimaryKeyIndexConfig, provider.GetIndexConfig());
    pkConfig->SetPrimaryKeyIndexType(pkIndexType);
    provider.Build(docString, SFP_OFFLINE);

    //prepare slice file
    PartitionDataPtr partitionData = provider.GetPartitionData();
    
    file_system::SliceFilePtr sliceFile;
    size_t sliceFileLen = 0;
    if (isUint128)
    {
        tr1::shared_ptr<PrimaryKeyIterator<uint128_t> > pkIterator =
            CreatePkIterator<uint128_t>(pkConfig, partitionData);
        HashPrimaryKeyFormatter<uint128_t> pkFormatter;
        sliceFileLen = pkFormatter.CalculatePkSliceFileLen(pkCount, docCount);
        sliceFile = mRootDirectory->CreateSliceFile(
                PRIMARY_KEY_DATA_SLICE_FILE_NAME, sliceFileLen, 1);
        pkFormatter.DeserializeToSliceFile(pkIterator, sliceFile, sliceFileLen);
        CheckSliceFile<uint128_t>(sliceFile, pkString, docidStr);
    }
    else
    {
        tr1::shared_ptr<PrimaryKeyIterator<uint64_t> > pkIterator =
            CreatePkIterator<uint64_t>(pkConfig, partitionData);
        HashPrimaryKeyFormatter<uint64_t> pkFormatter;
        sliceFileLen = pkFormatter.CalculatePkSliceFileLen(pkCount, docCount);
        sliceFile = mRootDirectory->CreateSliceFile(
                PRIMARY_KEY_DATA_SLICE_FILE_NAME, sliceFileLen, 1);
        pkFormatter.DeserializeToSliceFile(pkIterator, sliceFile, sliceFileLen);
        CheckSliceFile<uint64_t>(sliceFile, pkString, docidStr);
    }
    SliceFileReaderPtr sliceFileReader = sliceFile->CreateSliceFileReader();
    ASSERT_EQ(sliceFileLen, sliceFileReader->GetLength());
}
IE_NAMESPACE_END(index);

