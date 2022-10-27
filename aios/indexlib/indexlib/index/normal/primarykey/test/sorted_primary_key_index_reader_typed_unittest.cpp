#include "indexlib/index/normal/primarykey/test/sorted_primary_key_index_reader_typed_unittest.h"
#include "indexlib/file_system/buffered_file_reader.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);

INDEXLIB_UNIT_TEST_CASE(SortedPrimaryKeyIndexReaderTypedTest, TestCaseForOpenUInt64);
INDEXLIB_UNIT_TEST_CASE(SortedPrimaryKeyIndexReaderTypedTest, TestCaseForOpenWithoutPKAttributeUInt64);
INDEXLIB_UNIT_TEST_CASE(SortedPrimaryKeyIndexReaderTypedTest, TestCaseForOpenFailedUInt64);
INDEXLIB_UNIT_TEST_CASE(SortedPrimaryKeyIndexReaderTypedTest, TestCaseForOpenUInt128);
INDEXLIB_UNIT_TEST_CASE(SortedPrimaryKeyIndexReaderTypedTest, TestCaseForOpenWithoutPKAttributeUInt128);
INDEXLIB_UNIT_TEST_CASE(SortedPrimaryKeyIndexReaderTypedTest, TestCaseForOpenFailedUInt128);
INDEXLIB_UNIT_TEST_CASE(SortedPrimaryKeyIndexReaderTypedTest, TestCaseForSortedPKWrite);
INDEXLIB_UNIT_TEST_CASE(SortedPrimaryKeyIndexReaderTypedTest, TestCaseForSortedPKRead);

IndexConfigPtr SortedPrimaryKeyIndexReaderTypedTest::CreateIndexConfig(
        const uint64_t key, const string& indexName, 
        bool hasPKAttribute)
{
    return PrimaryKeyTestCaseHelper::CreateIndexConfig<uint64_t>("pk", pk_sort_array,
            hasPKAttribute);
}

IndexConfigPtr SortedPrimaryKeyIndexReaderTypedTest::CreateIndexConfig(
        const uint128_t key, const string& indexName,
        bool hasPKAttribute)
{
    return PrimaryKeyTestCaseHelper::CreateIndexConfig<uint128_t>("pk",
            pk_sort_array, hasPKAttribute);
}

void SortedPrimaryKeyIndexReaderTypedTest::TestCaseForSortedPKWrite()
{
    uint32_t pks[] = {1, 600, 2, 40, 7};
    uint32_t pksPerSegment[] = {5};
    segmentid_t segmentIds[] = {0};

    mSchema = PrimaryKeyTestCaseHelper::CreatePrimaryKeySchema<uint64_t>();
    mIndexConfig =  CreateIndexConfig((uint64_t)0, "pk", false);
    Version version = MakeOneVersion<uint64_t>(pks, pksPerSegment, segmentIds, 1, 0);

    string fileName = mRootDir + "segment_0_level_0/" + "index" + "/pk/" + PRIMARY_KEY_DATA_FILE_NAME;
    BufferedFileReaderPtr fileReader(new BufferedFileReader());
    fileReader->Open(fileName);
    size_t expectFileLength = pksPerSegment[0] * sizeof(PKPair<uint64_t>);
    INDEXLIB_TEST_EQUAL(expectFileLength, (size_t)fileReader->GetLength());
    PKPair<uint64_t> pkData[expectFileLength];
    INDEXLIB_TEST_EQUAL(expectFileLength, (size_t)fileReader->Read((void*)pkData,
                    expectFileLength));
    for (size_t i = 1; i < pksPerSegment[0]; ++i)
    {
        INDEXLIB_TEST_TRUE(pkData[i].key > pkData[i - 1].key);
    }
}

void SortedPrimaryKeyIndexReaderTypedTest::TestCaseForSortedPKRead()
{
    uint32_t pks[] = {1, 600, 2, 40, 7};
    uint32_t pksPerSegment[] = {5};
    segmentid_t segmentIds[] = {0};

    mSchema = PrimaryKeyTestCaseHelper::CreatePrimaryKeySchema<uint64_t>();
    mIndexConfig =  CreateIndexConfig((uint64_t)0, "pk", false);
    Version version = MakeOneVersion<uint64_t>(pks, pksPerSegment, segmentIds, 1, 0);

    IndexPartitionOptions options;
    options.SetIsOnline(false);
    index_base::PartitionDataPtr partitionData = 
        test::PartitionDataMaker::CreatePartitionData(
                GET_FILE_SYSTEM(), options, mSchema);

    FakePrimaryKeyIndexReaderTyped<uint64_t> reader;
    IndexConfigPtr indexConfig = CreateIndexConfig((uint64_t)0, "pk", false);
    ASSERT_NO_THROW(reader.OpenWithoutPKAttribute(indexConfig, partitionData));
}

IE_NAMESPACE_END(index);
