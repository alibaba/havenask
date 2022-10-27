#include <autil/StringUtil.h>
#include "indexlib/index/normal/primarykey/test/sorted_primary_key_pair_segment_iterator_unittest.h"
#include "indexlib/test/single_field_partition_data_provider.h"
#include "indexlib/config/single_field_index_config.h"
#include "indexlib/config/impl/single_field_index_config_impl.h"
#include "indexlib/util/hash_string.h"
#include "indexlib/index/normal/primarykey/primary_key_index_reader_typed.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, SortedPrimaryKeyPairSegmentIteratorTest);

bool compFun(const std::pair<uint64_t, docid_t>& left,
             const std::pair<uint64_t, docid_t>& right)
{
    if (left.first == right.first)
    {
        return left.second < right.second;
    }
    return left.first < right.first;
}

SortedPrimaryKeyPairSegmentIteratorTest::SortedPrimaryKeyPairSegmentIteratorTest()
{
}

SortedPrimaryKeyPairSegmentIteratorTest::~SortedPrimaryKeyPairSegmentIteratorTest()
{
}

void SortedPrimaryKeyPairSegmentIteratorTest::CaseSetUp()
{
}

void SortedPrimaryKeyPairSegmentIteratorTest::CaseTearDown()
{
}

void SortedPrimaryKeyPairSegmentIteratorTest::TestSimpleProcess()
{
    InnerTestIterator(FSOT_IN_MEM, "1,2,5", "1,0;2,1;5,2");
    InnerTestIterator(FSOT_BUFFERED, "3,7,8,9", "3,0;7,1;8,2;9,3");
}

void SortedPrimaryKeyPairSegmentIteratorTest::InnerTestIterator(
        FSOpenType fsOpenType, const string& docsStr, 
        const string& expectResultStr)
{
    TearDown();
    SetUp();

    SingleFieldPartitionDataProvider provider;
    provider.Init(GET_TEST_DATA_PATH(), "uint64", SFP_PK_INDEX);
    PrimaryKeyIndexConfigPtr config = DYNAMIC_POINTER_CAST(
            PrimaryKeyIndexConfig, provider.GetIndexConfig());
    assert(config);
    config->SetPrimaryKeyIndexType(pk_sort_array);
    provider.Build(docsStr, SFP_OFFLINE);

    DirectoryPtr segDirectory = 
        GET_PARTITION_DIRECTORY()->GetDirectory("segment_0_level_0", true);
    ASSERT_TRUE(segDirectory);

    segDirectory->MountPackageFile(PACKAGE_FILE_PREFIX);
    FileReaderPtr pkFileReader = segDirectory->CreateFileReader(
            "index/pk_index/data", fsOpenType);

    SortedPrimaryKeyPairSegmentIterator<uint64_t> iter;
    iter.Init(pkFileReader);

    vector<vector<string> > pairVector;
    StringUtil::fromString(expectResultStr, pairVector, ",", ";");

    util::HashString hashFunc;
    std::vector<std::pair<uint64_t, docid_t> > expectResults;
    for (size_t i = 0; i < pairVector.size(); i++)
    {
        uint64_t hashKey;
        hashFunc.Hash(hashKey, pairVector[i][0].c_str(), pairVector[i][0].size());
        docid_t docid = INVALID_DOCID;
        ASSERT_TRUE(autil::StringUtil::fromString(pairVector[i][1], docid));
        expectResults.push_back(std::make_pair(hashKey, docid));
        docid++;
    }
    sort(expectResults.begin(), expectResults.end(), compFun);

    size_t cursor = 0;
    while(iter.HasNext())
    {
        PrimaryKeyPairSegmentIterator<uint64_t>::PKPair pkPair;
        iter.Next(pkPair);

        ASSERT_EQ(expectResults[cursor].first, pkPair.key);
        ASSERT_EQ(expectResults[cursor].second, pkPair.docid);
        cursor++;
    }
    ASSERT_EQ(cursor, expectResults.size());
}

IE_NAMESPACE_END(index);

