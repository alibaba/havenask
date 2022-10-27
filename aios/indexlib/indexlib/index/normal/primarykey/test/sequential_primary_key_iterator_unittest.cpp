#include "indexlib/index/normal/primarykey/test/sequential_primary_key_iterator_unittest.h"
#include "indexlib/test/single_field_partition_data_provider.h"
#include "indexlib/index_base/segment/segment_directory.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, SequentialPrimaryKeyIteratorTest);

SequentialPrimaryKeyIteratorTest::SequentialPrimaryKeyIteratorTest()
{
}

SequentialPrimaryKeyIteratorTest::~SequentialPrimaryKeyIteratorTest()
{
}

void SequentialPrimaryKeyIteratorTest::CaseSetUp()
{
}

void SequentialPrimaryKeyIteratorTest::CaseTearDown()
{
}

void SequentialPrimaryKeyIteratorTest::TestIterate()
{
    // iterate with empty segments
    InnerTestIterator(pk_sort_array, "10,11,12,13#14,15,16##17,18,19",
                      "10:0,11:1,12:2,13:3,14:4,15:5,16:6,17:7,18:8,19:9");

    // iterate with duplicate pk
    InnerTestIterator(pk_sort_array, "10,11,13,13#14,15,15##17,18,19",
                      "10:0,11:1,13:3,14:4,15:6,17:7,18:8,19:9");

    // iterate with zero segments
    InnerTestIterator(pk_sort_array, "", "");
}

void SequentialPrimaryKeyIteratorTest::InnerTestIterator(
    PrimaryKeyIndexType pkIndexType, const string& docStr,
    const string& expectResults)
{
    TearDown();
    SetUp();
    SingleFieldPartitionDataProvider provider;
    provider.Init(GET_TEST_DATA_PATH(), "uint64", SFP_PK_INDEX);
    PrimaryKeyIndexConfigPtr pkConfig = DYNAMIC_POINTER_CAST(
        PrimaryKeyIndexConfig, provider.GetIndexConfig());
    assert(pkConfig);
    pkConfig->SetPrimaryKeyIndexType(pkIndexType);
    provider.Build(docStr, SFP_OFFLINE);
    PartitionDataPtr partData = provider.GetPartitionData();
    const vector<SegmentData>& segmentDatas =
        partData->GetSegmentDirectory()->GetSegmentDatas();

    size_t totalDocCount = 0;
    size_t totalPkCount = 0;
    vector<size_t> pkCountVec;
    vector<vector<string> > pkInfo;
    StringUtil::fromString(docStr, pkInfo, ",", "#");
    for (size_t i = 0; i < pkInfo.size(); ++i)
    {
        totalDocCount += pkInfo[i].size();
        set<string> pkSet;
        for (size_t j = 0; j < pkInfo[i].size(); ++j)
        {
            pkSet.insert(pkInfo[i][j]);
        }
        pkCountVec.push_back(pkSet.size());
        totalPkCount += pkSet.size();
    }
    SequentialPrimaryKeyIterator<uint64_t> seqIterator(pkConfig);
    seqIterator.Init(segmentDatas);
    ASSERT_EQ(totalDocCount, seqIterator.GetDocCount());
    ASSERT_EQ(totalPkCount, seqIterator.GetPkCount());
    
    bool sortKey = (pkIndexType == pk_hash_table) ? false : true;
    CheckIterator<uint64_t>(seqIterator, sortKey, pkCountVec, expectResults);
}


IE_NAMESPACE_END(index);

