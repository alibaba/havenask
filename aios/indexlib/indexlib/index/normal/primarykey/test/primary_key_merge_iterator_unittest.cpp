#include "indexlib/index/normal/primarykey/test/primary_key_merge_iterator_unittest.h"
#include "indexlib/index_base/segment/segment_directory.h"
#include "indexlib/test/single_field_partition_data_provider.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, PrimaryKeyMergeIteratorTest);

PrimaryKeyMergeIteratorTest::PrimaryKeyMergeIteratorTest()
{
}

PrimaryKeyMergeIteratorTest::~PrimaryKeyMergeIteratorTest()
{
}

void PrimaryKeyMergeIteratorTest::CaseSetUp()
{
}

void PrimaryKeyMergeIteratorTest::CaseTearDown()
{
}


void PrimaryKeyMergeIteratorTest::TestSimpleProcess()
{
    SingleFieldPartitionDataProvider provider;
    provider.Init(GET_TEST_DATA_PATH(), "uint64", SFP_PK_INDEX);
    PrimaryKeyIndexConfigPtr pkConfig = DYNAMIC_POINTER_CAST(
        PrimaryKeyIndexConfig, provider.GetIndexConfig());
    assert(pkConfig);
    pkConfig->SetPrimaryKeyIndexType(pk_sort_array);

    string docsStr = "0,1,2,3#4,5,6#7,8,9"; 
    provider.Build(docsStr, SFP_OFFLINE);
    PartitionDataPtr partData = provider.GetPartitionData();
    const vector<SegmentData>& segmentDatas =
        partData->GetSegmentDirectory()->GetSegmentDatas();

    {
        ReclaimMapPtr reclaimMap(new ReclaimMap());
        InitReclaimMap(reclaimMap, 10);
        PrimaryKeyMergeIterator<uint64_t> pkMergeIter(pkConfig, reclaimMap);
        pkMergeIter.Init(segmentDatas);
        CheckIterator<uint64_t>(pkMergeIter, true, "0:0,1:1,2:2,3:3,4:4,5:5,6:6,7:7,8:8,9:9");
    }
    {
        ReclaimMapPtr reclaimMap(new ReclaimMap());
        InitReclaimMap(reclaimMap, 10, "1:10,3:30,5:50,7:70,9:90");
        PrimaryKeyMergeIterator<uint64_t> pkMergeIter(pkConfig, reclaimMap);
        pkMergeIter.Init(segmentDatas);
        CheckIterator<uint64_t>(pkMergeIter, true, "0:0,1:10,2:2,3:30,4:4,5:50,6:6,7:70,8:8,9:90");
    }
}

void PrimaryKeyMergeIteratorTest::TestIterate()
{
    // check normal iterate  
    InnerTestIterator("10,11,12,13#14,15,16#17,18,19", "",
                      "10:0,11:1,12:2,13:3,14:4,15:5,16:6,17:7,18:8,19:9");

    // check reclaim map
    InnerTestIterator("10,11,12,13#14,15,16#17,18,19", "1:101,3:103,5:105,7:107,9:109",
                      "10:0,11:101,12:2,13:103,14:4,15:105,16:6,17:107,18:8,19:109");

    // check with deleted doc in reclaimMap and empty segments
    InnerTestIterator("10,11,12,13##14,15,16##17,18,19", "0:-1,1:-1,2:-1,3:-1,5:-1,7:-1,9:-1",
                      "14:4,16:6,18:8");

    // check with all docs deleted
    InnerTestIterator("10,11#12,13", "0:-1,1:-1,2:-1,3:-1", "");
}

void PrimaryKeyMergeIteratorTest::InitReclaimMap(const ReclaimMapPtr& reclaimMap,
                                                 uint32_t docCount,
                                                 const string& mapStr)
{
    assert(reclaimMap);
    reclaimMap->Init(docCount);
    vector<vector<string> > docidMap;
    StringUtil::fromString(mapStr, docidMap, ":", ",");
    assert(docidMap.size() <= docCount);
    
    for (docid_t i = 0; (size_t)i < docCount; ++i)
    {
        reclaimMap->SetNewId(i, i);
    }

    for (size_t i = 0; i < docidMap.size(); ++i)
    {
        assert(docidMap[i].size() == 2);
        docid_t oldDocId = StringUtil::fromString<docid_t>(docidMap[i][0]);
        docid_t newDocId = StringUtil::fromString<docid_t>(docidMap[i][1]);
        assert((size_t)oldDocId < docCount);
        reclaimMap->SetNewId(oldDocId, newDocId);
    }
}

void PrimaryKeyMergeIteratorTest::InnerTestIterator(
    const string& docStr, const string& reclaimMapStr,
    const string& expectResults)
{
    InnerTestIteratorOnPkIndexType(pk_hash_table, docStr, reclaimMapStr, expectResults);
    InnerTestIteratorOnPkIndexType(pk_sort_array, docStr, reclaimMapStr, expectResults);
}

void PrimaryKeyMergeIteratorTest::InnerTestIteratorOnPkIndexType(
    PrimaryKeyIndexType pkIndexType,
    const string& docStr, const string& reclaimMapStr,
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

    size_t docCount = 0;
    for (size_t i = 0; i < segmentDatas.size(); ++i)
    {
        docCount += segmentDatas[i].GetSegmentInfo().docCount;
    }

    ReclaimMapPtr reclaimMap(new ReclaimMap());
    InitReclaimMap(reclaimMap, docCount, reclaimMapStr);
    PrimaryKeyMergeIterator<uint64_t> pkMergeIter(pkConfig, reclaimMap);
    pkMergeIter.Init(segmentDatas);
    bool sortKey = (pkIndexType == pk_hash_table) ? false : true;
    CheckIterator<uint64_t>(pkMergeIter, sortKey, expectResults);
}

IE_NAMESPACE_END(index);
