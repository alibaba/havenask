#include "indexlib/index/normal/attribute/test/single_field_patch_iterator_unittest.h"
#include "indexlib/index/normal/attribute/accessor/attr_field_value.h"
#include "indexlib/test/single_field_partition_data_provider.h"
#include "indexlib/index_base/patch/patch_file_finder_creator.h"
#include "indexlib/index/normal/attribute/accessor/single_field_patch_work_item.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_patch_reader.h"
#include "indexlib/index/normal/attribute/accessor/attribute_segment_reader.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include <autil/ConstString.h>
#include <autil/StringUtil.h>
#include <vector>
#include <map>

using namespace std;
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, SingleFieldPatchIteratorTest);

namespace
{
    class FakeAttributeSegmentReader : public AttributeSegmentReader
    {
    public:
        FakeAttributeSegmentReader()
            : AttributeSegmentReader(NULL)
            {}
        ~FakeAttributeSegmentReader() {}

    public:
        bool IsInMemory() const override { return false; }
        uint32_t GetDataLength(docid_t docId) const { return 0u;}
        uint64_t GetOffset(docid_t docId) const { return 0u; }
        bool Read(docid_t docId, uint8_t* buf, uint32_t bufLen) { return true; }
        bool UpdateField(docid_t docId, uint8_t* buf, uint32_t bufLen) override
        {
            mDataMap[docId] = *(int32_t*)(buf);
            return true;
        } 
    public:
        map<docid_t, int32_t> mDataMap;
    };
};


SingleFieldPatchIteratorTest::SingleFieldPatchIteratorTest()
{
}

SingleFieldPatchIteratorTest::~SingleFieldPatchIteratorTest()
{
}

void SingleFieldPatchIteratorTest::CaseSetUp()
{
    mRootDir = GET_TEST_DATA_PATH();
}

void SingleFieldPatchIteratorTest::CaseTearDown()
{
}

void SingleFieldPatchIteratorTest::TestSimpleProcess()
{
    SingleFieldPartitionDataProvider provider;
    provider.Init(mRootDir, "int32", SFP_ATTRIBUTE);
    
    /*[-->: update]
      segment_0: 0, 1, 2
      segment_1: 5, 6, 0-->3, 1-->4
      segment_2: 2-->7, 0-->8, 5-->9, 6-->10
     */
    string docString = "0,1,2#"
                       "update_field:0:3,update_field:1:4,5,6#"
                       "update_field:2:7,update_field:0:8,update_field:3:9,update_field:4:10";
    provider.Build(docString, SFP_OFFLINE);
    PartitionDataPtr partitionData = provider.GetPartitionData();

    SingleFieldPatchIterator iter(provider.GetAttributeConfig(), false);
    iter.Init(partitionData, false, INVALID_SEGMENTID);
    ASSERT_EQ((fieldid_t)1, iter.GetFieldId());
    CheckIterator(iter, 0, 1, 8);
    CheckIterator(iter, 1, 1, 4);
    CheckIterator(iter, 2, 1, 7);
    CheckIterator(iter, 3, 1, 9);
    CheckIterator(iter, 4, 1, 10);
    ASSERT_FALSE(iter.HasNext());
}

void SingleFieldPatchIteratorTest::TestCreateIndependentPatchWorkItems()
{
    SingleFieldPartitionDataProvider provider;
    provider.Init(mRootDir, "int32", SFP_ATTRIBUTE);
    
    /*[-->: update]
      segment_0: 0, 1, 2
      segment_1: 5, 6, 0-->3, 1-->4
      segment_2: 2-->7, 0-->8, 5-->9, 6-->10
     */
    string docString = "0,1,2#"
                       "update_field:0:3,update_field:1:4,5,6#"
                       "update_field:2:7,update_field:0:8,update_field:3:9,update_field:4:10";
    provider.Build(docString, SFP_OFFLINE);
    PartitionDataPtr partitionData = provider.GetPartitionData();

    SingleFieldPatchIterator iter(provider.GetAttributeConfig(), false);
    iter.Init(partitionData, false, INVALID_SEGMENTID);
    ASSERT_EQ((fieldid_t)1, iter.GetFieldId());
    vector<AttributePatchWorkItem*> workItems;
    iter.CreateIndependentPatchWorkItems(workItems);
    ASSERT_EQ(2,workItems.size());
    CheckIterator(iter, 0, 1, 8);
    CheckIterator(iter, 1, 1, 4);
    CheckIterator(iter, 2, 1, 7);
    CheckIterator(iter, 3, 1, 9);
    CheckIterator(iter, 4, 1, 10);
    ASSERT_FALSE(iter.HasNext());
    for (auto& item : workItems)
    {
        DELETE_AND_SET_NULL(item);
    }
}

void SingleFieldPatchIteratorTest::TestIncConsistentWithRealtime()
{
    SingleFieldPartitionDataProvider provider;
    provider.Init(mRootDir, "int32", SFP_ATTRIBUTE);
    
    /*[-->: update]
      segment_0: 0, 1, 2
      segment_1: 5, 6, 0-->3, 1-->4
      segment_2: 2-->7, 0-->8, 5-->9, 6-->10
     */
    string docString = "0,1,2#"
                       "update_field:0:3,update_field:1:4,5,6#"
                       "update_field:2:7,update_field:0:8,update_field:3:9,update_field:4:10";
    provider.Build(docString, SFP_OFFLINE);
    PartitionDataPtr partitionData = provider.GetPartitionData();
    {
        SingleFieldPatchIterator iter(provider.GetAttributeConfig(), false);
        iter.Init(partitionData, true, 1);
        ASSERT_EQ((fieldid_t)1, iter.GetFieldId());
        vector<AttributePatchWorkItem*> workItems;
        iter.CreateIndependentPatchWorkItems(workItems);
        ASSERT_EQ(1,workItems.size());
        CheckIterator(iter, 3, 1, 9);
        CheckIterator(iter, 4, 1, 10);
        ASSERT_FALSE(iter.HasNext());
        for (auto& item : workItems)
        {
            DELETE_AND_SET_NULL(item);
        }
    }
    {
        SingleFieldPatchIterator iter(provider.GetAttributeConfig(), false);
        iter.Init(partitionData, false, 2);
        ASSERT_EQ((fieldid_t)1, iter.GetFieldId());
        vector<AttributePatchWorkItem*> workItems;
        iter.CreateIndependentPatchWorkItems(workItems);
        ASSERT_EQ(2,workItems.size());
        CheckIterator(iter, 0, 1, 8);
        CheckIterator(iter, 2, 1, 7);
        CheckIterator(iter, 3, 1, 9);
        CheckIterator(iter, 4, 1, 10);
        ASSERT_FALSE(iter.HasNext());
        for (auto& item : workItems)
        {
            DELETE_AND_SET_NULL(item);
        }

    }
    {
        SingleFieldPatchIterator iter(provider.GetAttributeConfig(), false);
        iter.Init(partitionData, true, 2);
        ASSERT_EQ((fieldid_t)1, iter.GetFieldId());
        vector<AttributePatchWorkItem*> workItems;
        iter.CreateIndependentPatchWorkItems(workItems);
        ASSERT_EQ(0,workItems.size());
        ASSERT_FALSE(iter.HasNext());
        for (auto& item : workItems)
        {
            DELETE_AND_SET_NULL(item);
        }

    }
}

void SingleFieldPatchIteratorTest::TestAttributeSegmentPatchInWorkItems()
{
    SingleFieldPartitionDataProvider provider;
    provider.Init(mRootDir, "int32", SFP_ATTRIBUTE);
    
    /*[-->: update]
      segment_0: 0, 1, 2
      segment_1: 5, 6, 0-->3, 1-->4
      segment_2: 2-->7, 0-->8, 5-->9, 6-->10
     */
    string docString = "0,1,2#"
                       "update_field:0:3,update_field:1:4,5,6#"
                       "update_field:2:7,update_field:0:8,update_field:3:9,update_field:4:10";
    provider.Build(docString, SFP_OFFLINE);
    PartitionDataPtr partitionData = provider.GetPartitionData();
    DeletionMapReaderPtr deletionMapReader(new DeletionMapReader());
    ASSERT_TRUE(deletionMapReader->Open(partitionData.get()));
    SingleFieldPatchIterator iter1(provider.GetAttributeConfig(), false);
    iter1.Init(partitionData, false, INVALID_SEGMENTID);
    SingleFieldPatchIterator iter2(provider.GetAttributeConfig(), false);
    iter2.Init(partitionData, false, INVALID_SEGMENTID);
    ASSERT_EQ((fieldid_t)1, iter2.GetFieldId());
    vector<AttributePatchWorkItem*> workItems;
    iter1.CreateIndependentPatchWorkItems(workItems);
    ASSERT_EQ(2,workItems.size());
    map<docid_t, int32_t> expectedValues;
    AttrFieldValue actualValue;
    iter2.Reserve(actualValue);
    while(iter2.HasNext()){
        docid_t docId = iter2.GetCurrentDocId();
        iter2.Next(actualValue);
        expectedValues[docId] = *actualValue.Data();
    }

    map<docid_t, int32_t> actualValues;
    for(auto workItem: workItems){
        SingleFieldPatchWorkItem* workItemPtr = dynamic_cast<SingleFieldPatchWorkItem*>(workItem);
        FakeAttributeSegmentReader* fakeReader = new FakeAttributeSegmentReader();
        workItemPtr->mAttrSegReader.reset(fakeReader);
        workItemPtr->mDeletionMapReader = deletionMapReader;
        workItemPtr->mBuffer.Reserve(sizeof(int32_t));
        while (workItemPtr->HasNext())
        {
            workItemPtr->ProcessNext();
        }

        for (const auto& kv : fakeReader->mDataMap)
        {
            actualValues.insert(make_pair(workItemPtr->mBaseDocId + kv.first, kv.second));
        }
    }
    ASSERT_EQ(expectedValues.size(), actualValues.size());
    for (const auto& kv : expectedValues)
    {
        EXPECT_EQ(kv.second, actualValues[kv.first]);
    }

    for (auto &item : workItems)
    {
        DELETE_AND_SET_NULL(item);
    }
}

void SingleFieldPatchIteratorTest::TestInit()
{
    {
        /* test no patch
          segment_0: 0, 1, 2
          segment_1: 5, 6
          segment_2: 7
        */
        string docString = "0,1,2#5,6#7";
        PartitionDataPtr partitionData;
        AttributeConfigPtr attrConfig;
        PreparePatchPartitionData(docString, partitionData, attrConfig);
        SingleFieldPatchIterator iter(attrConfig, false);
        iter.Init(partitionData, false, INVALID_SEGMENTID);
        ASSERT_FALSE(iter.HasNext());
        ASSERT_EQ(INVALID_DOCID, iter.GetCurrentDocId());
    }

    {
        /* test patch filtered
          segment_0: 0, 1, 2
          segment_1: 5, 6, 0--->7
        */
        string docString = "0,1,2#5,6,update_field:0:7";
        PartitionDataPtr partitionData;
        AttributeConfigPtr attrConfig;
        PreparePatchPartitionData(docString, partitionData, attrConfig);
        SingleFieldPatchIterator iter(attrConfig, false);
        iter.Init(partitionData, false, 2);
        ASSERT_FALSE(iter.HasNext());
        ASSERT_EQ(INVALID_DOCID, iter.GetCurrentDocId());
    }

    {
        /* test normal case
          segment_0: 0, 1, 2
          segment_1: 3, 4, 0--->7
          segment_2: 1--->8, 4--->9
        */
        string docString = "0,1,2#5,6,update_field:0:7#"
                           "update_field:1:8,update_field:4:9";
        PartitionDataPtr partitionData;
        AttributeConfigPtr attrConfig;
        PreparePatchPartitionData(docString, partitionData, attrConfig);
        SingleFieldPatchIterator iter(attrConfig, false);
        iter.Init(partitionData, false, 2);
        ASSERT_TRUE(iter.HasNext());
        ASSERT_EQ(1, iter.GetCurrentDocId());
        CheckIterator(iter, 1, 1, 8);
        CheckIterator(iter, 4, 1, 9);
    }
}

void SingleFieldPatchIteratorTest::TestNext()
{
    /* check patch value get in order
       with empty segment in middle
       segment_0: 0, 1, 2
       segment_1: 3, 4, 0--->100
       segment_2: del 3, del 4, 2--->102
       segment_3: 5, 6, 2--->103, 3--->104
       segment_4: 5--->105
    */
    string docString = "0,1,2#3,4,update_field:0:100#"
                       "delete:3,delete:4,update_field:2:102#"
                       "5,6,update_field:2:103,update_field:3:104#"
                       "update_field:5:105";

    PartitionDataPtr partitionData;
    AttributeConfigPtr attrConfig;
    PreparePatchPartitionData(docString, partitionData, attrConfig);
    SingleFieldPatchIterator iter(attrConfig, false);
    iter.Init(partitionData, false, INVALID_SEGMENTID);
    CheckIterator(iter, 0, 1, 100);
    CheckIterator(iter, 2, 1, 103);
    CheckIterator(iter, 5, 1, 105);
    ASSERT_FALSE(iter.HasNext());
}

void SingleFieldPatchIteratorTest::TestFilterLoadedPatchFileInfos()
{
    /*[-->: update]
      segment_0: 0, 1, 2
      segment_1: 5, 6, 0-->3, 1-->4
      segment_2: 2-->7, 0-->8, 5-->9, 6-->10
     */
    string docString = "0,1,2#"
                       "update_field:0:3,update_field:1:4,5,6#"
                       "update_field:2:7,update_field:0:8,update_field:3:9,update_field:4:10";
    PartitionDataPtr partitionData;
    AttributeConfigPtr attrConfig;
    PreparePatchPartitionData(docString, partitionData, attrConfig);

    AttrPatchInfos attrPatchInfos;
    PatchFileFinderPtr patchFinder = 
        PatchFileFinderCreator::Create(partitionData.get());
    patchFinder->FindAttrPatchFiles(attrConfig, attrPatchInfos);
    {
        //check filter nothing
        AttrPatchFileInfoVec patchVec = attrPatchInfos[1];
        ASSERT_EQ((size_t)1, patchVec.size());
        SingleFieldPatchIterator iter(attrConfig, false);
        AttrPatchFileInfoVec validPatchVec = 
            iter.FilterLoadedPatchFileInfos(patchVec, 1);
        ASSERT_EQ((size_t)1, validPatchVec.size());
    }

    {
        //check filter part patch
        AttrPatchFileInfoVec patchVec = attrPatchInfos[0];
        ASSERT_EQ((size_t)2, patchVec.size());
        SingleFieldPatchIterator iter(attrConfig, false);
        AttrPatchFileInfoVec validPatchVec = 
            iter.FilterLoadedPatchFileInfos(patchVec, 2);
        ASSERT_EQ((size_t)1, validPatchVec.size());
        ASSERT_EQ(string("2_0.patch"), validPatchVec[0].patchFileName);
    }

    {
        //check filter all patch
        AttrPatchFileInfoVec patchVec = attrPatchInfos[0];
        ASSERT_EQ((size_t)2, patchVec.size());
        SingleFieldPatchIterator iter(attrConfig, false);
        AttrPatchFileInfoVec validPatchVec = 
            iter.FilterLoadedPatchFileInfos(patchVec, 3);
        ASSERT_EQ((size_t)0, validPatchVec.size());
    }
    
    {
        //check filter segment is invalid
        AttrPatchFileInfoVec patchVec = attrPatchInfos[0];
        ASSERT_EQ((size_t)2, patchVec.size());
        SingleFieldPatchIterator iter(attrConfig, false);
        AttrPatchFileInfoVec validPatchVec = 
            iter.FilterLoadedPatchFileInfos(patchVec, INVALID_SEGMENTID);
        ASSERT_EQ((size_t)2, validPatchVec.size());
    }
}

void SingleFieldPatchIteratorTest::CheckIterator(
        SingleFieldPatchIterator& iter, 
        docid_t expectDocid, fieldid_t expectFieldId, int32_t expectValue)
{
    AttrFieldValue actualValue;
    iter.Reserve(actualValue);
    ASSERT_TRUE(iter.HasNext());
    ASSERT_EQ(expectDocid, iter.GetCurrentDocId());
    iter.Next(actualValue);
    ASSERT_EQ(expectValue, *(int32_t*)actualValue.Data());
    ASSERT_EQ(expectFieldId, actualValue.GetFieldId());
    ASSERT_EQ(expectDocid, actualValue.GetDocId());
    ASSERT_EQ((size_t)4, actualValue.GetDataSize());
}

void SingleFieldPatchIteratorTest::PreparePatchPartitionData(
        const string& docs, PartitionDataPtr& partitionData, 
        AttributeConfigPtr& attrConfig)
{
    TearDown();
    SetUp();
    SingleFieldPartitionDataProvider provider;
    provider.Init(mRootDir, "int32", SFP_ATTRIBUTE);    
    provider.Build(docs, SFP_OFFLINE);
    partitionData = provider.GetPartitionData();
    attrConfig = provider.GetAttributeConfig();
}

IE_NAMESPACE_END(index);

