#include "indexlib/index/normal/attribute/test/multi_field_patch_iterator_unittest.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/test/partition_data_maker.h"
#include "indexlib/index/normal/attribute/accessor/pack_attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/attr_field_value.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, MultiFieldPatchIteratorTest);

MultiFieldPatchIteratorTest::MultiFieldPatchIteratorTest()
{
}

MultiFieldPatchIteratorTest::~MultiFieldPatchIteratorTest()
{
}

void MultiFieldPatchIteratorTest::CaseSetUp()
{
    mRootDir = GET_TEST_DATA_PATH();
    string field = "string1:string;price:int32;count:int32";
    string index = "pk:primarykey64:string1";
    string attribute = "price;count";
    mSchema = SchemaMaker::MakeSchema(field, index, attribute, "");

}

void MultiFieldPatchIteratorTest::CaseTearDown()
{
}

void MultiFieldPatchIteratorTest::TestSimpleProcess()
{
    IndexPartitionOptions options;
    options.SetIsOnline(false);
    options.GetBuildConfig().maxDocCount = 1;
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, options, mRootDir));
    
    string fullDocStr = "cmd=add,string1=pk1;"
                        "cmd=add,string1=pk2;"
                        "cmd=add,string1=pk3;"
                        "cmd=add,string1=pk4";

    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocStr, "", ""));

    string incDocStr1 = "cmd=update_field,price=1,count=2,string1=pk1;"
                        "cmd=update_field,price=4,count=5,string1=pk3";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocStr1, "", ""));

    string incDocStr2 = "cmd=update_field,price=2,string1=pk1;"
                        "cmd=update_field,price=4,count=5,string1=pk2;";

    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocStr2, "", ""));

    PartitionDataPtr partitionData = 
        PartitionDataMaker::CreateSimplePartitionData(GET_FILE_SYSTEM());
    MultiFieldPatchIterator iter(mSchema);
    iter.Init(partitionData, false, INVALID_VERSION);
    CheckNormalField(iter, 0, 1, 2);
    CheckNormalField(iter, 0, 2, 2);
    CheckNormalField(iter, 1, 1, 4);
    CheckNormalField(iter, 1, 2, 5);
    CheckNormalField(iter, 2, 1, 4);
    CheckNormalField(iter, 2, 2, 5);
    ASSERT_FALSE(iter.HasNext());
}

void MultiFieldPatchIteratorTest::TestCreateIndependentPatchWorkItems()
{
    IndexPartitionOptions options;
    options.SetIsOnline(false);
    options.GetBuildConfig().maxDocCount = 1;
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, options, mRootDir));
    
    string fullDocStr = "cmd=add,string1=pk1;"
                        "cmd=add,string1=pk2;"
                        "cmd=add,string1=pk3;"
                        "cmd=add,string1=pk4";

    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocStr, "", ""));

    string incDocStr1 = "cmd=update_field,price=1,count=2,string1=pk1;"
                        "cmd=update_field,price=4,count=5,string1=pk3";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocStr1, "", ""));

    string incDocStr2 = "cmd=update_field,price=2,string1=pk1;"
                        "cmd=update_field,price=4,count=5,string1=pk2;";

    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocStr2, "", ""));

    PartitionDataPtr partitionData = 
        PartitionDataMaker::CreateSimplePartitionData(GET_FILE_SYSTEM());
    MultiFieldPatchIterator iter(mSchema);
    iter.Init(partitionData, false, INVALID_VERSION);
    vector<AttributePatchWorkItem*> workItems;
    iter.CreateIndependentPatchWorkItems(workItems);
    ASSERT_EQ(6,workItems.size());
    CheckNormalField(iter, 0, 1, 2);
    CheckNormalField(iter, 0, 2, 2);
    CheckNormalField(iter, 1, 1, 4);
    CheckNormalField(iter, 1, 2, 5);
    CheckNormalField(iter, 2, 1, 4);
    CheckNormalField(iter, 2, 2, 5);
    ASSERT_FALSE(iter.HasNext());

    for (auto &item : workItems)
    {
        DELETE_AND_SET_NULL(item);
    } 
}

void MultiFieldPatchIteratorTest::TestIncConsistentWithRealtime()
{
    IndexPartitionOptions options;
    options.SetIsOnline(false);
    options.GetBuildConfig().maxDocCount = 1;
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, options, mRootDir));
    
    string fullDocStr = "cmd=add,string1=pk1;" 
                        "cmd=add,string1=pk2;"
                        "cmd=add,string1=pk3;"
                        "cmd=add,string1=pk4";

    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocStr, "", ""));

    // 4_0.patch, 4_2.patch
    string incDocStr1 = "cmd=update_field,price=1,count=2,string1=pk1;"
                        "cmd=update_field,price=4,count=5,string1=pk3";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocStr1, "", ""));

    // 5_0.patch, 5_1.patch
    string incDocStr2 = "cmd=update_field,price=2,string1=pk1;"
                        "cmd=update_field,price=4,count=5,string1=pk2;";

    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocStr2, "", ""));

    PartitionDataPtr partitionData = 
        PartitionDataMaker::CreateSimplePartitionData(GET_FILE_SYSTEM());
    {
        // 5_1, 4_2 will be loaded
        MultiFieldPatchIterator iter(mSchema);
        iter.Init(partitionData, true, INVALID_VERSION, 1);
        vector<AttributePatchWorkItem*> workItems;
        iter.CreateIndependentPatchWorkItems(workItems);
        ASSERT_EQ(4,workItems.size());
        CheckNormalField(iter, 1, 1, 4);
        CheckNormalField(iter, 1, 2, 5);
        CheckNormalField(iter, 2, 1, 4);
        CheckNormalField(iter, 2, 2, 5);
        ASSERT_FALSE(iter.HasNext());
        for (auto &item : workItems)
        {
            DELETE_AND_SET_NULL(item);
        } 

    }
    {
        // 5_0, 5_1 will be loaded
        MultiFieldPatchIterator iter(mSchema);
        iter.Init(partitionData, false, INVALID_VERSION, 5);
        vector<AttributePatchWorkItem*> workItems;
        iter.CreateIndependentPatchWorkItems(workItems);
        ASSERT_EQ(3,workItems.size());
        CheckNormalField(iter, 0, 1, 2);
        CheckNormalField(iter, 1, 1, 4);
        CheckNormalField(iter, 1, 2, 5);
        ASSERT_FALSE(iter.HasNext());
        for (auto &item : workItems)
        {
            DELETE_AND_SET_NULL(item);
        } 

    }
    {
        // no patch will be loaded
        MultiFieldPatchIterator iter(mSchema);
        iter.Init(partitionData, true, INVALID_VERSION, 5);
        vector<AttributePatchWorkItem*> workItems;
        iter.CreateIndependentPatchWorkItems(workItems);
        ASSERT_EQ(0,workItems.size());
        ASSERT_FALSE(iter.HasNext());
        for (auto &item : workItems)
        {
            DELETE_AND_SET_NULL(item);
        } 

    }
}

void MultiFieldPatchIteratorTest::TestIterateWithPackAttributes()
{
    string field = "string0:string;id:int32;price:int32;count:int32;size:int32;";
    string index = "pk:primarykey64:string0";
    string attribute = "id;pack_attr:price,count;size";
    IndexPartitionSchemaPtr schema =
        SchemaMaker::MakeSchema(field, index, attribute, "");

    string fullDocStr = "cmd=add,string0=pk0;"
                        "cmd=add,string0=pk1;"
                        "cmd=add,string0=pk2;";

    IndexPartitionOptions options;
    options.SetIsOnline(false);
    options.GetBuildConfig().maxDocCount = 1;
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(schema, options, mRootDir));

    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocStr, "", ""));

    string incDocStr1 = "cmd=update_field,string0=pk0,id=10;"
        "cmd=update_field,string0=pk0,price=20,count=31";

    string incDocStr2 = "cmd=update_field,string0=pk2,size=15,count=6;"
        "cmd=update_field,string0=pk1,size=13,count=5";

    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocStr1, "", ""));
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocStr2, "", ""));
    
    PartitionDataPtr partitionData = 
        PartitionDataMaker::CreateSimplePartitionData(GET_FILE_SYSTEM());
    MultiFieldPatchIterator iter(schema);
    iter.Init(partitionData, false, INVALID_VERSION);
    CheckNormalField(iter, 0, 1, 10);
    CheckPackField(iter, 0, 0, "1:20;2:31");
    CheckNormalField(iter, 1, 4, 13);
    CheckPackField(iter, 1, 0, "2:5");
    CheckNormalField(iter, 2, 4, 15);
    CheckPackField(iter, 2, 0, "2:6");
    ASSERT_FALSE(iter.HasNext());
}

void MultiFieldPatchIteratorTest::CheckNormalField(
        MultiFieldPatchIterator& iter,
        docid_t expectDocId, fieldid_t expectFieldId,
        int32_t expectPatchValue)
{
    ASSERT_TRUE(iter.HasNext());
    AttrFieldValue actualValue;
    iter.Reserve(actualValue);
    iter.Next(actualValue);
    EXPECT_FALSE(actualValue.IsPackAttr());
    EXPECT_EQ(sizeof(int32_t), actualValue.GetDataSize());
    EXPECT_EQ(expectDocId, actualValue.GetDocId());
    EXPECT_EQ(expectFieldId, actualValue.GetFieldId());
    EXPECT_EQ(expectPatchValue, *(int32_t*)actualValue.Data());
}

void MultiFieldPatchIteratorTest::CheckPackField(
    MultiFieldPatchIterator& iter,
    docid_t expectDocId, packattrid_t expectPackId,
    const string& expectPatchValueStr)
{
    ASSERT_TRUE(iter.HasNext());
    AttrFieldValue actualValue;
    iter.Reserve(actualValue);
    iter.Next(actualValue);
    EXPECT_TRUE(actualValue.IsPackAttr());
    EXPECT_EQ(expectDocId, actualValue.GetDocId());
    EXPECT_EQ(expectPackId, actualValue.GetPackAttrId());
    PackAttributeFormatter::PackAttributeFields packFields;
    ASSERT_TRUE(PackAttributeFormatter::DecodePatchValues(
                    actualValue.Data(), actualValue.GetDataSize(), packFields));
    vector<vector<uint32_t> > expectValues;
    StringUtil::fromString(expectPatchValueStr, expectValues, ":", ";");

    ASSERT_EQ(expectValues.size(), packFields.size());
    for (size_t i = 0; i < expectValues.size(); ++i)
    {
        assert(expectValues[i].size() == 2);
        EXPECT_EQ(expectValues[i][0], packFields[i].first);
        EXPECT_EQ(expectValues[i][1], *(uint32_t*)(packFields[i].second.data()));
    }
}


IE_NAMESPACE_END(index);

