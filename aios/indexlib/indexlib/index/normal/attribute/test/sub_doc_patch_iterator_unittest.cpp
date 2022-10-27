#include "indexlib/index/normal/attribute/test/sub_doc_patch_iterator_unittest.h"
#include "indexlib/index/normal/attribute/accessor/attr_field_value.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/partition_data_maker.h"

using namespace std;
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, SubDocPatchIteratorTest);

SubDocPatchIteratorTest::SubDocPatchIteratorTest()
{
}

SubDocPatchIteratorTest::~SubDocPatchIteratorTest()
{
}

void SubDocPatchIteratorTest::CaseSetUp()
{
    string field = "string1:string;price:int32;count:int32";
    string index = "pk:primarykey64:string1";
    string attr = "price;count";
    string summary = "";
    mSchema = SchemaMaker::MakeSchema(field, index, attr, summary);
    IndexPartitionSchemaPtr subSchema = SchemaMaker::MakeSchema(
            "sub_pk:string;sub_int:int32;",
            "sub_pk:primarykey64:sub_pk",
            "sub_pk;sub_int;",
            "");
    mSchema->SetSubIndexPartitionSchema(subSchema);
    mSubSchema = subSchema;
    mRootDir = GET_TEST_DATA_PATH();
}

void SubDocPatchIteratorTest::CaseTearDown()
 {
}

void SubDocPatchIteratorTest::TestSimpleProcess()
{
    IndexPartitionOptions options;
    options.SetIsOnline(false);
    options.GetBuildConfig().maxDocCount = 1;
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, options, mRootDir));
    
    string fullDocStr = "cmd=add,string1=pk1,sub_pk=sub11^sub12^sub13,sub_int=1^2^3;"
                        "cmd=add,string1=pk2,sub_pk=sub2,sub_int=2;"
                        "cmd=add,string1=pk3,sub_pk=sub3,sub_int=3;"
                        "cmd=add,string1=pk4,sub_pk=sub4,sub_int=4;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocStr, "", ""));

    string incDocStr1 = "cmd=update_field,price=1,count=2,string1=pk1,sub_pk=sub11^sub13,sub_int=11^33;"
                        "cmd=update_field,price=4,count=5,string1=pk3;"
                        "cmd=update_field,string1=pk2,sub_pk=sub2,sub_int=22;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocStr1, "", ""));

    PartitionDataPtr partitionData = 
        PartitionDataMaker::CreateSimplePartitionData(
                GET_FILE_SYSTEM(), "", index_base::Version(), true);
    SubDocPatchIterator iter(psm.GetIndexPartition()->GetSchema());
    iter.Init(partitionData, false, INVALID_VERSION);

    CheckIter(iter, 0, 1, 11, true); // MainDocId = 0
    CheckIter(iter, 2, 1, 33, true); // MainDocId = 0
    CheckIter(iter, 0, 1, 1, false);
    CheckIter(iter, 0, 2, 2, false);
    CheckIter(iter, 3, 1, 22, true); // MainDocId = 1
    CheckIter(iter, 2, 1, 4, false);
    CheckIter(iter, 2, 2, 5, false);
    ASSERT_FALSE(iter.HasNext());
}

void SubDocPatchIteratorTest::CheckIter(
        SubDocPatchIterator& iter, docid_t expectDocid, 
        fieldid_t expectFieldId, int32_t expectPatchValue, bool isSub)
{
    stringstream ss;
    ss << "IsSub:" << isSub << ", "
       << "expectDocid:" << expectDocid << ", "
       << "expectFieldId:" << expectFieldId << ", "
       << "expectPatchValue:" << expectPatchValue;
    SCOPED_TRACE(ss.str());
    AttrFieldValue value;
    iter.Reserve(value);
    ASSERT_TRUE(iter.HasNext());
    iter.Next(value);
    ASSERT_EQ(isSub, value.IsSubDocId());
    ASSERT_EQ(expectDocid, value.GetDocId());
    ASSERT_EQ(expectFieldId, value.GetFieldId());
    ASSERT_EQ(sizeof(int32_t), value.GetDataSize());
    ASSERT_EQ(expectPatchValue, *(int32_t*)value.Data());
}


IE_NAMESPACE_END(index);

