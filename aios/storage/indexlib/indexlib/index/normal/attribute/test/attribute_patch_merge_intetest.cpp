#include "indexlib/index/normal/attribute/test/attribute_patch_merge_intetest.h"

#include "autil/StringUtil.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_reader.h"
#include "indexlib/test/schema_maker.h"

using namespace std;
using namespace autil;

using namespace indexlib::test;
using namespace indexlib::file_system;
using namespace indexlib::partition;
using namespace indexlib::index_base;
using namespace indexlib::config;

namespace indexlib { namespace index {
IE_LOG_SETUP(attribute, AttributePatchMergeTest);
typedef SingleValueAttributeReader<uint32_t> UInt32AttributeReader;

AttributePatchMergeTest::AttributePatchMergeTest() {}

AttributePatchMergeTest::~AttributePatchMergeTest() {}

void AttributePatchMergeTest::CaseSetUp()
{
    string field = "pk:string:pk;long1:uint32;multi_long:uint64:true:true;"
                   "multi_str_attr:string:true:true;single_str_attr:string:false:true";
    string index = "pk:primarykey64:pk;";
    string attr = "long1;multi_long;multi_str_attr;single_str_attr";

    mSchema = SchemaMaker::MakeSchema(field, index, attr, "");
    mRootDir = GET_TEMP_DATA_PATH();
}

void AttributePatchMergeTest::CaseTearDown() {}

void AttributePatchMergeTest::TestSingleValueAttrPatchMerge()
{
    PartitionStateMachine psm;
    IndexPartitionOptions options;
    MergeConfig& mergeConfig = options.GetMergeConfig();
    mergeConfig.mergeStrategyStr = OPTIMIZE_MERGE_STRATEGY_STR;
    mergeConfig.mergeStrategyParameter.SetLegacyString("max-doc-count=1");

    ASSERT_TRUE(psm.Init(mSchema, options, mRootDir));

    string seg0Docs = "cmd=add,pk=0,long1=0;cmd=add,pk=2;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, seg0Docs, "", ""));

    string seg1Docs = "cmd=update_field,pk=0,long1=1;cmd=add,pk=3;cmd=add,pk=4;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, seg1Docs, "", ""));

    string seg2Docs = "cmd=update_field,pk=0,long1=2;cmd=add,pk=5;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, seg2Docs, "", ""));

    string seg3Docs = "cmd=update_field,pk=0,long1=3;cmd=add,pk=6;cmd=add,pk=7;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, seg3Docs, "", ""));

    string seg4Docs = "cmd=update_field,pk=0,long1=4;cmd=add,pk=8;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, seg4Docs, "", ""));

    string seg5Docs = "cmd=update_field,pk=0,long1=5;cmd=add,pk=9;cmd=add,pk=10;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC, seg5Docs, "pk:0", "docid=0, long1=5"));

    GET_FILE_SYSTEM()->TEST_MountLastVersion();
    DirectoryPtr partitionDir = GET_PARTITION_DIRECTORY();
    string long1AttrDir = "segment_6_level_0/attribute/long1/";
    ASSERT_TRUE(partitionDir->IsExist(long1AttrDir + "4_0.patch"));
    ASSERT_FALSE(partitionDir->IsExist(long1AttrDir + "2_0.patch"));
}

void AttributePatchMergeTest::TestVarNumAttrPatchMerge()
{
    PartitionStateMachine psm;
    IndexPartitionOptions options;
    MergeConfig& mergeConfig = options.GetMergeConfig();
    mergeConfig.mergeStrategyStr = OPTIMIZE_MERGE_STRATEGY_STR;
    mergeConfig.mergeStrategyParameter.SetLegacyString("max-doc-count=1");

    ASSERT_TRUE(psm.Init(mSchema, options, mRootDir));

    string seg0Docs = "cmd=add,pk=0,multi_long=0;cmd=add,pk=2;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, seg0Docs, "", ""));

    string seg1Docs = "cmd=update_field,pk=0,multi_long=1;cmd=add,pk=3;cmd=add,pk=4;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, seg1Docs, "", ""));

    string seg2Docs = "cmd=update_field,pk=0,multi_long=2;cmd=add,pk=5;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, seg2Docs, "", ""));

    string seg3Docs = "cmd=update_field,pk=0,multi_long=3;cmd=add,pk=6;cmd=add,pk=7;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, seg3Docs, "", ""));

    string seg4Docs = "cmd=update_field,pk=0,multi_long=4;cmd=add,pk=8;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, seg4Docs, "", ""));

    string seg5Docs = "cmd=update_field,pk=0,multi_long=5;cmd=add,pk=9;cmd=add,pk=10;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC, seg5Docs, "pk:0", "docid=0, multi_long=5"));

    GET_FILE_SYSTEM()->TEST_MountLastVersion();
    DirectoryPtr partitionDir = GET_PARTITION_DIRECTORY();
    string attrDir = "segment_6_level_0/attribute/multi_long/";
    ASSERT_TRUE(partitionDir->IsExist(attrDir + "4_0.patch"));
    ASSERT_FALSE(partitionDir->IsExist(attrDir + "2_0.patch"));
}

void AttributePatchMergeTest::TestMultiStringAttrPatchMerge()
{
    PartitionStateMachine psm;
    IndexPartitionOptions options;
    MergeConfig& mergeConfig = options.GetMergeConfig();
    mergeConfig.mergeStrategyStr = OPTIMIZE_MERGE_STRATEGY_STR;
    mergeConfig.mergeStrategyParameter.SetLegacyString("max-doc-count=1");

    ASSERT_TRUE(psm.Init(mSchema, options, mRootDir));

    string seg0Docs = "cmd=add,pk=0,multi_str_attr=0;cmd=add,pk=2;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, seg0Docs, "", ""));

    string seg1Docs = "cmd=update_field,pk=0,multi_str_attr=1;cmd=add,pk=3;cmd=add,pk=4;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, seg1Docs, "", ""));

    string seg2Docs = "cmd=update_field,pk=0,multi_str_attr=2;cmd=add,pk=5;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, seg2Docs, "", ""));

    string seg3Docs = "cmd=update_field,pk=0,multi_str_attr=3;cmd=add,pk=6;cmd=add,pk=7;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, seg3Docs, "", ""));

    string seg4Docs = "cmd=update_field,pk=0,multi_str_attr=4;cmd=add,pk=8;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, seg4Docs, "", ""));

    string seg5Docs = "cmd=update_field,pk=0,multi_str_attr=5;cmd=add,pk=9;cmd=add,pk=10;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC, seg5Docs, "pk:0", "docid=0,multi_str_attr=5"));

    GET_FILE_SYSTEM()->TEST_MountLastVersion();
    DirectoryPtr partitionDir = GET_PARTITION_DIRECTORY();
    string attrDir = "segment_6_level_0/attribute/multi_str_attr/";
    ASSERT_TRUE(partitionDir->IsExist(attrDir + "4_0.patch"));
    ASSERT_FALSE(partitionDir->IsExist(attrDir + "2_0.patch"));
}

void AttributePatchMergeTest::TestSingleStringAttrPatchMerge()
{
    PartitionStateMachine psm;
    IndexPartitionOptions options;
    MergeConfig& mergeConfig = options.GetMergeConfig();
    mergeConfig.mergeStrategyStr = OPTIMIZE_MERGE_STRATEGY_STR;
    mergeConfig.mergeStrategyParameter.SetLegacyString("max-doc-count=1");

    ASSERT_TRUE(psm.Init(mSchema, options, mRootDir));

    string seg0Docs = "cmd=add,pk=0,single_str_attr=0;cmd=add,pk=2;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, seg0Docs, "", ""));

    string seg1Docs = "cmd=update_field,pk=0,single_str_attr=1;cmd=add,pk=3;cmd=add,pk=4;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, seg1Docs, "", ""));

    string seg2Docs = "cmd=update_field,pk=0,single_str_attr=2;cmd=add,pk=5;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, seg2Docs, "", ""));

    string seg3Docs = "cmd=update_field,pk=0,single_str_attr=3;cmd=add,pk=6;cmd=add,pk=7;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, seg3Docs, "", ""));

    string seg4Docs = "cmd=update_field,pk=0,single_str_attr=4;cmd=add,pk=8;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, seg4Docs, "", ""));

    string seg5Docs = "cmd=update_field,pk=0,single_str_attr=5;cmd=add,pk=9;cmd=add,pk=10;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC, seg5Docs, "pk:0", "docid=0, single_str_attr=5"));

    GET_FILE_SYSTEM()->TEST_MountLastVersion();
    DirectoryPtr partitionDir = GET_PARTITION_DIRECTORY();
    string attrDir = "segment_6_level_0/attribute/single_str_attr/";
    ASSERT_TRUE(partitionDir->IsExist(attrDir + "4_0.patch"));
    ASSERT_FALSE(partitionDir->IsExist(attrDir + "2_0.patch"));
}

void AttributePatchMergeTest::TestMergeTwoSegsForFormerSegWithNewerPatch()
{
    SingleFieldPartitionDataProvider provider;
    provider.Init(mRootDir, "uint32", SFP_ATTRIBUTE);
    int segNum = 20;
    string docStr = GenerateDocString(segNum);
    provider.Build(docStr, SFP_OFFLINE);

    // check results
    CheckMergeResult(provider, segNum);

    // generate merged segments
    provider.Merge("7,8");   // 20
    provider.Merge("3,4");   // 21
    provider.Merge("2,15");  // 22
    provider.Merge("9,11");  // 23
    provider.Merge("16,12"); // 24
    provider.Merge("1,5");   // 25
    provider.Merge("13");    // 26
    provider.Merge("17,18"); // 27

    // merge s22 and s25
    provider.Merge("22,25");

    // check results
    CheckMergeResult(provider, segNum);
}

void AttributePatchMergeTest::TestMergeTwoSegsForLatterSegWithNewerPatch()
{
    SingleFieldPartitionDataProvider provider;
    provider.Init(mRootDir, "uint32", SFP_ATTRIBUTE);
    int segNum = 20;
    string docStr = GenerateDocString(segNum);
    provider.Build(docStr, SFP_OFFLINE);

    // check results
    CheckMergeResult(provider, segNum);

    // generate merged segments
    provider.Merge("7,8");   // 20
    provider.Merge("3,4");   // 21
    provider.Merge("1,5");   // 22
    provider.Merge("9,11");  // 23
    provider.Merge("16,12"); // 24
    provider.Merge("2,15");  // 25
    provider.Merge("13");    // 26
    provider.Merge("17,18"); // 27

    // merge s22 and s25
    provider.Merge("22,25");

    // check results
    CheckMergeResult(provider, segNum);
}

void AttributePatchMergeTest::TestMergeTwoSegsForLatterHasNoPatch()
{
    SingleFieldPartitionDataProvider provider;
    provider.Init(mRootDir, "uint32", SFP_ATTRIBUTE);
    int segNum = 10;
    string docStr = GenerateDocString(segNum);
    provider.Build(docStr, SFP_OFFLINE);
    // check results
    CheckMergeResult(provider, segNum);

    // generate merged segments
    provider.Merge("2,4"); // 10
    provider.Merge("1,8"); // 11
    provider.Merge("6,9"); // 12

    provider.Build("add:13", SFP_OFFLINE); // 13 with no patch

    provider.Merge("3,7"); // 14

    // merge s22 and s25
    provider.Merge("11,13");

    // check results
    CheckMergeResult(provider, segNum);
}

void AttributePatchMergeTest::TestMergeTwoSegsForFormerHasNoPatch()
{
    SingleFieldPartitionDataProvider provider;
    provider.Init(mRootDir, "uint32", SFP_ATTRIBUTE);
    int segNum = 10;
    string docStr = GenerateDocString(segNum);
    provider.Build(docStr, SFP_OFFLINE);
    // check results
    CheckMergeResult(provider, segNum);

    // generate merged segments
    provider.Merge("2,4");                 // 10
    provider.Build("add:11", SFP_OFFLINE); // 11 with no patch
    provider.Merge("6,9");                 // 12
    provider.Merge("1,8");                 // 13
    provider.Merge("3,7");                 // 14

    // merge s22 and s25
    provider.Merge("11,13");

    // check results
    CheckMergeResult(provider, segNum);
}

void AttributePatchMergeTest::TestMergeSingleSegment()
{
    SingleFieldPartitionDataProvider provider;
    provider.Init(mRootDir, "uint32", SFP_ATTRIBUTE);
    int segNum = 10;
    string docStr = GenerateDocString(segNum);
    provider.Build(docStr, SFP_OFFLINE);
    // check results
    CheckMergeResult(provider, segNum);

    // generate merged segments
    provider.Merge("3,7"); // 10
    provider.Merge("2,6"); // 11
    provider.Merge("1,8"); // 12

    // merge s11
    provider.Merge("11");

    // check results
    CheckMergeResult(provider, segNum);
}

void AttributePatchMergeTest::TestMergeWithTwoMergePlan()
{
    SingleFieldPartitionDataProvider provider;
    provider.Init(mRootDir, "uint32", SFP_ATTRIBUTE);
    int segNum = 5;
    string docStr = GenerateDocString(segNum);
    provider.Build(docStr, SFP_OFFLINE);
    // check results
    CheckMergeResult(provider, segNum);

    // generate merged segments
    provider.Merge("1,3;2,4"); // 5 and 6

    // check results
    CheckMergeResult(provider, segNum);
}

void AttributePatchMergeTest::CheckMergeResult(SingleFieldPartitionDataProvider& provider, int segNum)
{
    PartitionDataPtr partitionData = provider.GetPartitionData();
    IndexPartitionOptions options;
    partition::OnlinePartition onlinePart;
    onlinePart.Open(mRootDir, "", provider.GetSchema(), options);
    partition::IndexPartitionReaderPtr partReader = onlinePart.GetReader();

    string fieldName = provider.GetAttributeConfig()->GetAttrName();
    AttributeReaderPtr attrReaderBase = partReader->GetAttributeReader(fieldName);
    typedef SingleValueAttributeReader<uint32_t> UInt32AttributeReader;
    DEFINE_SHARED_PTR(UInt32AttributeReader);
    UInt32AttributeReaderPtr attrReader = DYNAMIC_POINTER_CAST(UInt32AttributeReader, attrReaderBase);
    uint32_t value;
    for (int docId = 0; docId < segNum; docId++) {
        bool isNull;
        ASSERT_TRUE(attrReader->Read(docId, value, isNull));
        EXPECT_EQ((uint32_t)(segNum - 1 - docId), value);
    }
}

string AttributePatchMergeTest::GenerateDocString(int segNum)
{
    int docNum = segNum;
    string seg0DocStr = "add:0";
    for (int i = 0; i < docNum - 1; i++) {
        seg0DocStr += ",add:0";
    }

    string docStrings = seg0DocStr;
    for (int segId = 1; segId < segNum; segId++) {
        string segDocStr = "#update_field:0:" + StringUtil::toString(segId);
        for (int docId = 1; docId < docNum - segId; ++docId) {
            segDocStr += ",update_field:" + StringUtil::toString(docId) + ":" + StringUtil::toString(segId);
        }
        docStrings += segDocStr;
    }
    return docStrings;
}
}} // namespace indexlib::index
