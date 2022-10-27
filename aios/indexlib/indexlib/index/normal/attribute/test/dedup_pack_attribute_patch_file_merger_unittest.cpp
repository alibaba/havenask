#include "indexlib/index/normal/attribute/test/dedup_pack_attribute_patch_file_merger_unittest.h"
#include "indexlib/test/partition_data_maker.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/config/index_partition_options.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, DedupPackAttributePatchFileMergerTest);

DedupPackAttributePatchFileMergerTest::DedupPackAttributePatchFileMergerTest()
{
}

DedupPackAttributePatchFileMergerTest::~DedupPackAttributePatchFileMergerTest()
{
}

void DedupPackAttributePatchFileMergerTest::CaseSetUp()
{
    string field = "string1:string;string2:string:false:true;price:uint32;name:string:true:true";
    string index = "pk:primarykey64:string1";
    string attribute = "packAttr:string2,name,price";
    mSchema = SchemaMaker::MakeSchema(field, index, attribute, "");
}

void DedupPackAttributePatchFileMergerTest::CaseTearDown()
{
}

void DedupPackAttributePatchFileMergerTest::TestSimpleProcess()
{
    IndexPartitionOptions options;
    options.SetIsOnline(false);
    options.GetBuildConfig().keepVersionCount = 10;
    options.GetBuildConfig().enablePackageFile = false;

    PartitionStateMachine psm;
    DirectoryPtr partDir = GET_PARTITION_DIRECTORY();
    ASSERT_TRUE(psm.Init(mSchema, options, GET_TEST_DATA_PATH()));

    string fullDoc = "cmd=add,string1=pk0,string2=hello,price=33,name=dog name;";
    string incDoc1 = "cmd=update_field,string1=pk0,string2=hello1;";
    string incDoc2 = "cmd=update_field,string1=pk0,string2=hello2,price=35;";
    string incDoc3 = "cmd=update_field,string1=pk0,name=cat name;";
    
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDoc, "", ""));
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDoc1, "", ""));
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDoc2, "", ""));
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDoc3, "", ""));

    const PackAttributeConfigPtr& packAttrConfig = mSchema->
        GetAttributeSchema()->GetPackAttributeConfig("packAttr");
    assert(packAttrConfig);
    DedupPackAttributePatchFileMerger dedupPatchMerger(packAttrConfig);
    
    PartitionDataPtr partData =
        PartitionDataMaker::CreateSimplePartitionData(GET_FILE_SYSTEM());

    SegmentMergeInfos segmentMergeInfos = MakeSegmentMergeInfos("1,2,3");
    DirectoryPtr attrDir = partDir->MakeDirectory("segment_4_level_0/attribute/packAttr");
    
    dedupPatchMerger.Merge(partData, segmentMergeInfos, attrDir);
    
    ASSERT_TRUE(attrDir->IsExist("string2/2_0.patch"));
    ASSERT_TRUE(attrDir->IsExist("price/2_0.patch"));
    ASSERT_TRUE(attrDir->IsExist("name/3_0.patch"));
    
}

SegmentMergeInfos DedupPackAttributePatchFileMergerTest::MakeSegmentMergeInfos(
        const string& segmentIds)
{
    vector<segmentid_t> segIds;
    StringUtil::fromString(segmentIds, segIds, ",");
    SegmentMergeInfos segMergeInfos;
    for (size_t i = 0; i < segIds.size(); ++i)
    {
        SegmentMergeInfo segMergeInfo;
        segMergeInfo.segmentId = segIds[i];
        segMergeInfos.push_back(segMergeInfo);
    }
    return segMergeInfos;
}


IE_NAMESPACE_END(index);

