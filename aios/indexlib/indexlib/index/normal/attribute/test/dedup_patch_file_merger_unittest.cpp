#include <autil/StringUtil.h>
#include "indexlib/index/normal/attribute/test/dedup_patch_file_merger_unittest.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_patch_merger.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/file_system/directory_creator.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, DedupPatchFileMergerTest);

DedupPatchFileMergerTest::DedupPatchFileMergerTest()
{
}

DedupPatchFileMergerTest::~DedupPatchFileMergerTest()
{
}

void DedupPatchFileMergerTest::CaseSetUp()
{
    string field = "pk:string;long1:uint32;multi_long:uint64:true:true;";
    string index = "pk:primarykey64:pk;";
    string attr = "long1;multi_long";

    mSchema = SchemaMaker::MakeSchema(field, index, attr, "");
    mRootDir = GET_TEST_DATA_PATH();

    IndexPartitionOptions options;
    MergeConfig& mergeConfig = options.GetMergeConfig();
    mergeConfig.mergeStrategyStr = OPTIMIZE_MERGE_STRATEGY_STR;
    mergeConfig.mergeStrategyParameter.SetLegacyString("max-doc-count=1");

    mPsm.reset(new PartitionStateMachine());
    mPsm->Init(mSchema, options, mRootDir);

    mAttrConfig = mSchema->GetAttributeSchema()
        ->GetAttributeConfig("long1");
}

void DedupPatchFileMergerTest::CaseTearDown()
{
    mPsm.reset();
}

void DedupPatchFileMergerTest::TestNotCollectPatchForDstSegIdInMergeInfos()
{
//    this case test dedupPatchFileMerger.collectPatchFiles() will not 
//    collect patch files whose dst segment_id belongs to the merging segments.
//    case generate 3 segments:
//    s0: 2 docs 
//    s1: 1 doc with 1_0.patch
//    s2: 1 doc with 2_0.patch and 2_1.patch
//    case then merge s1 and s2,  and expect 2_1.patch will not be collected.

    string seg0Docs = "cmd=add,pk=0,long1=0;cmd=add,pk=2;";
    ASSERT_TRUE(mPsm->Transfer(BUILD_INC_NO_MERGE, seg0Docs, "", ""));
    string seg1Docs = "cmd=update_field,pk=0,long1=1;cmd=add,pk=3;;";
    ASSERT_TRUE(mPsm->Transfer(BUILD_INC_NO_MERGE, seg1Docs, "", ""));
    string seg2Docs = "cmd=update_field,pk=0,long1=2;cmd=update_field,pk=3,long1=2;cmd=add,pk=5;";
    ASSERT_TRUE(mPsm->Transfer(BUILD_INC_NO_MERGE, seg2Docs, "", ""));

    SegmentMergeInfos segMergeInfos = MakeSegmentMergeInfos("1,2");
    AttrPatchInfos patchInfos;

    DedupPatchFileMerger::CollectPatchFiles(
        mAttrConfig, GetPartitionData(), segMergeInfos, patchInfos);
    ASSERT_EQ((size_t)1, patchInfos.size());
    ASSERT_EQ((size_t)2, patchInfos[0].size());
    ASSERT_EQ((string)"1_0.patch", patchInfos[0][0].patchFileName);
    ASSERT_EQ((string)"2_0.patch", patchInfos[0][1].patchFileName);
}

void DedupPatchFileMergerTest::TestNotCollectPatchForDstSegIdNotInVersion()
{
//     this case test dedupPatchFileMerger.collectPatchFiles() will not 
//     collect patch files whose dst segment_id does not belong to current
//     version
//     case generate 4 segments:
//         0: 2 docs
//         1: 1 doc
//         2: 2 docs with 2_0.patch and 2_1.patch
//         3: merged from s1
//     case then merge s2 and s3 , and expect only 2_0.patch to be collected 


    string seg0Docs = "cmd=add,pk=0,long1=0;cmd=add,pk=1;";
    ASSERT_TRUE(mPsm->Transfer(BUILD_INC_NO_MERGE, seg0Docs, "", ""));
    string seg1Docs = "cmd=add,pk=2;";
    ASSERT_TRUE(mPsm->Transfer(BUILD_INC_NO_MERGE, seg1Docs, "", ""));
    string seg2Docs = "cmd=update_field,pk=0,long1=2;cmd=update_field,pk=2,long1=2;"
                      "cmd=add,pk=5;cmd=add,pk=6";
    ASSERT_TRUE(mPsm->Transfer(BUILD_INC, seg2Docs, "", ""));
    SegmentMergeInfos segMergeInfos = MakeSegmentMergeInfos("2,3");

    AttrPatchInfos patchInfos;
    DedupPatchFileMerger::CollectPatchFiles(
        mAttrConfig, GetPartitionData(), segMergeInfos, patchInfos);
    
    ASSERT_EQ((size_t)1, patchInfos.size());
    ASSERT_EQ((size_t)1, patchInfos[0].size());
    ASSERT_EQ((string)"2_0.patch", patchInfos[0][0].patchFileName);
}

void DedupPatchFileMergerTest::TestCollectPatchForSegIdBetweenMergingSegId()
{
//    the case test dedupPatchFileMerger.collectPatchFiles() will collect 
//    patch files from segments whose segId is between the merging segments' id
//    f.g. merge s1 ~ s3 will collect patch files from s2 
//    however, if a segment dose not belong to the current version, its patch 
//    files will not be collected.
//    the case generate 5 segments:
//    s0: 2 docs
//    s1: 2 docs with 1_0.patch
//    s2: 2 docs with 2_0.patch
//    s3: 1 docs with 3_0.patch
//    s4: 2 docs with 4_0.patch
//    case then merge s3 to s5, which makes s3 not belong to the current version.
//    after that, case merge s1 and s4, and expect 2_0.patch 3_0.patch will be collected.
//    To be noted, case use unorderd SegmentMergeInfos and expect correct results

    string seg0Docs = "cmd=add,pk=0,long1=0;cmd=add,pk=1;";
    ASSERT_TRUE(mPsm->Transfer(BUILD_INC_NO_MERGE, seg0Docs, "", ""));
    string seg1Docs = "cmd=update_field,pk=0,long1=1;cmd=add,pk=2;cmd=add,pk=3;";
    ASSERT_TRUE(mPsm->Transfer(BUILD_INC_NO_MERGE, seg1Docs, "", ""));
    string seg2Docs = "cmd=update_field,pk=0,long1=2;cmd=add,pk=4;cmd=add,pk=5;";
    ASSERT_TRUE(mPsm->Transfer(BUILD_INC_NO_MERGE, seg2Docs, "", ""));
    string seg3Docs = "cmd=update_field,pk=0,long1=3;cmd=add,pk=6;";
    ASSERT_TRUE(mPsm->Transfer(BUILD_INC_NO_MERGE, seg3Docs, "", ""));
    string seg4Docs = "cmd=update_field,pk=0,long1=4;cmd=add,pk=7;cmd=add,pk=8;";
    ASSERT_TRUE(mPsm->Transfer(BUILD_INC, seg4Docs, "", ""));

    SegmentMergeInfos unorderSegMergeInfos = MakeSegmentMergeInfos("4,1");
    AttrPatchInfos patchInfos;
    DedupPatchFileMerger::CollectPatchFiles(
        mAttrConfig, GetPartitionData(), unorderSegMergeInfos, patchInfos);
    
    ASSERT_EQ((size_t)1, patchInfos.size());
    ASSERT_EQ((size_t)4, patchInfos[0].size());
    ASSERT_EQ((string)"1_0.patch", patchInfos[0][0].patchFileName);
    ASSERT_EQ((string)"2_0.patch", patchInfos[0][1].patchFileName);
    ASSERT_EQ((string)"4_0.patch", patchInfos[0][2].patchFileName);
    ASSERT_EQ((string)"3_0.patch", patchInfos[0][3].patchFileName);
}

namespace {
class MockAttributePatchMerger : public SingleValueAttributePatchMerger<uint32_t>
{
public:
    MockAttributePatchMerger(const AttributeConfigPtr& attrConf = AttributeConfigPtr())
        : SingleValueAttributePatchMerger<uint32_t>(attrConf){}
    ~MockAttributePatchMerger() {}
public:
    MOCK_METHOD2(Merge, void (const AttrPatchFileInfoVec&, const FileWriterPtr&));
};
DEFINE_SHARED_PTR(MockAttributePatchMerger);

class MockDedupPatchFileMerger : public DedupPatchFileMerger
{
public:
    MockDedupPatchFileMerger(const config::AttributeConfigPtr& attrConfig)
        : DedupPatchFileMerger(attrConfig) {}
    ~MockDedupPatchFileMerger() {}

public:
    MOCK_CONST_METHOD1(CreateAttributePatchMerger, AttributePatchMergerPtr(segmentid_t));
};

}

void DedupPatchFileMergerTest::TestMergePatchFiles()
{
    AttrPatchInfos attrPatchInfos = MakePatchInfos("0,2_0#1,4_1,2_1");

    AttributeConfigPtr attrConfig(new AttributeConfig());
    attrConfig->Init(FieldConfigPtr(new FieldConfig("name", FieldType::ft_string, false)));
    MockDedupPatchFileMerger dedupPatchFileMerger(attrConfig);

    MockAttributePatchMergerPtr attributePatchMerger(new MockAttributePatchMerger(attrConfig));
    EXPECT_CALL(dedupPatchFileMerger, CreateAttributePatchMerger(_))
        .WillOnce(Return(attributePatchMerger))
        .WillOnce(Return(attributePatchMerger));
    
    EXPECT_CALL(*attributePatchMerger, Merge(attrPatchInfos[0], _))
        .WillOnce(Return());
    EXPECT_CALL(*attributePatchMerger, Merge(attrPatchInfos[1], _))
        .WillOnce(Return());

    dedupPatchFileMerger.DoMergePatchFiles(
        attrPatchInfos, GET_PARTITION_DIRECTORY()->MakeDirectory("attribute"));
    
    DirectoryPtr attrDirectory = GET_PARTITION_DIRECTORY()->GetDirectory("attribute", true);
    ASSERT_TRUE(attrDirectory->IsExist("2_0.patch"));
    ASSERT_TRUE(attrDirectory->IsExist("4_1.patch"));
}

// despStr: 0,2_0,3_0#1,3_1,4_1
AttrPatchInfos DedupPatchFileMergerTest::MakePatchInfos(const string& despStr)
{
    AttrPatchInfos attrPatchInfos;
    vector<vector<string> > patchInfoVec;
    StringUtil::fromString(despStr, patchInfoVec, ",", "#");
    for (size_t i = 0; i< patchInfoVec.size(); ++i) 
    {
        assert(patchInfoVec[i].size() > 1);
        segmentid_t dstSegId = 
            StringUtil::fromString<segmentid_t>(patchInfoVec[i][0]);

        AttrPatchFileInfoVec patchInfos;
        for (size_t j = 1; j < patchInfoVec[i].size(); ++j)
        {
            PatchFileInfo patchFileInfo;
            patchFileInfo.patchFileName = patchInfoVec[i][j];
            patchFileInfo.srcSegment = 
                StringUtil::fromString<segmentid_t>(patchInfoVec[i][j]);
            patchFileInfo.patchDirectory = GET_PARTITION_DIRECTORY()->MakeDirectory("attribute");
            patchFileInfo.patchDirectory->Store(patchInfoVec[i][j], "");
            patchInfos.push_back(patchFileInfo);
        }
        attrPatchInfos[dstSegId] = patchInfos;
    }
    return attrPatchInfos;
}

SegmentMergeInfos DedupPatchFileMergerTest::MakeSegmentMergeInfos(
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

PartitionDataPtr DedupPatchFileMergerTest::GetPartitionData()
{
    const partition::OnlinePartitionPtr indexPart = DYNAMIC_POINTER_CAST(OnlinePartition,
            mPsm->GetIndexPartition());
    return indexPart->GetPartitionData();
}


IE_NAMESPACE_END(index);

