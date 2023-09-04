#include "indexlib/index/normal/attribute/test/updatable_var_num_attribute_merger_intetest.h"

#include "indexlib/index/normal/attribute/accessor/var_num_attribute_reader.h"
#include "indexlib/index_base/patch/patch_file_finder.h"
#include "indexlib/index_base/patch/patch_file_finder_creator.h"
#include "indexlib/partition/patch_loader.h"
#include "indexlib/test/partition_data_maker.h"
#include "indexlib/test/single_field_partition_data_provider.h"

using namespace std;
using namespace autil;
using namespace indexlib::test;
using namespace indexlib::file_system;
using namespace indexlib::partition;
using namespace indexlib::index_base;
using namespace indexlib::config;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, UpdatableVarNumAttributeMergerInteTest);

UpdatableVarNumAttributeMergerInteTest::UpdatableVarNumAttributeMergerInteTest() {}

UpdatableVarNumAttributeMergerInteTest::~UpdatableVarNumAttributeMergerInteTest() {}

void UpdatableVarNumAttributeMergerInteTest::CaseSetUp() {}

void UpdatableVarNumAttributeMergerInteTest::CaseTearDown() {}

void UpdatableVarNumAttributeMergerInteTest::TestMergePatches()
{
    string fieldTypeStr = MakeFieldTypeString(GET_PARAM_VALUE(0), GET_PARAM_VALUE(1));

    SingleFieldPartitionDataProvider provider;
    provider.Init(GET_TEMP_DATA_PATH(), fieldTypeStr, SFP_ATTRIBUTE);

    string docsStr = "0,1,2,3,4#5,6,7,update_field:0:8#"
                     "8,9,update_field:1:11,update_field:5:12,"
                     "update_field:0:13,update_field:7:1415";
    provider.Build(docsStr, SFP_OFFLINE);
    provider.Merge("1,2");

    AttributeConfigPtr attrConfig = provider.GetAttributeConfig();
    CheckResult(attrConfig, "13;11;2;3;4;12;6;14,15;8;9");
    CheckPatchFiles(attrConfig->GetAttrName(), 3, "2_0.patch");

    string incDocsStr = "update_field:0:1617,update_field:5:18,10,11#"
                        "update_field:10:15,update_field:6:23";
    provider.Build(incDocsStr, SFP_OFFLINE);
    CheckResult(attrConfig, "16,17;11;2;3;4;18;23;14,15;8;9;15;11");
    provider.Merge("3,4");
    CheckResult(attrConfig, "16,17;11;2;3;4;18;23;14,15;8;9;15;11");
    CheckPatchFiles(attrConfig->GetAttrName(), 6, "4_0.patch");
    CheckPatchFiles(attrConfig->GetAttrName(), 5, "5_3.patch,5_4.patch");

    provider.Merge("5,6");
    CheckResult(attrConfig, "16,17;11;2;3;4;18;23;14,15;8;9;15;11");
    CheckPatchFiles(attrConfig->GetAttrName(), 7, "4_0.patch");
}

void UpdatableVarNumAttributeMergerInteTest::TestMergeSegmentsWithPatch()
{
    string fieldTypeStr = MakeFieldTypeString(GET_PARAM_VALUE(0), GET_PARAM_VALUE(1));

    SingleFieldPartitionDataProvider provider;
    provider.Init(GET_TEMP_DATA_PATH(), fieldTypeStr, SFP_ATTRIBUTE);

    string docsStr = "0,1,2,3,4#5,6,7,update_field:0:8#"
                     "9,10,update_field:1:11,update_field:5:12,"
                     "update_field:0:13,update_field:7:1415";
    provider.Build(docsStr, SFP_OFFLINE);
    provider.Merge("0,1,2");

    AttributeConfigPtr attrConfig = provider.GetAttributeConfig();
    CheckResult(attrConfig, "13;11;2;3;4;12;6;14,15;9;10");
    CheckPatchFiles(attrConfig->GetAttrName(), 3, "");
}

void UpdatableVarNumAttributeMergerInteTest::TestMergeWithCompressOffset()
{
    string fieldTypeStr = MakeFieldTypeString(GET_PARAM_VALUE(0), GET_PARAM_VALUE(1));

    SingleFieldPartitionDataProvider provider;
    provider.Init(GET_TEMP_DATA_PATH(), fieldTypeStr, SFP_ATTRIBUTE);

    string docsStr = "0,1,2,3,4#5,6,7,update_field:0:8#"
                     "8,9,update_field:1:11,update_field:5:12,"
                     "update_field:0:13,update_field:7:1415";
    provider.Build(docsStr, SFP_OFFLINE);
    provider.Merge("1,2");
    GET_FILE_SYSTEM()->TEST_MountLastVersion();

    AttributeConfigPtr attrConfig = provider.GetAttributeConfig();

    if (GET_PARAM_VALUE(0)) {
        size_t expectFileLen = sizeof(uint32_t) * 2        // compress header
                               + sizeof(uint64_t)          // one slot item
                               + sizeof(uint64_t) * 2      // slot block header
                               + sizeof(uint8_t) * (5 + 1) // block item (5 doc + 1 guard)
                               + sizeof(uint32_t);         // magic tail

        CheckOffsetLength(attrConfig->GetAttrName(), 3, expectFileLen);
    }

    CheckResult(attrConfig, "13;11;2;3;4;12;6;14,15;8;9");
    CheckPatchFiles(attrConfig->GetAttrName(), 3, "2_0.patch");

    string incDocsStr = "update_field:0:1617,update_field:5:18,10,11#"
                        "update_field:10:15,update_field:6:23";
    provider.Build(incDocsStr, SFP_OFFLINE);
    GET_FILE_SYSTEM()->TEST_MountLastVersion();
    CheckResult(attrConfig, "16,17;11;2;3;4;18;23;14,15;8;9;15;11");

    provider.Merge("3,4");
    GET_FILE_SYSTEM()->TEST_MountLastVersion();

    if (GET_PARAM_VALUE(0)) {
        size_t expectFileLen = sizeof(uint32_t) * 2        // compress header
                               + sizeof(uint64_t)          // one slot item
                               + sizeof(uint64_t) * 2      // slot block header
                               + sizeof(uint8_t) * (7 + 1) // block item (7 doc + 1 guard)
                               + sizeof(uint32_t);         // magic tail

        CheckOffsetLength(attrConfig->GetAttrName(), 6, expectFileLen);
    }

    CheckResult(attrConfig, "16,17;11;2;3;4;18;23;14,15;8;9;15;11");
    CheckPatchFiles(attrConfig->GetAttrName(), 6, "4_0.patch");
    CheckPatchFiles(attrConfig->GetAttrName(), 5, "5_3.patch,5_4.patch");

    provider.Merge("5,6");
    GET_FILE_SYSTEM()->TEST_MountLastVersion();
    CheckResult(attrConfig, "16,17;11;2;3;4;18;23;14,15;8;9;15;11");
    CheckPatchFiles(attrConfig->GetAttrName(), 7, "4_0.patch");
}

void UpdatableVarNumAttributeMergerInteTest::CheckOffsetLength(const string& attrName, segmentid_t segId,
                                                               size_t expectOffsetLength)
{
    OnDiskPartitionDataPtr partitionData = PartitionDataMaker::CreateSimplePartitionData(GET_FILE_SYSTEM());

    SegmentData segData = partitionData->GetSegmentData(segId);
    file_system::DirectoryPtr segmentDirectory = segData.GetAttributeDirectory(attrName, true);
    ASSERT_TRUE(segmentDirectory);

    size_t offsetFileLen = segmentDirectory->GetFileLength(ATTRIBUTE_OFFSET_FILE_NAME);
    ASSERT_EQ(expectOffsetLength, offsetFileLen);
}

// patchFiles: patchFileName,patchFileName
void UpdatableVarNumAttributeMergerInteTest::CheckPatchFiles(const string& attrName, segmentid_t segId,
                                                             const string& expectPatchFiles)
{
    vector<string> patchFileNames;
    StringUtil::fromString(expectPatchFiles, patchFileNames, ",");

    OnDiskPartitionDataPtr partitionData = PartitionDataMaker::CreateSimplePartitionData(GET_FILE_SYSTEM());
    index_base::PatchFileFinderPtr patchFinder = PatchFileFinderCreator::Create(partitionData.get());

    PatchInfos attrPatchInfos;
    patchFinder->FindPatchFilesFromSegment(index_base::PatchFileFinder::PatchType::ATTRIBUTE, attrName, segId,
                                           INVALID_SCHEMA_OP_ID, &attrPatchInfos);

    size_t count = 0;
    PatchInfos::const_iterator iter;
    for (iter = attrPatchInfos.begin(); iter != attrPatchInfos.end(); iter++) {
        const PatchFileInfoVec& patchFileVec = iter->second;
        for (size_t i = 0; i < patchFileVec.size(); i++) {
            vector<string>::const_iterator fileIter;
            fileIter = find(patchFileNames.begin(), patchFileNames.end(), patchFileVec[i].patchFileName);
            ASSERT_TRUE(fileIter != patchFileNames.end());
            count++;
        }
    }
    ASSERT_EQ(count, patchFileNames.size());
}

void UpdatableVarNumAttributeMergerInteTest::CheckResult(const AttributeConfigPtr& attrConfig,
                                                         const string& expectResults)
{
    GET_FILE_SYSTEM()->TEST_MountLastVersion();
    PartitionDataPtr partitionData = PartitionDataMaker::CreateSimplePartitionData(GET_FILE_SYSTEM());

    VarNumAttributeReader<int32_t> attrReader;
    attrReader.Open(attrConfig, partitionData, PatchApplyStrategy::PAS_APPLY_NO_PATCH);
    PatchLoader::LoadAttributePatch(attrReader, attrConfig, partitionData);

    vector<vector<int32_t>> expectValues;
    StringUtil::fromString(expectResults, expectValues, ",", ";");

    for (docid_t i = 0; i < (docid_t)expectValues.size(); i++) {
        MultiValueType<int32_t> multiValue;
        attrReader.Read(i, multiValue);
        ASSERT_EQ((uint32_t)expectValues[i].size(), multiValue.size());

        for (size_t j = 0; j < multiValue.size(); j++) {
            ASSERT_EQ((int32_t)expectValues[i][j], multiValue[j]);
        }
    }
}

string UpdatableVarNumAttributeMergerInteTest::MakeFieldTypeString(bool isCompress, bool isUniq)
{
    string fieldTypeStr = "int32:true:true";
    if (isCompress && isUniq) {
        return fieldTypeStr + ":equal|uniq";
    }

    if (isCompress) {
        return fieldTypeStr + ":equal";
    }

    if (isUniq) {
        return fieldTypeStr + ":uniq";
    }
    return fieldTypeStr;
}
}} // namespace indexlib::index
