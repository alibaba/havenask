#ifndef __INDEXLIB_DEDUPPATCHFILEMERGERTEST_H
#define __INDEXLIB_DEDUPPATCHFILEMERGERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/dedup_patch_file_merger.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class DedupPatchFileMergerTest : public INDEXLIB_TESTBASE
{
public:
    DedupPatchFileMergerTest();
    ~DedupPatchFileMergerTest();
    DECLARE_CLASS_NAME(DedupPatchFileMergerTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestNotCollectPatchForDstSegIdInMergeInfos();
    void TestNotCollectPatchForDstSegIdNotInVersion();
    void TestCollectPatchForSegIdBetweenMergingSegId();
    void TestMergePatchFiles();

private:
    index_base::SegmentMergeInfos MakeSegmentMergeInfos(const std::string& segmentIds);
    index_base::PartitionDataPtr GetPartitionData();
    // despStr: 0,2_0,3_0#1,3_1,4_1
    index_base::PatchInfos MakePatchInfos(const std::string& despStr);

private:
    test::PartitionStateMachinePtr mPsm;
    std::string mRootDir;
    config::IndexPartitionSchemaPtr mSchema;
    config::AttributeConfigPtr mAttrConfig;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(DedupPatchFileMergerTest, TestNotCollectPatchForDstSegIdInMergeInfos);
INDEXLIB_UNIT_TEST_CASE(DedupPatchFileMergerTest, TestNotCollectPatchForDstSegIdNotInVersion);
INDEXLIB_UNIT_TEST_CASE(DedupPatchFileMergerTest, TestCollectPatchForSegIdBetweenMergingSegId);
INDEXLIB_UNIT_TEST_CASE(DedupPatchFileMergerTest, TestMergePatchFiles);
}} // namespace indexlib::index

#endif //__INDEXLIB_DEDUPPATCHFILEMERGERTEST_H
