#pragma once

#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/dedup_pack_attribute_patch_file_merger.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class DedupPackAttributePatchFileMergerTest : public INDEXLIB_TESTBASE
{
public:
    DedupPackAttributePatchFileMergerTest();
    ~DedupPackAttributePatchFileMergerTest();

    DECLARE_CLASS_NAME(DedupPackAttributePatchFileMergerTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    index_base::SegmentMergeInfos MakeSegmentMergeInfos(const std::string& segmentIds);

private:
    test::PartitionStateMachinePtr mPsm;
    std::string mRootDir;
    config::IndexPartitionSchemaPtr mSchema;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(DedupPackAttributePatchFileMergerTest, TestSimpleProcess);
}} // namespace indexlib::index
