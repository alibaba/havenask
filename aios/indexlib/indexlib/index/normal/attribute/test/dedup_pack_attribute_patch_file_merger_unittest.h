#ifndef __INDEXLIB_DEDUPPACKATTRIBUTEPATCHFILEMERGERTEST_H
#define __INDEXLIB_DEDUPPACKATTRIBUTEPATCHFILEMERGERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/attribute/accessor/dedup_pack_attribute_patch_file_merger.h"
#include "indexlib/test/partition_state_machine.h"

IE_NAMESPACE_BEGIN(index);

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

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_DEDUPPACKATTRIBUTEPATCHFILEMERGERTEST_H
