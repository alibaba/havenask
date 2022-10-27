#ifndef __INDEXLIB_PARTITIONPATCHMETATEST_H
#define __INDEXLIB_PARTITIONPATCHMETATEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index_base/patch/partition_patch_meta.h"

IE_NAMESPACE_BEGIN(index_base);

class PartitionPatchMetaTest : public INDEXLIB_TESTBASE
{
public:
    PartitionPatchMetaTest();
    ~PartitionPatchMetaTest();

    DECLARE_CLASS_NAME(PartitionPatchMetaTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(PartitionPatchMetaTest, TestSimpleProcess);

IE_NAMESPACE_END(index_base);

#endif //__INDEXLIB_PARTITIONPATCHMETATEST_H
