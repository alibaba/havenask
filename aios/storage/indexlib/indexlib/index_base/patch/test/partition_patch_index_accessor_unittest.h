#ifndef __INDEXLIB_PARTITIONPATCHINDEXACCESSORTEST_H
#define __INDEXLIB_PARTITIONPATCHINDEXACCESSORTEST_H

#include "indexlib/common_define.h"
#include "indexlib/index_base/patch/partition_patch_index_accessor.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index_base {

class PartitionPatchIndexAccessorTest : public INDEXLIB_TESTBASE
{
public:
    PartitionPatchIndexAccessorTest();
    ~PartitionPatchIndexAccessorTest();

    DECLARE_CLASS_NAME(PartitionPatchIndexAccessorTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestListPatchRootDirs();

private:
    void PrepareDirs(const std::string& dirStr);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(PartitionPatchIndexAccessorTest, TestListPatchRootDirs);
}} // namespace indexlib::index_base

#endif //__INDEXLIB_PARTITIONPATCHINDEXACCESSORTEST_H
