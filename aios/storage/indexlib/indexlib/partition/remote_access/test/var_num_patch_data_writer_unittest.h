#ifndef __INDEXLIB_VARNUMPATCHDATAWRITERTEST_H
#define __INDEXLIB_VARNUMPATCHDATAWRITERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/partition/remote_access/var_num_patch_data_writer.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace partition {

class VarNumPatchDataWriterTest : public INDEXLIB_TESTBASE
{
public:
    VarNumPatchDataWriterTest();
    ~VarNumPatchDataWriterTest();

    DECLARE_CLASS_NAME(VarNumPatchDataWriterTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(VarNumPatchDataWriterTest, TestSimpleProcess);
}} // namespace indexlib::partition

#endif //__INDEXLIB_VARNUMPATCHDATAWRITERTEST_H
