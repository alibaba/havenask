#pragma once

#include "indexlib/common_define.h"
#include "indexlib/index_base/index_meta/merge_task_resource.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index_base {

class MergeTaskResourceTest : public INDEXLIB_TESTBASE
{
public:
    MergeTaskResourceTest();
    ~MergeTaskResourceTest();

    DECLARE_CLASS_NAME(MergeTaskResourceTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(MergeTaskResourceTest, TestSimpleProcess);
}} // namespace indexlib::index_base
