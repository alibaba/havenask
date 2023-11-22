#pragma once

#include "indexlib/common_define.h"
#include "indexlib/merger/merge_plan_meta.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace merger {

class MergePlanMetaTest : public INDEXLIB_TESTBASE
{
public:
    MergePlanMetaTest();
    ~MergePlanMetaTest();

    DECLARE_CLASS_NAME(MergePlanMetaTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestStoreAndLoad();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(MergePlanMetaTest, TestStoreAndLoad);
}} // namespace indexlib::merger
