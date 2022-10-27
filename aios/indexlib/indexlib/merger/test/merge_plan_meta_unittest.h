#ifndef __INDEXLIB_MERGEPLANMETATEST_H
#define __INDEXLIB_MERGEPLANMETATEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/merger/merge_plan_meta.h"

IE_NAMESPACE_BEGIN(merger);

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

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_MERGEPLANMETATEST_H
