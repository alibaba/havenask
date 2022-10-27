#ifndef __INDEXLIB_MERGETASKRESOURCETEST_H
#define __INDEXLIB_MERGETASKRESOURCETEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index_base/index_meta/merge_task_resource.h"

IE_NAMESPACE_BEGIN(index_base);

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

IE_NAMESPACE_END(index_base);

#endif //__INDEXLIB_MERGETASKRESOURCETEST_H
