#ifndef __INDEXLIB_MERGETASKRESOURCEMANAGERTEST_H
#define __INDEXLIB_MERGETASKRESOURCEMANAGERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index_base/merge_task_resource_manager.h"

IE_NAMESPACE_BEGIN(index_base);

class MergeTaskResourceManagerTest : public INDEXLIB_TESTBASE
{
public:
    MergeTaskResourceManagerTest();
    ~MergeTaskResourceManagerTest();

    DECLARE_CLASS_NAME(MergeTaskResourceManagerTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    
private:
    std::string MakeDataForResource(resourceid_t resId);
    void InnerTest();
    
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(MergeTaskResourceManagerTest, TestSimpleProcess);

IE_NAMESPACE_END(index_base);

#endif //__INDEXLIB_MERGETASKRESOURCEMANAGERTEST_H
