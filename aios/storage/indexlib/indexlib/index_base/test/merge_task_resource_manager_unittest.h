#pragma once

#include "indexlib/common_define.h"
#include "indexlib/index_base/merge_task_resource_manager.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index_base {

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
    void TestFence();

private:
    std::string MakeDataForResource(resourceid_t resId);
    void InnerTest(file_system::FenceContext* fenceContext);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(MergeTaskResourceManagerTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(MergeTaskResourceManagerTest, TestFence);

}} // namespace indexlib::index_base
