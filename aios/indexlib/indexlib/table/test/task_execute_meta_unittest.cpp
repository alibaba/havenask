#include "indexlib/table/test/task_execute_meta_unittest.h"

using namespace std;

IE_NAMESPACE_BEGIN(table);
IE_LOG_SETUP(table, TaskExecuteMetaTest);

TaskExecuteMetaTest::TaskExecuteMetaTest()
{
}

TaskExecuteMetaTest::~TaskExecuteMetaTest()
{
}

void TaskExecuteMetaTest::CaseSetUp()
{
}

void TaskExecuteMetaTest::CaseTearDown()
{
}

void TaskExecuteMetaTest::TestSimpleProcess()
{
    TaskExecuteMeta meta1(0,1,10);
    string identify = meta1.GetIdentifier();
    ASSERT_EQ(string("__merge_task_0#1"), identify);

    uint32_t planIdx, taskIdx;
    ASSERT_TRUE(meta1.GetIdxFromIdentifier(identify, planIdx, taskIdx));

    EXPECT_EQ(0u, planIdx);
    EXPECT_EQ(1u, taskIdx);

    string jsonStr = ToJsonString(meta1);

    TaskExecuteMeta meta2;
    FromJsonString(meta2, jsonStr);
    EXPECT_EQ(meta1, meta2);
}

IE_NAMESPACE_END(table);

