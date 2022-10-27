#include "indexlib/common/test/executor_scheduler_unittest.h"

using namespace std;

IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(common, ExecutorSchedulerTest);

class MockExecutor : public Executor
{
public:
    MOCK_METHOD0(Execute, void());
};
DEFINE_SHARED_PTR(MockExecutor);

ExecutorSchedulerTest::ExecutorSchedulerTest()
{
}

ExecutorSchedulerTest::~ExecutorSchedulerTest()
{
}

void ExecutorSchedulerTest::CaseSetUp()
{
}

void ExecutorSchedulerTest::CaseTearDown()
{
}

void ExecutorSchedulerTest::TestSimpleProcess()
{
    ExecutorScheduler scheduler;

    MockExecutorPtr executor1(new MockExecutor);
    EXPECT_CALL(*executor1, Execute())
        .Times(2);
    MockExecutorPtr executor2(new MockExecutor);
    EXPECT_CALL(*executor2, Execute())
        .WillOnce(Return());

    scheduler.Add(executor1, ExecutorScheduler::ST_REPEATEDLY);
    scheduler.Add(executor2, ExecutorScheduler::ST_ONCE);

    scheduler.Execute();
    scheduler.Execute();
}

IE_NAMESPACE_END(common);

