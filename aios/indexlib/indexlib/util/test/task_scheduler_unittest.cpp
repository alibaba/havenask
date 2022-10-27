#include "indexlib/util/test/task_scheduler_unittest.h"

using namespace std;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(util);
IE_LOG_SETUP(util, TaskSchedulerTest);


class SimpleTask : public TaskItem
{
public:
    SimpleTask(vector<uint32_t>& outputVec, uint32_t outputIdx)
        : mOutputVec(outputVec)
        , mOutputIdx(outputIdx)
    {}
    ~SimpleTask() {}
public:
    void Run()
    {
        mOutputVec[mOutputIdx]++;
    }
private:
    vector<uint32_t>& mOutputVec;
    uint32_t mOutputIdx;
};

TaskSchedulerTest::TaskSchedulerTest()
{
}

TaskSchedulerTest::~TaskSchedulerTest()
{
}

void TaskSchedulerTest::CaseSetUp()
{
}

void TaskSchedulerTest::CaseTearDown()
{
}

void TaskSchedulerTest::TestSimpleProcess()
{
    TaskScheduler ts;
    ASSERT_TRUE(ts.DeclareTaskGroup("test", 1 * 1000000)); // task interval: 1 second
    ASSERT_TRUE(ts.DeclareTaskGroup("test1", 4 * 1000000)); // task interval: 4 second

    size_t taskCount = 5;
    vector<uint32_t> taskIdVec;
    vector<uint32_t> taskOutput(taskCount * 2, 0);
    
    for (size_t i = 0; i < taskCount; ++i)
    {
        TaskItemPtr taskPtr(new SimpleTask(taskOutput, i));
        int32_t taskId = ts.AddTask("test", taskPtr);
        ASSERT_TRUE(taskId != TaskScheduler::INVALID_TASK_ID);
        taskIdVec.push_back(taskId);
        taskPtr.reset(new SimpleTask(taskOutput, i + 5));
        taskId = ts.AddTask("test1", taskPtr);
        ASSERT_TRUE(taskId != TaskScheduler::INVALID_TASK_ID);
        taskIdVec.push_back(taskId);
    }

    sleep(5);
    // todo: assert thread count is 10
    for (size_t i = 0; i < taskIdVec.size(); ++i)
    {
        ASSERT_TRUE(ts.DeleteTask(taskIdVec[i]));
    }

    for (size_t i = 0; i < taskCount; ++i)
    {
        ASSERT_TRUE(taskOutput[i] >= 4) << taskOutput[i];
        ASSERT_TRUE(taskOutput[i] <= 6) << taskOutput[i];
    }

    for (size_t i = taskCount; i < taskCount * 2; ++i)
    {
        ASSERT_TRUE(taskOutput[i] >= 1) << taskOutput[i];
        ASSERT_TRUE(taskOutput[i] <= 2) << taskOutput[i];        
    }
}

void TaskSchedulerTest::TestAddTaskException()
{
    TaskScheduler ts;
    ASSERT_TRUE(ts.DeclareTaskGroup("test", 1 * 1000000)); // task interval: 1 second
    ASSERT_FALSE(ts.DeclareTaskGroup("test", 2 * 1000000)); // fail due to different time interval setting
    vector<uint32_t> taskOutput(20);
    TaskItemPtr task(new SimpleTask(taskOutput, 0));

    ASSERT_TRUE(TaskScheduler::INVALID_TASK_ID ==  ts.AddTask("non-exist-task-group-name", task));
    int32_t taskId0 = ts.AddTask("test", task);
    ASSERT_TRUE(taskId0 != TaskScheduler::INVALID_TASK_ID);

    int32_t non_exist_task_id = 65535;
    ASSERT_FALSE(ts.DeleteTask(non_exist_task_id));
    ASSERT_TRUE(ts.DeleteTask(taskId0));
    ASSERT_FALSE(ts.DeleteTask(taskId0));
}


IE_NAMESPACE_END(util);

