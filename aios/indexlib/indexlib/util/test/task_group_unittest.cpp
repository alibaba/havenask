#include "indexlib/util/test/task_group_unittest.h"

using namespace std;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(util);
IE_LOG_SETUP(util, TaskGroupTest);

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

TaskGroupTest::TaskGroupTest()
{
}

TaskGroupTest::~TaskGroupTest()
{
}

void TaskGroupTest::CaseSetUp()
{
}

void TaskGroupTest::CaseTearDown()
{
}

void TaskGroupTest::TestSimpleProcess()
{
    vector<uint32_t> output(20, 0);
    TaskGroup taskGroup("test", 1 * 1000000);
    ASSERT_FALSE(taskGroup.mBackGroundThreadPtr);
    ASSERT_FALSE(taskGroup.mRunning);

    ASSERT_EQ(1 * 1000000, taskGroup.GetTimeInterval());
    
    TaskItemPtr taskItem(new SimpleTask(output, 0));

    ASSERT_TRUE(taskGroup.AddTask(0, taskItem));

    ASSERT_TRUE(taskGroup.mBackGroundThreadPtr);
    ASSERT_TRUE(taskGroup.mRunning); 
    
    ASSERT_FALSE(taskGroup.AddTask(0, taskItem)); 
    ASSERT_FALSE(taskGroup.DeleteTask(1));
    ASSERT_TRUE(taskGroup.DeleteTask(0)); 
    ASSERT_FALSE(taskGroup.DeleteTask(0));

    ASSERT_EQ(1, taskGroup.mTaskItems.size());
    ASSERT_TRUE(taskGroup.AddTask(1, taskItem));
    ASSERT_EQ(1, taskGroup.mTaskItems.size());
    ASSERT_TRUE(taskGroup.DeleteTask(1)); 
}

IE_NAMESPACE_END(util);

