#include "indexlib/table/test/merge_task_dispatcher_unittest.h"
#include "indexlib/table/merge_task_description.h"

using namespace std;
using testing::UnorderedElementsAreArray;

IE_NAMESPACE_BEGIN(table);
IE_LOG_SETUP(table, MergeTaskDispatcherTest);

MergeTaskDispatcherTest::MergeTaskDispatcherTest()
{
}

MergeTaskDispatcherTest::~MergeTaskDispatcherTest()
{
}

void MergeTaskDispatcherTest::CaseSetUp()
{
}

void MergeTaskDispatcherTest::CaseTearDown()
{
}

void MergeTaskDispatcherTest::TestSimpleProcess()
{
    // empty MergePlan
    VerifyDispatch(3, {}, {});

    // TaskDesc.size() < instanceCount
    VerifyDispatch(3, {{10},{20}}, {
            {{1,0,20}},
            {{0,0,10}},
            {}
        });

    // TaskDesc.size() == instanceCount
    VerifyDispatch(3, {{30,10},{20}}, {
            {{1,0,20}},
            {{0,1,10}},
            {{0,0,30}}
        });
    
    // TaskDesc.size() > instanceCount, in ascending order
    VerifyDispatch(3, {{10,20},{30,40,50}}, {
            {{1,2,50}},
            {{1,1,40}, {0,0,10}},
            {{1,0,30}, {0,1,20}}
        });
    
    // TaskDesc.size() > instanceCount, in descending order
    VerifyDispatch(3, {{50,40},{30,20},{10}}, {
            {{0,0,50}},
            {{0,1,40}, {2,0,10}},
            {{1,0,30}, {1,1,20}}
        });

    // TaskDesc.size() > instanceCount, in random order
    VerifyDispatch(3, {{50,10},{30},{20,40}}, {
            {{0,0,50}},
            {{2,1,40}, {0,1,10}},
            {{1,0,30}, {2,0,20}}
        });
    VerifyDispatch(3, {{30},{10,20,50,40}}, {
            {{1,2,50}},
            {{1,3,40}, {1,0,10}},
            {{0,0,30}, {1,1,20}}
        });
    
    // all cost = 0
    VerifyDispatch(5, {{0,0},{0,0},{0}}, {
            {{0,0,0}},
            {{0,1,0}},
            {{1,0,0}}, 
            {{1,1,0}}, 
            {{2,0,0}}, 
        });

    // some cost = 0
    VerifyDispatch(3, {{10,20},{30,40,50,0}}, {
            {{1,2,50}, {1,3,0}},
            {{1,1,40}, {0,0,10}},
            {{1,0,30}, {0,1,20}}
        });
}

/**
 * @param planCosts = { plan0{ task0-cost, task1-cost }, plan1{}, ... }
 * @param expectExecMetas = { instance0{ execMeta0{planIdx, taskIdx, cost}, execMeta1{}, ... }, instance1{}, ... }
 */
void MergeTaskDispatcherTest::VerifyDispatch(
    uint32_t instanceCount,
    const vector<vector<uint32_t>>& planCosts,
    const vector<vector<vector<uint32_t>>>& expectExecMetas)
{
    MergeTaskDispatcher dispatcher;
    
    vector<MergeTaskDescriptions> allTaskDescriptions;
    allTaskDescriptions.reserve(planCosts.size());
    for (const auto& taskCosts: planCosts)
    {
        MergeTaskDescriptions planTaskDescriptions;
        planTaskDescriptions.reserve(taskCosts.size());
        for (const auto& cost: taskCosts)
        {
            MergeTaskDescription desc;
            desc.cost = cost;
            planTaskDescriptions.push_back(desc);
        }
        allTaskDescriptions.push_back(planTaskDescriptions);
    }
    
    vector<TaskExecuteMetas> execMetas = dispatcher.DispatchMergeTasks(allTaskDescriptions, instanceCount);
    vector<vector<vector<uint32_t>>> actual;
    actual.reserve(execMetas.size());
    for (const TaskExecuteMetas& taskExecuteMetas: execMetas)
    {
        vector<vector<uint32_t>> tmp;
        tmp.reserve(taskExecuteMetas.size());
        for (const TaskExecuteMeta& meta: taskExecuteMetas)
        {
            tmp.push_back({meta.planIdx, meta.taskIdx, meta.cost});
        }
        actual.push_back(tmp);
    }
    
    ASSERT_EQ(expectExecMetas.size(), execMetas.size());
    EXPECT_THAT(actual, UnorderedElementsAreArray(expectExecMetas));
}

IE_NAMESPACE_END(table);

