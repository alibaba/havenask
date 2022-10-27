#include "indexlib/partition/test/multi_partition_task_scheduler_intetest.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/util/task_scheduler.h"
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(index);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, MultiPartitionTaskSchedulerIntetest);

MultiPartitionTaskSchedulerIntetest::MultiPartitionTaskSchedulerIntetest()
{
}

MultiPartitionTaskSchedulerIntetest::~MultiPartitionTaskSchedulerIntetest()
{
}

void MultiPartitionTaskSchedulerIntetest::CaseSetUp()
{
    mTimestamp = 1;

    string field = "pk:string:pk;string1:string;long1:uint32;";
    string index = "pk:primarykey64:pk;index1:string:string1;";
    string attr = "long1;";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attr, "");

    IndexPartitionOptions options;
    options.GetOnlineConfig().onlineKeepVersionCount = 10;
    options.GetOfflineConfig().buildConfig.keepVersionCount = 100;
    BuildConfig& buildConfig = options.GetBuildConfig();
    buildConfig.maxDocCount = 10;

    mPartitionResource.taskScheduler.reset(new TaskScheduler);
    for (size_t i = 0; i < 50; i++)
    {
        PartitionStateMachinePtr stateMachine(
            new PartitionStateMachine(DEFAULT_MEMORY_USE_LIMIT,
                                      false, mPartitionResource));
        mPsms.push_back(stateMachine);
        string indexPath = GET_TEST_DATA_PATH() + "/" + StringUtil::toString(i);
        ASSERT_TRUE(stateMachine->Init(schema, options, indexPath));
        ASSERT_TRUE(stateMachine->Transfer(BUILD_RT, MakeDocStr(), "", ""));
    }
}

void MultiPartitionTaskSchedulerIntetest::CaseTearDown()
{
    for (size_t i = 0; i < mPsms.size(); i++)
    {
        mPsms[i].reset();
    }
}

void MultiPartitionTaskSchedulerIntetest::DoWrite()
{
    ThreadPtr rtThread = Thread::createThread(
            tr1::bind(&MultiPartitionTaskSchedulerIntetest::DoRtBuild, this));
    ThreadPtr incThread = Thread::createThread(
            tr1::bind(&MultiPartitionTaskSchedulerIntetest::DoIncBuild, this));

    incThread.reset();
    rtThread.reset();
}

void MultiPartitionTaskSchedulerIntetest::DoRtBuild()
{
    while (!IsFinished())
    {
        string docStr = MakeDocStr();
        PushDoc(docStr);
        for (size_t i = 0; i < mPsms.size(); i++)
        {
            {
                ScopedLock lock(mMachineLock);
                if (mPsms[i])
                {
                    ASSERT_TRUE(mPsms[i]->Transfer(BUILD_RT, docStr, "", ""));
                }
            }
        }
        usleep(5);
    }
}

void MultiPartitionTaskSchedulerIntetest::DoIncBuild()
{
    while (!IsFinished())
    {
        string docs = "";
        for (size_t i = 0; i < 10; ++i)
        {
            string doc = PopDoc();
            if (doc.empty())
            {
                continue;
            }
            docs += doc;
        }
        if (!docs.empty())
        {
            continue;
        }

        for (size_t i = 0; i < mPsms.size(); i++)
        {
            {
                ScopedLock lock(mMachineLock);
                if (mPsms[i])
                {
                    ASSERT_TRUE(mPsms[i]->Transfer(BUILD_INC, docs, "", ""));
                    usleep(10);
                }
            }
        }
    }
}

void MultiPartitionTaskSchedulerIntetest::DoRead(int* errCode)
{
    size_t taskCount = mPsms.size();
    std::vector<TaskScheduler::TaskGroupMetric> metrics;
    while(!IsFinished())
    {
        metrics.clear();
        mPartitionResource.taskScheduler->GetTaskGroupMetrics(metrics);
        for (size_t i = 0; i < metrics.size(); i++)
        {
            ASSERT_TRUE(metrics[i].tasksExecuteUseTime < 500000);
        }
        MEMORY_BARRIER();
        if (taskCount > 0 && mMachineLock.trylock() == 0)
        {
            mPsms[taskCount - 1].reset();
            taskCount--;
            mMachineLock.unlock();
        }
    }
}

string MultiPartitionTaskSchedulerIntetest::MakeDocStr()
{
    string tsStr = StringUtil::toString(mTimestamp++);
    return string("cmd=add,string1=hello,ts=") + tsStr + ",pk=" + tsStr + ";";
}

void MultiPartitionTaskSchedulerIntetest::TestSimpleProcess()
{
    DoMultiThreadTest(1, 5);
}

IE_NAMESPACE_END(partition);

