#include "indexlib/partition/test/partition_counter_perf_test.h"
#include "indexlib/util/counter/accumulative_counter.h"
#include "indexlib/util/counter/counter_map.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/config/index_partition_options.h"
#include <autil/Thread.h>
#include <autil/TimeUtility.h>
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(config);

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, PartitionCounterPerfTest);

PartitionCounterPerfTest::PartitionCounterPerfTest()
{
}

PartitionCounterPerfTest::~PartitionCounterPerfTest()
{
}

void PartitionCounterPerfTest::CaseSetUp()
{
    for (size_t i = 0; i < 10; ++i)
    {
        string fieldName = "field" + StringUtil::toString(i);
        AccumulativeCounterPtr accCounter(new AccumulativeCounter(fieldName));
        mTestAccessCounters[fieldName] = accCounter;
        mCompAccessCounters[fieldName] = AtomicCounter();
        mFieldNames.push_back(fieldName);
    }

    mSingleAccCounter.reset(new AccumulativeCounter("test"));
    
    mRootDir = GET_TEST_DATA_PATH();
    string field = "pk:string:pk;string1:string;text1:text;"
                   "long1:uint32;long2:uint32;long3:uint32;long4:uint32;long5:uint32;long6:uint32;long7:uint32;long8:uint32;"
                   "multi_long:uint32:true;"
                   "updatable_multi_long:uint32:true:true;";
    string index = "pk:primarykey64:pk;index1:string:string1;pack1:pack:text1;";
    string attr = "long1;long2;long3;long4;long7;long8;multi_long;updatable_multi_long;packAttr1:long5,long6";
    string summary = "string1";

    mSchema = SchemaMaker::MakeSchema(field, index, 
            attr, summary);
    mOptions = IndexPartitionOptions();

    mPsm.reset(new PartitionStateMachine());
    ASSERT_TRUE(mPsm->Init(mSchema, mOptions, mRootDir));
}

void PartitionCounterPerfTest::CaseTearDown()
{
}

void PartitionCounterPerfTest::UpdateCounters(bool useTestCounters, int64_t duration)
{
    while (!mIsRun) { usleep(0); }
    int64_t loopCnt = 0;

    while (!mIsFinish)
    {
        if (useTestCounters)
        {
            for (const auto& field : mFieldNames)
            {
                mTestAccessCounters[field]->Increase(1);
            }
        }
        else
        {
            for (const auto& field : mFieldNames)
            {
                mCompAccessCounters[field].inc();
            }
        }
        loopCnt += 1;
    }

    printf("thread qps : %f\n", double(loopCnt)/duration);
}

void PartitionCounterPerfTest::TestMultiThreadUpdateCounters()
{
    size_t readThreadNum = 4;
    int64_t duration = 20;

    {
        mIsFinish = false;
        mIsRun = false;
        cout << "test autil Atomic Counters " << endl;
        vector<ThreadPtr> readThreads;
        for (size_t i = 0; i < readThreadNum; ++i)
        {
            ThreadPtr thread = Thread::createThread(
                    tr1::bind(&PartitionCounterPerfTest::UpdateCounters, this, false, duration));
            readThreads.push_back(thread);
        }

        mIsRun = true;
        int64_t beginTime = TimeUtility::currentTimeInSeconds();
        int64_t endTime = beginTime;
        while (endTime - beginTime < duration)
        {
            sleep(1);
            endTime = TimeUtility::currentTimeInSeconds();
        }
        mIsFinish = true;

        for (size_t i = 0; i < readThreadNum; ++i)
        {
            readThreads[i].reset();
        }            
    }

    {
        cout << "test Accumulative Counters " << endl;
        vector<ThreadPtr> readThreads;
        mIsFinish = false;
        mIsRun = false;
        for (size_t i = 0; i < readThreadNum; ++i)
        {
            ThreadPtr thread = Thread::createThread(
                    tr1::bind(&PartitionCounterPerfTest::UpdateCounters, this, true, duration));
            readThreads.push_back(thread);
        }

        mIsRun = true;
        int64_t beginTime = TimeUtility::currentTimeInSeconds();
        int64_t endTime = beginTime;
        while (endTime - beginTime < duration)
        {
            sleep(1);
            endTime = TimeUtility::currentTimeInSeconds();
        }
        mIsFinish = true;

        for (size_t i = 0; i < readThreadNum; ++i)
        {
            readThreads[i].reset();
        }            
    }    
}

void PartitionCounterPerfTest::Query(int64_t duration)
{
    while (!mIsRun) { usleep(0); }
    int64_t loopCnt = 0;

    while (!mIsFinish)
    {
        mPsm->Transfer(QUERY, "", "index1:hello", "docid=0;docid=1;docid=2;docid=3;docid=4;docid=5");
        loopCnt ++;
    }    
    printf("thread qps : %f\n", double(loopCnt)/duration);
}

void PartitionCounterPerfTest::TestMultiThreadReadWithAccessCounters()
{
    size_t readThreadNum = 4;
    int64_t duration = 20;

    assert(mPsm);
    string fullDocStr = "cmd=add,pk=1,string1=hello,ts=1;" 
                        "cmd=add,pk=2,string1=hello,ts=1;" 
                        "cmd=add,pk=3,string1=hello,ts=1;" 
                        "cmd=add,pk=4,string1=hello,ts=1;" 
                        "cmd=add,pk=5,string1=hello,ts=1;"; 

    ASSERT_TRUE(mPsm->Transfer(BUILD_FULL, fullDocStr, "", ""));
    string rtDocStr = "cmd=add,pk=6,string1=hello,ts=1;"; 
    ASSERT_TRUE(mPsm->Transfer(BUILD_RT, rtDocStr, "", "")); 
    
    {
        mIsFinish = false;
        mIsRun = false;
        cout << "test query update access counters" << endl;
        vector<ThreadPtr> readThreads;
        for (size_t i = 0; i < readThreadNum; ++i)
        {
            ThreadPtr thread = Thread::createThread(
                    tr1::bind(&PartitionCounterPerfTest::Query, this, duration));
            readThreads.push_back(thread);
        }

        mIsRun = true;
        int64_t beginTime = TimeUtility::currentTimeInSeconds();
        int64_t endTime = beginTime;
        while (endTime - beginTime < duration)
        {
            sleep(1);
            endTime = TimeUtility::currentTimeInSeconds();
        }
        mIsFinish = true;

        for (size_t i = 0; i < readThreadNum; ++i)
        {
            readThreads[i].reset();
        }            
    }

    auto indexPartition = mPsm->GetIndexPartition();
    auto counterMap = indexPartition->GetCounterMap();
    ASSERT_TRUE(counterMap);
    cout << counterMap->ToJsonString() << endl;
}

IE_NAMESPACE_END(partition);

