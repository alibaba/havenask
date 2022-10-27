#include <autil/Thread.h>
#include <autil/Lock.h>
#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/file_system/async_dump_scheduler.h"


using namespace std;

IE_NAMESPACE_BEGIN(file_system);

class FakeDumpTask : public Dumpable
{
public:
    FakeDumpTask(autil::ThreadCond* cond) 
        : mDumped(false)
        , mDumping(false)
        , mCond(cond)
    {}
    virtual ~FakeDumpTask() {}    

public:
    void Dump() override
    {
        mDumping = true;
        mCond->wait();
        mDumped = true;
    }

    bool IsDumped() const {return mDumped;}
    bool IsDumping() const {return mDumping;}

private:
    bool mDumped;
    bool mDumping;
    autil::ThreadCond* mCond;
};

class AsyncDumpSchedulerTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(AsyncDumpSchedulerTest);
    void CaseSetUp() override
    {
        mDumpScheduler = AsyncDumpSchedulerPtr(new AsyncDumpScheduler);
    }

    void CaseTearDown() override
    {
        mDumpScheduler->WaitFinished();
        if (mWaitDumpThread)
        {
            mWaitDumpThread->join();
        }
    }

    void TestCaseForHasDumpTaskAndWaitDumpQueueEmpty()
    {
        INDEXLIB_TEST_TRUE(mDumpScheduler->HasDumpTask() == false);
        autil::ThreadCond cond;
        DumpablePtr dumpTask = DumpablePtr(new FakeDumpTask(&cond));
        mDumpScheduler->Push(dumpTask);
        INDEXLIB_TEST_TRUE(mDumpScheduler->HasDumpTask() == true);

        int waitFlag = 0, finishFlag = 0;

        mWaitDumpThread = autil::Thread::createThread(
                std::tr1::bind(&AsyncDumpSchedulerTest::WaitDumpFunc, this, 
                        &waitFlag, &finishFlag));
        while(waitFlag == 0)
        {
            usleep(200);
        }
        INDEXLIB_TEST_EQUAL(0, finishFlag);

        FakeDumpTask* fakeDumpTask = dynamic_cast<FakeDumpTask*>(dumpTask.get());
        while(!fakeDumpTask->IsDumping())
        {
            usleep(200);
        }

        INDEXLIB_TEST_TRUE(fakeDumpTask->IsDumped() == false);
        INDEXLIB_TEST_TRUE(mDumpScheduler->HasDumpTask() == true);
        INDEXLIB_TEST_EQUAL(0, finishFlag);

        cond.signal();
        
        while(!fakeDumpTask->IsDumped())
        {
            usleep(200);
        }

        INDEXLIB_TEST_TRUE(mDumpScheduler->HasDumpTask() == false);
        mWaitDumpThread->join();
        INDEXLIB_TEST_EQUAL(1, finishFlag);        
    }


private:
    void WaitDumpFunc(int* waitFlag, int* finishFlag)
    {
        *waitFlag = 1;
        mDumpScheduler->WaitDumpQueueEmpty();
        *waitFlag = 0;
        *finishFlag = 1;
    }

private:
    AsyncDumpSchedulerPtr mDumpScheduler;
    autil::ThreadPtr mWaitDumpThread;
};

INDEXLIB_UNIT_TEST_CASE(AsyncDumpSchedulerTest, TestCaseForHasDumpTaskAndWaitDumpQueueEmpty);

IE_NAMESPACE_END(file_system);
