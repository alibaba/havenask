

#include "indexlib/file_system/flush/AsyncDumpScheduler.h"

#include <functional>
#include <iosfwd>
#include <memory>
#include <string>
#include <unistd.h>

#include "autil/Lock.h"
#include "autil/Thread.h"
#include "indexlib/file_system/flush/Dumpable.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;

namespace indexlib { namespace file_system {

class FakeDumpTask : public Dumpable
{
public:
    FakeDumpTask(autil::ThreadCond* cond) : _dumped(false), _dumping(false), _cond(cond) {}
    virtual ~FakeDumpTask() {}

public:
    void Dump() override
    {
        _dumping = true;
        _cond->wait();
        _dumped = true;
    }

    bool IsDumped() const { return _dumped; }
    bool IsDumping() const { return _dumping; }

private:
    bool _dumped;
    bool _dumping;
    autil::ThreadCond* _cond;
};

class AsyncDumpSchedulerTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(AsyncDumpSchedulerTest);
    void CaseSetUp() override
    {
        _dumpScheduler = AsyncDumpSchedulerPtr(new AsyncDumpScheduler);
        ASSERT_TRUE(_dumpScheduler->Init());
    }

    void CaseTearDown() override
    {
        _dumpScheduler->WaitFinished();
        if (_waitDumpThread) {
            _waitDumpThread->join();
        }
    }

    void TestCaseForHasDumpTaskAndWaitDumpQueueEmpty()
    {
        ASSERT_TRUE(_dumpScheduler->HasDumpTask() == false);
        autil::ThreadCond cond;
        cond.lock();
        DumpablePtr dumpTask = DumpablePtr(new FakeDumpTask(&cond));
        ASSERT_EQ(FSEC_OK, _dumpScheduler->Push(dumpTask));
        ASSERT_TRUE(_dumpScheduler->HasDumpTask() == true);

        int waitFlag = 0, finishFlag = 0;

        _waitDumpThread =
            autil::Thread::createThread(std::bind(&AsyncDumpSchedulerTest::WaitDumpFunc, this, &waitFlag, &finishFlag));
        while (waitFlag == 0) {
            usleep(200);
        }
        ASSERT_EQ(0, finishFlag);

        FakeDumpTask* fakeDumpTask = dynamic_cast<FakeDumpTask*>(dumpTask.get());
        while (!fakeDumpTask->IsDumping()) {
            usleep(200);
        }

        ASSERT_TRUE(fakeDumpTask->IsDumped() == false);
        ASSERT_TRUE(_dumpScheduler->HasDumpTask() == true);
        ASSERT_EQ(0, finishFlag);

        cond.signal();

        while (!fakeDumpTask->IsDumped()) {
            usleep(200);
        }

        ASSERT_TRUE(_dumpScheduler->HasDumpTask() == false);
        _waitDumpThread->join();
        ASSERT_EQ(1, finishFlag);
    }

private:
    void WaitDumpFunc(int* waitFlag, int* finishFlag)
    {
        *waitFlag = 1;
        EXPECT_EQ(FSEC_OK, _dumpScheduler->WaitDumpQueueEmpty());
        *waitFlag = 0;
        *finishFlag = 1;
    }

private:
    AsyncDumpSchedulerPtr _dumpScheduler;
    autil::ThreadPtr _waitDumpThread;
};

INDEXLIB_UNIT_TEST_CASE(AsyncDumpSchedulerTest, TestCaseForHasDumpTaskAndWaitDumpQueueEmpty);
}} // namespace indexlib::file_system
