#pragma once

#include <memory>

#include "autil/Thread.h"
#include "autil/TimeUtility.h"
#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace test {

class MultiThreadTestBase : public INDEXLIB_TESTBASE
{
public:
    MultiThreadTestBase() {}
    virtual ~MultiThreadTestBase() {}

public:
    virtual void CaseSetUp() override {}
    virtual void CaseTearDown() override {}

protected:
    void DoMultiThreadTest(size_t readThreadNum, int64_t duration);

    void Write()
    {
        while (!mIsRun) {
            usleep(0);
        }
        DoWrite();
    }
    void Read(int* status)
    {
        while (!mIsRun) {
            usleep(0);
        }
        DoRead(status);
    }

    bool IsFinished() { return mIsFinish; }

    virtual void DoWrite() = 0;
    virtual void DoRead(int* status) = 0;

private:
    bool volatile mIsFinish;
    bool volatile mIsRun;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MultiThreadTestBase);

////////////////////////////////////////////////////////////
inline void MultiThreadTestBase::DoMultiThreadTest(size_t readThreadNum, int64_t duration)
{
    std::vector<int> status(readThreadNum, 0);
    mIsFinish = false;
    mIsRun = false;

    std::vector<autil::ThreadPtr> readThreads;
    for (size_t i = 0; i < readThreadNum; ++i) {
        autil::ThreadPtr thread = autil::Thread::createThread(std::bind(&MultiThreadTestBase::Read, this, &status[i]));
        readThreads.push_back(thread);
    }
    autil::ThreadPtr writeThread = autil::Thread::createThread(std::bind(&MultiThreadTestBase::Write, this));

    mIsRun = true;
    int64_t beginTime = autil::TimeUtility::currentTimeInSeconds();
    int64_t endTime = beginTime;
    while (endTime - beginTime < duration) {
        sleep(1);
        endTime = autil::TimeUtility::currentTimeInSeconds();
    }
    mIsFinish = true;

    for (size_t i = 0; i < readThreadNum; ++i) {
        readThreads[i].reset();
    }
    writeThread.reset();
    for (size_t i = 0; i < readThreadNum; ++i) {
        INDEXLIB_TEST_EQUAL((int)0, status[i]);
    }
}
}} // namespace indexlib::test
